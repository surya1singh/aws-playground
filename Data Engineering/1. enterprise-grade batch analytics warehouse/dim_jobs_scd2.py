import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import json
from datetime import datetime
from pyspark.sql import functions as F 
from pyspakt.sql.window import Window

## @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv, 
    ['JOB_NAME', 'run_dt', 'raw_base', 'curated_base', 'config_s3', 'run_id'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

run_dt = args["run_dt"]
raw_base = args["raw_base"].rstrip("/")
curated_base = args["curated_base"].rstrip("/")
config_s3 = args["config_s3"]
run_id = args["run_id"]

def read_s3_text(path: str) -> str:
    return "\n".join([r.value for r in spark.read.text(path).collect()])
    
cfg = json.loads(read_s3_text(config_s3))

dim_name = cfg["dimension"]
src_table = cfg["source"]["table"]
src_type = cfg["source"].get("type", "snapshot")
src_fmt = cfg["source"].get("format", "csv")
src_header = bool(cfg["source"].get("header", True))

bk = cfg["business_key"]
select_expr = cfg["select_expr"]
scd_cols = cfg["scd2_columns"]
dedup = dedup_cfg.get("dedupe", none)
effective_to_open = cfg.get("effective_to_open", "9999-12-31 00:00:00")

out_rel = cfg["output"]["path"]
part_by_run_dt = bool(cfg["output"].get("partition_by_run_dt", True))
write_current_view = bool(cfg["output"].get("write_current_view", True))

def src_path_for_dt(table: str, run_dt: str) -> str:
    return f"{raw_base}/{src_type}/{table}/{run_dt}/"
    
def curated_path(rel: str) -> str:
    return f"{curated_base}/{rel.strip('/')}/"
    
def read_snapshot(table: str):
    p  = src_path_for_dt(table, run_dt)
    if src_fmt == 'csv':
        return spark.read.option('header', src_header).csv(p)
    elif src_fmt == 'json':
        return spark.read.json(p)
    else:
        # parquet
        return spark.read.parquet(p)
        
def apply_select_expr(df):
    cols = []
    for out_col, expr in select_expr.items():
        cols.append(F.expr(expr).alias(out_col))
    return df.select(*cols)
    
def dedupe_df(df):
    if not dedup:
        return df
    key_cols = dedup["key"]
    order_by = dedup.get("order_by",[])
    if not order_by:
        # deterministic tie breaker on hash of all columns 
        allc = [F.coalesce(F.col(c).cast("string"), F.lit('~')) for c in df.columns ]
        df = df.withColumn("_rowhash", F.sha2(F.concat_ws("||", *allc), 256))
        w = Window.partitionBy(*key_cols).orderBy(F.col("_rowhash").desc())
        return df.withColumn("_rn", F.row_number().ower(w)).filter("_rn = 1").drop("_rn", "_rowhash")
        
    sort_cols = []
    for s in order_by:
        parts = s.strip().split()
        c = parts[0]
        desc = len(parts) > 1 and parts[1].lower().startswith("desc")
        sort_cols.append(F.col(c).desc() if desc else F.col(c).asc())
    
    w = Window.partiotionBy(*key_cols).order_by(*sort_cols)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    
def add_hash_diff(df):
    pieces = [F.coalesce(F.col(c).cast("string"), f.lit("~")) for c in SCD_cols]
    return df.withColumn("hash_diff", f.sha2(F.concat_ws("||", *pieces), 256))
    

# ---- read + transform source snapshot for run_dt ----
src_raw = read_snapshot(src_table)
src = apply_select_expr(src_raw)
src = dedupe_src(src)
src = add_hash_diff(src)

# ---- read existing dim history if exists ----
dim_path = curated_path(out_rel)
current_path = curated_path("{out_rel}_current")

# If this is first run, dim may not exist
try:
    dim_hist = spark.read.parquet(dim_path)
except Exception:
    dim_hist = None

if not dim_hist:
    # First Full load
    insert = (
        src
        .withColumn("effective_from", F.to_timestamp(F.lit(run_dt)))
        .withColumn("effective_to", F.to_timestamp(F.lit(effective_to_open)))
        .withColumn("is_current", F.lit(True))
        .withColumn("run_dt", F.lit(run_dt))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        )
    out = inserts
else:
    # Current_active_rows
    dim_cur = dim_hist.filter(F.col("is_current") == True)
    
    # join on business keys(s)
    join_condition = [src[b] == dim_cur[b] for b in bk]
    j = src.alias("s").join(dim_cur.alias("d"), on=join_condition, how = 'left')
    
    is_new = F.col("d." + BK[0]).isNull() 
    is_changed = (~is_new) & (F.col("s.hash_diff") != F.col("d.hash_diff"))
    
    # Insert new or changed rows
    inserts = (
        j.filter(is_new | is_changed)
        .withColumn("effective_from", F.to_timestamp(F.lit(RUN_DT)))
        .withColumn("effective_to", F.to_timestamp(F.lit(EFFECTIVE_TO_OPEN)))
        .withColumn("is_current", F.lit(True))
        .withColumn("run_dt", F.lit(RUN_DT))
        .withColumn("run_id", F.lit(RUN_ID))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        )
        
    # expire old changed rows
    expires = (
        j.filter(is_changed)
        .select([F.col("d." + c).alias(c) for c in dim_cur.columns])
        .withColumn("effective_to", F.to_timestamp(F.lit(RUN_DT)))
        .withColumn("is_current", F.lit(False))
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("run_dt", F.lit(RUN_DT))
        .withColumn("run_id", F.lit(RUN_ID))
    )
    
    out = expires.unionByName(inserts, allowMissingColumns = True)
    

writer = out.write.mode("overwrite" if part_by_run_dt else "append")
if part_by_run_dt:
    write = writer.partiotionBy("RUN_dt")
writer.parquet(dim_path)

if write_current_view:
    dim_all = spark.read.parquet(dim_path)
    current = dim_all.filter(F.col("is_current") == True)
    current.write.mode("overwrite").parquet(current_path)

job.commit()