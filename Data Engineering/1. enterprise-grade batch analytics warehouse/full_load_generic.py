import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import json
from pyspark.sql import functions as F 
from pyspark.sql.window import Window 

## @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv, 
    ["JOB_NAME", "run_dt", "raw_base", "curated_base", "config_s3", "run_id"])

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

def read_s3_config(path: str) -> str:
    return "\n".join([r.value for r in pyspark.read.text(path).collect()])
    
cfg = read_s3_config(config_s3)

name = cfg["name"]
src_table = cfg["source"]["table"]
src_fmt = cfg["source"].get("format", "csv")
src_header = bool(cfg["source"].get("header", True))
select_expr = cfg["select_expr"]
dedup = cfg.get("dedupe")
out_rel = cfg["output"]["path"]
part_by_dt = bool(cfg["output"].get("partition_by_dt", False))

def src_path_for_dt(table: str, run_dt: str) -> str:
    return f"{raw_base}/snapshot/{table}/{run_dt}/"
    
def curated_path(rel: str) -> str:
    return f"{curated_base}/{rel.strip('/')}/"
    
def read_snapshot(table: str):
    p = src_path_for_dt(table, run_dt)
    if src_fmt == "csv":
        return spark.read.option("header", src_header).csv(p)
    elif src_fmt == 'json':
        return spark.read.json(p)
    else:
        return spark.read.parquet(p)
        
def apply_select_expr(df):
    return df.select(*[F.expr(expr).alias(out_col) for out_col, expr in select_expr.items()])
    
def dedup_df(df):
    if not dedup:
        return df 
    key_cols = dedup["key"]
    order_by = dedup.get("order_by", [])
    sort_cols = []
    for s in order_by:
        parts = s.strip().split()
        c = parts[0]
        desc = len(parts) > 1 and parts[-1].lower().startswith("desc")
        sort_cols.append(F.col(c).desc() if desc else F.col(c).asc() )
    w = Window.partitionBy(*key_cols).order_by(*sort_cols)
    return df.withColumn("_rn", R.row_number().over(w)).filter("_rn" = 1).drop("_rn")
    
src = read_snapshot(src_table)
out = apply_select_expr(src)
our = dedup_df(out)

out = (
    out
    .withColumn("load_dt", F.lit(RUN_DT))
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("load_ts", F.current_timestamp())
    )

out_path = curated_path(out_rel)
w = out.write_mode("overwrite")

if part_by_dt:
    w = w.partitionBy("load_dt")

w.parquet(out_path)



job.commit()