import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import json
from pyspark.sql import functions as F 

## @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "run_dt", "raw_base", "curated_base", "config_s3", "run_id"]
)

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


name = cfg["name"]
src_table = cfg["source"]["table"]
src_fmt = cfg["source"].get("format", "csv")
src_header = bool(cfg["source"].get("header", True))
op_col = cfg.get("op_col", "op")
select_expr = cfg["select_expr"]
target = cfg["target"]

key_cols = cfg.get("key", [])

def delta_path_for_dt(table: str, run_dt: str) -> str:
    return f"{raw_base}/deltas/{table}/{run_dt}/"

def curated_path(rel: str) -> str:
    return f"{curated_base}/{rel.strip('/')}/"

def read_delta(table: str):
    p = delta_path_for_dt(table, run_dt)
    if src_fmt == "csv":
        return spark.read.option("header", src_header).csv(p)
    elif src_fmt == "json":
        return spark.read.json(p)
    else:
        return spark.read.parquet(p)
        
def apply_select_expr(df):
    return df.select(*[F.expr(expr).alias(out_col) for out_col, expr in select_expr.items()], F.col(op_col).alias(op_col))
    
src = read_delta(src_table)
df = apply_select_expr(src)

df = df.dropDuplicate()

df = (
    df.withColumn("run_dt", F.lit(run_dt))
      .withColumn("run_id", F.lit(run_id))
      .withColumn("load_ts", F.current_timestamp())
)


if target("type") == "s3_append":
    out_rel = target["path"]
    part_col = target.get("partition", "run_dt")
    
    only_insert = df.filter(F.col(op_col) == F.lit("I")).drop(op_col)
    
    out_path = curated_path(out_rel)
    only_inserts.write.mode("append").partitionBy(part_col).parquet(out_path)
    
elif target["type"] == "postgres_upsert":
    from awsglue.dynamicframe import DynamicFrame 
    
    if not key_cols:
        raise ValueError("postgress_upsert requires 'key' columns in config")
        
    conn_name = target["connection_name"]
    db = target["database"]
    tgt_table = target["target_table"]
    stg_table = target["staging_table"]
    
    d_i = df.filter(F.col(op_col) == "I").drop(op_col)
    d_c = df.filter(F.col(op_col) == "C").drop(op_col)
    d_d = df.filter(F.col(op_col) == "D").drop(op_col)
    
    upsert_df = d_i.unionByName(d_c, allowMissingColumns=True)
    
    stg_dyn = DynamicFrame.fromDF(upsert_df, glueContext, "stg_dyn")
    
    key_list = ", ".join(key_cols)
    exclude_set = ", ".join(f"{c}=EXCLUDED.{c}" for c in upsert_df.columns if c not in key_cols)
    
    del_stg = f"{stg_table}_del"
    def_df = d_d.select(*key_cols).dropDuplicate()
    del_dyn = DynamicFrame.fromDF(del_df, glueContext, "del_dyn")
    
    preactions = f"""
    CREATE TABLE IF NOT EXISTS {stg_table} (LIKE {tgt_table} INCLUDING DEFAULTS);
    TRUNCATE TABLE {stg_table};
    CREATE TABLE IF NOT EXISTS {del_stg} ({", ".join([f"{k} TEXT" for k in key_cols])});
    TRUNCATE TABLE {del_stg};
    """
    
    on_conflict = f"ON CONFLICT ({key_list}) DO UPDATE SET {excluded_set}" if excluded_set else f"ON CONFLICT ({key_list}) DO NOTHING"

    postactions = f"""
    -- Apply deletes
    DELETE FROM {tgt_table} t
    USING {del_stg} d
    WHERE {" AND ".join([f"t.{k}::text = d.{k}" for k in KEY_COLS])};

    -- Upsert inserts/changes
    INSERT INTO {tgt_table} ({", ".join(upsert_df.columns)})
    SELECT {", ".join(upsert_df.columns)} FROM {stg_table}
    {on_conflict};

    TRUNCATE TABLE {stg_table};
    TRUNCATE TABLE {del_stg};
    """
    
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=stg_dyn,
        catalog_connection=conn_name,
        connection_option = {
            "database": db,
            "dbtable": stg_table,
            "preactions": preactions,
            "postactions": postactions
        }
        )
        
    glueContext.write_dynamic_frame.from_jdbc_config(
        frame=del_dyn,
        catalog_connection=conn_name,
        connection_options = {
            "database": db,
            "dbtable": del_stg
        })
        
else:
    raise ValueError(f"Unknown target.type: {target['type']}")
    
job.commit()