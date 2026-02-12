import argparse
from pyspark.sql import SparkSession
from etl.spec_loader import load_spec
from etl.transforms import build_select
from etl.engines.scd2 import scd2_merge

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--spec", required=True)
    ap.add_argument("--run_dt", required=True)
    ap.add_argument("--run_id", required=True)
    args = ap.parse_args()

    spark = SparkSession.builder.appName("metadata-etl").getOrCreate()

    spec = load_spec(args.spec, {"run_dt": args.run_dt, "run_id": args.run_id})

    # Read source (start with S3 csv snapshot)
    src = spec["source"]
    df = (spark.read.option("header", src["options"].get("header", True))
                 .csv(src["path"]))

    today_df = build_select(df, spec["columns"])

    if spec["mode"] == "scd2":
        # current read (parquet)
        cur_path = spec["target"]["current_view_path"]
        try:
            cur_df = spark.read.parquet(cur_path)
        except Exception:
            cur_df = spark.createDataFrame([], today_df.schema) \
                         .withColumn("hash_diff", F.lit(None).cast("string")) \
                         .withColumn("effective_from", F.lit(None).cast("timestamp")) \
                         .withColumn("effective_to", F.lit(None).cast("timestamp")) \
                         .withColumn("is_current", F.lit(True))

        inserts, expires = scd2_merge(
            today_df=today_df,
            current_df=cur_df.filter("is_current=true"),
            business_keys=spec["keys"]["business"],
            track_cols=spec["scd2"]["track_columns"],
            run_dt=args.run_dt,
            open_to=spec["scd2"]["effective_to_open"]
        )

        out_path = spec["target"]["path"]
        inserts.withColumn("run_dt", F.lit(args.run_dt)).write.mode("append").parquet(out_path)
        expires.withColumn("run_dt", F.lit(args.run_dt)).write.mode("append").parquet(out_path)

        # refresh current view from history (simple)
        hist = spark.read.parquet(out_path)
        hist.filter("is_current=true").write.mode("overwrite").parquet(cur_path)

    else:
        # batch mode: implement transforms/joins/agg (next step)
        pass

if __name__ == "__main__":
    main()
