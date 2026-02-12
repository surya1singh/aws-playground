from pyspark.sql import functions as F

def add_hash(df, cols):
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in cols]
    return df.withColumn("hash_diff", F.sha2(F.concat_ws("||", *parts), 256))

def scd2_merge(today_df, current_df, business_keys, track_cols, run_dt, open_to):
    today = add_hash(today_df, track_cols)
    cur = current_df

    j = today.alias("s").join(cur.alias("d"), on=business_keys, how="left")

    is_new = F.col("d." + business_keys[0]).isNull()
    is_changed = (~is_new) & (F.col("s.hash_diff") != F.col("d.hash_diff"))

    inserts = (
        j.filter(is_new | is_changed)
         .select([F.col("s."+c).alias(c) for c in today.columns])
         .withColumn("effective_from", F.to_timestamp(F.lit(run_dt)))
         .withColumn("effective_to", F.to_timestamp(F.lit(open_to)))
         .withColumn("is_current", F.lit(True))
    )

    expires = (
        j.filter(is_changed)
         .select([F.col("d."+c).alias(c) for c in cur.columns])
         .withColumn("effective_to", F.to_timestamp(F.lit(run_dt)))
         .withColumn("is_current", F.lit(False))
         .withColumn("updated_at", F.current_timestamp())
    )

    return inserts, expires
