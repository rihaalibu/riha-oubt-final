############
import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        ["BUCKET", "MASTER_PREFIX", "SCD2_PREFIX", "AUDIT_PREFIX"],
    )

    bucket = args["BUCKET"]
    golden_vendors_path = (
        f"s3://{bucket}/{args['MASTER_PREFIX']}vendors/golden_vendors.csv"
    )
    scd2_vendors_path = f"s3://{bucket}/{args['SCD2_PREFIX']}dim_vendors/"
    audit_path = f"s3://{bucket}/{args['AUDIT_PREFIX']}scd2_vendors/"

    spark = SparkSession.builder.appName("03b_scd2_vendors").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    def dq_fail(msg: str) -> None:
        raise ValueError(msg)

    def now_iso() -> str:
        return datetime.utcnow().isoformat()

    # -------------------------
    # Read + DQ guards
    # -------------------------
    new_raw = spark.read.option("header", True).csv(golden_vendors_path)

    required_cols = ["vendor_code", "vendor_name", "active_flag"]
    missing = [c for c in required_cols if c not in new_raw.columns]
    if missing:
        dq_fail(f"Golden vendors missing required columns: {missing}")

    new_df = (
        new_raw.withColumn("vendor_code", F.col("vendor_code").cast(T.IntegerType()))
        .withColumn("vendor_name", F.upper(F.trim(F.col("vendor_name"))))
        .withColumn(
            "vendor_name",
            F.when(
                F.col("vendor_name").isNull() | (F.col("vendor_name") == ""),
                F.lit("UNKNOWN"),
            ).otherwise(F.col("vendor_name")),
        )
        .withColumn(
            "active_flag",
            F.when(F.col("active_flag").isNull(), F.lit(True)).otherwise(
                F.col("active_flag").cast(T.BooleanType())
            ),
        )
    )

    null_keys = new_df.filter(F.col("vendor_code").isNull()).count()
    if null_keys > 0:
        dq_fail(f"SCD2 vendors aborted: {null_keys} rows have NULL vendor_code")

    dup_keys = new_df.groupBy("vendor_code").count().filter(F.col("count") > 1).count()
    if dup_keys > 0:
        dq_fail(
            f"SCD2 vendors aborted: found {dup_keys} duplicate vendor_code keys in golden input"
        )

    rows_in_golden = new_df.count()
    if rows_in_golden == 0:
        dq_fail("SCD2 vendors aborted: golden input is empty")

    # Hash over business columns
    new_df = new_df.withColumn(
        "hash_diff",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("vendor_code").cast("string"),
                F.coalesce(F.col("vendor_name"), F.lit("")),
                F.coalesce(F.col("active_flag").cast("string"), F.lit("")),
            ),
            256,
        ),
    )

    # -------------------------
    # Ensure SCD2 Delta exists
    # -------------------------
    if not DeltaTable.isDeltaTable(spark, scd2_vendors_path):
        base_df = (
            new_df.withColumn(
                "vendor_key",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.col("vendor_code").cast("string"),
                        F.lit(now_iso()),
                        F.lit("1"),
                    ),
                    256,
                ),
            )
            .withColumn("version_number", F.lit(1))
            .withColumn("is_current", F.lit(True))
            .withColumn("start_date", F.current_timestamp())
            .withColumn("end_date", F.lit(None).cast("timestamp"))
            .select(
                "vendor_key",
                "vendor_code",
                "vendor_name",
                "active_flag",
                "hash_diff",
                "version_number",
                "is_current",
                "start_date",
                "end_date",
            )
        )

        base_df.write.format("delta").mode("overwrite").save(scd2_vendors_path)

        audit_df = spark.createDataFrame(
            [
                {
                    "run_ts": now_iso(),
                    "rows_in_golden": rows_in_golden,
                    "rows_inserted": base_df.count(),
                    "rows_expired": 0,
                    "rows_noop": 0,
                    "note": "first_run_delta_create",
                }
            ]
        )
        audit_df.write.mode("append").json(audit_path)

        spark.stop()
        return  # ✅ Glue-safe early exit

    # -------------------------
    # Load current SCD2
    # -------------------------
    delta_tbl = DeltaTable.forPath(spark, scd2_vendors_path)
    scd2_df = delta_tbl.toDF()

    current_df = scd2_df.filter(F.col("is_current") == True).select(
        "vendor_code", "hash_diff", "version_number"
    )

    # Join to detect new/changed
    joined = new_df.alias("n").join(current_df.alias("c"), on="vendor_code", how="left")

    changes = joined.filter(
        F.col("c.vendor_code").isNull() | (F.col("n.hash_diff") != F.col("c.hash_diff"))
    )

    rows_changed_or_new = changes.count()
    if rows_changed_or_new == 0:
        # No-op run (still write audit)
        audit_df = spark.createDataFrame(
            [
                {
                    "run_ts": now_iso(),
                    "rows_in_golden": rows_in_golden,
                    "rows_inserted": 0,
                    "rows_expired": 0,
                    "rows_noop": rows_in_golden,
                    "note": "noop_no_changes",
                }
            ]
        )
        audit_df.write.mode("append").json(audit_path)
        spark.stop()
        return

    # Compute next version_number: existing + 1, else 1
    changes_ins = changes.withColumn(
        "next_version",
        F.when(F.col("c.version_number").isNull(), F.lit(1)).otherwise(
            F.col("c.version_number") + 1
        ),
    ).select(
        F.col("vendor_code"),
        F.col("n.vendor_name").alias("vendor_name"),
        F.col("n.active_flag").alias("active_flag"),
        F.col("n.hash_diff").alias("hash_diff"),
        F.col("next_version").alias("version_number"),
    )

    # -------------------------
    # Stage rows for a SINGLE MERGE:
    #  - "expire" rows match current target rows (merge_key = vendor_code)
    #  - "insert" rows do NOT match (merge_key = NULL) so they go to insert clause
    # -------------------------
    expire_rows = changes_ins.select(
        F.col("vendor_code").alias("merge_key"),
        F.lit("expire").alias("op"),
        F.col("vendor_code"),
        F.lit(None).cast("string").alias("vendor_key"),
        F.lit(None).cast("string").alias("vendor_name"),
        F.lit(None).cast("boolean").alias("active_flag"),
        F.lit(None).cast("string").alias("hash_diff"),
        F.lit(None).cast("int").alias("version_number"),
    )

    insert_rows = changes_ins.select(
        F.lit(None).cast("int").alias("merge_key"),
        F.lit("insert").alias("op"),
        F.col("vendor_code"),
        # deterministic surrogate key (string) suitable for Delta
        F.sha2(
            F.concat_ws(
                "||",
                F.col("vendor_code").cast("string"),
                F.current_timestamp().cast("string"),
                F.col("version_number").cast("string"),
            ),
            256,
        ).alias("vendor_key"),
        F.col("vendor_name"),
        F.col("active_flag"),
        F.col("hash_diff"),
        F.col("version_number"),
    )

    staged = expire_rows.unionByName(insert_rows)

    # -------------------------
    # MERGE SCD2
    # -------------------------
    # Match only current records
    merge_cond = "t.vendor_code = s.merge_key AND t.is_current = true"

    (
        delta_tbl.alias("t")
        .merge(staged.alias("s"), merge_cond)
        .whenMatchedUpdate(
            condition="s.op = 'expire'",
            set={
                "is_current": F.lit(False),
                "end_date": F.current_timestamp(),
            },
        )
        .whenNotMatchedInsert(
            condition="s.op = 'insert'",
            values={
                "vendor_key": F.col("s.vendor_key"),
                "vendor_code": F.col("s.vendor_code"),
                "vendor_name": F.col("s.vendor_name"),
                "active_flag": F.col("s.active_flag"),
                "hash_diff": F.col("s.hash_diff"),
                "version_number": F.col("s.version_number"),
                "is_current": F.lit(True),
                "start_date": F.current_timestamp(),
                "end_date": F.lit(None).cast("timestamp"),
            },
        )
        .execute()
    )

    # -------------------------
    # Audit
    # -------------------------
    rows_inserted = insert_rows.count()
    rows_expired = expire_rows.select("vendor_code").distinct().count()
    rows_total_scd2 = DeltaTable.forPath(spark, scd2_vendors_path).toDF().count()

    audit_df = spark.createDataFrame(
        [
            {
                "run_ts": now_iso(),
                "rows_in_golden": rows_in_golden,
                "rows_inserted": rows_inserted,
                "rows_expired": rows_expired,
                "rows_total_scd2": rows_total_scd2,
                "note": "merge_scd2",
            }
        ]
    )
    audit_df.write.mode("append").json(audit_path)

    spark.stop()


if __name__ == "__main__":
    main()
