# 03b_scd2_vendors.py

import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def main():
    # Glue arguments
    args = getResolvedOptions(
        sys.argv, ["BUCKET", "MASTER_PREFIX", "SCD2_PREFIX", "AUDIT_PREFIX"]
    )

    bucket = args["BUCKET"]
    golden_path = f"s3://{bucket}/{args['MASTER_PREFIX']}vendors/"
    scd2_path = f"s3://{bucket}/{args['SCD2_PREFIX']}dim_vendors/"
    audit_path = f"s3://{bucket}/{args['AUDIT_PREFIX']}scd2_vendors/"

    # Spark
    spark = SparkSession.builder.appName("03b_scd2_vendors").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Read Golden Vendors
    raw_df = spark.read.option("header", True).csv(golden_path)

    # Schema enforcement (Golden → Canonical)
    required_cols = ["vendorid", "vendor_name"]
    missing = [c for c in required_cols if c not in raw_df.columns]
    if missing:
        raise ValueError(f"SCD2 vendors aborted: missing columns {missing}")

    # Canonicalize column names + types (MDM contract)
    new_df = (
        raw_df.withColumn("VendorID", F.col("vendorid").cast(IntegerType()))
        .withColumn("vendor_name", F.upper(F.trim(F.col("vendor_name"))))
        .select("VendorID", "vendor_name")
    )

    # Business key must exist
    null_keys = new_df.filter(F.col("VendorID").isNull()).count()
    if null_keys > 0:
        raise ValueError(f"SCD2 vendors aborted: {null_keys} NULL VendorID values")

    rows_in_golden = new_df.count()
    if rows_in_golden == 0:
        raise ValueError("SCD2 vendors aborted: golden vendors is empty")

    # Hash for change detection (business attributes only)
    new_df = new_df.withColumn(
        "hash_diff",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("VendorID").cast("string"),
                F.coalesce(F.col("vendor_name"), F.lit("")),
            ),
            256,
        ),
    )

    # First run: create SCD2 Delta table
    if not DeltaTable.isDeltaTable(spark, scd2_path):
        base_df = (
            new_df.withColumn(
                "vendor_key",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.col("VendorID").cast("string"),
                        F.current_timestamp().cast("string"),
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
                "VendorID",
                "vendor_name",
                "hash_diff",
                "version_number",
                "is_current",
                "start_date",
                "end_date",
            )
        )

        base_df.write.format("delta").mode("overwrite").save(scd2_path)

        audit_df = spark.createDataFrame(
            [
                {
                    "run_ts": datetime.utcnow().isoformat(),
                    "rows_in_golden": rows_in_golden,
                    "rows_inserted": base_df.count(),
                    "rows_expired": 0,
                    "note": "first_run_create",
                }
            ]
        )
        audit_df.write.mode("append").json(audit_path)
        spark.stop()
        return

    # Load current SCD2 state
    delta_tbl = DeltaTable.forPath(spark, scd2_path)

    current_df = (
        delta_tbl.toDF()
        .filter(F.col("is_current") == True)
        .select("VendorID", "hash_diff", "version_number")
    )

    joined = new_df.alias("n").join(current_df.alias("c"), on="VendorID", how="left")

    changes = joined.filter(
        F.col("c.VendorID").isNull() | (F.col("n.hash_diff") != F.col("c.hash_diff"))
    )

    if changes.count() == 0:
        audit_df = spark.createDataFrame(
            [
                {
                    "run_ts": datetime.utcnow().isoformat(),
                    "rows_in_golden": rows_in_golden,
                    "rows_inserted": 0,
                    "rows_expired": 0,
                    "note": "noop_no_changes",
                }
            ]
        )
        audit_df.write.mode("append").json(audit_path)
        spark.stop()
        return

    # Prepare staged rows for SINGLE MERGE
    staged_base = changes.withColumn(
        "next_version",
        F.when(F.col("c.version_number").isNull(), F.lit(1)).otherwise(
            F.col("c.version_number") + 1
        ),
    ).select(
        "VendorID",
        "vendor_name",
        "hash_diff",
        F.col("next_version").alias("version_number"),
    )

    # Expire rows
    expire_rows = staged_base.select(
        F.col("VendorID").alias("merge_key"),
        F.lit("expire").alias("op"),
        F.col("VendorID"),
        F.lit(None).cast("string").alias("vendor_key"),
        F.lit(None).cast("string").alias("vendor_name"),
        F.lit(None).cast("string").alias("hash_diff"),
        F.lit(None).cast("int").alias("version_number"),
    )

    # Insert rows
    insert_rows = staged_base.select(
        F.lit(None).cast("int").alias("merge_key"),
        F.lit("insert").alias("op"),
        F.col("VendorID"),
        F.sha2(
            F.concat_ws(
                "||",
                F.col("VendorID").cast("string"),
                F.current_timestamp().cast("string"),
                F.col("version_number").cast("string"),
            ),
            256,
        ).alias("vendor_key"),
        F.col("vendor_name"),
        F.col("hash_diff"),
        F.col("version_number"),
    )

    staged_df = expire_rows.unionByName(insert_rows)

    # Delta MERGE (SCD2)
    (
        delta_tbl.alias("t")
        .merge(
            staged_df.alias("s"),
            "t.VendorID = s.merge_key AND t.is_current = true",
        )
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
                "VendorID": F.col("s.VendorID"),
                "vendor_name": F.col("s.vendor_name"),
                "hash_diff": F.col("s.hash_diff"),
                "version_number": F.col("s.version_number"),
                "is_current": F.lit(True),
                "start_date": F.current_timestamp(),
                "end_date": F.lit(None).cast("timestamp"),
            },
        )
        .execute()
    )

    audit_df = spark.createDataFrame(
        [
            {
                "run_ts": datetime.utcnow().isoformat(),
                "rows_in_golden": rows_in_golden,
                "rows_inserted": insert_rows.count(),
                "rows_expired": expire_rows.count(),
                "note": "merge_scd2",
            }
        ]
    )
    audit_df.write.mode("append").json(audit_path)

    spark.stop()


if __name__ == "__main__":
    main()
