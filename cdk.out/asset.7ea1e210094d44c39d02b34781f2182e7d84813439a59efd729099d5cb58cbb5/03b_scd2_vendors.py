# 03b_scd2_vendors.py
# Placeholder in CDK-only bundle.
import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    sha2,
    concat_ws,
    when,
    coalesce,
    trim,
)
from pyspark.sql.types import IntegerType, BooleanType

args = getResolvedOptions(
    sys.argv,
    [
        "BUCKET",
        "MASTER_PREFIX",
        "SCD2_PREFIX",
        "AUDIT_PREFIX",
    ],
)

bucket = args["BUCKET"]

golden_vendors_path = f"s3://{bucket}/{args['MASTER_PREFIX']}vendors/golden_vendors.csv"
scd2_vendors_path = f"s3://{bucket}/{args['SCD2_PREFIX']}dim_vendors/"
audit_path = f"s3://{bucket}/{args['AUDIT_PREFIX']}scd2_vendors/"

spark = (
    SparkSession.builder.appName("03b_scd2_vendors")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# -------------------------
# Read golden vendors (MDM)
# -------------------------
new_df = spark.read.option("header", True).csv(golden_vendors_path)

# Normalize & enforce types
new_df = (
    new_df.withColumn("vendor_code", col("vendor_code").cast(IntegerType()))
    .withColumn("vendor_name", trim(col("vendor_name")))
    .withColumn("active_flag", col("active_flag").cast(BooleanType()))
)

# Basic completeness checks (fail-fast if business key missing)
missing_keys = new_df.filter(col("vendor_code").isNull()).count()
if missing_keys > 0:
    raise ValueError(f"SCD2 vendors aborted: {missing_keys} rows have null vendor_code")

# Compute hash over business columns only
hash_cols = ["vendor_code", "vendor_name", "active_flag"]
new_df = new_df.withColumn("hash_diff", sha2(concat_ws("||", *hash_cols), 256))

# -------------------------
# Read existing SCD2 if any
# -------------------------
try:
    old_df = spark.read.parquet(scd2_vendors_path)
    scd2_exists = True
except Exception:
    scd2_exists = False

# -------------------------
# First run creates SCD2 base
# -------------------------
if not scd2_exists:
    final_df = (
        new_df.withColumn("vendor_key", lit(None).cast("long"))
        .withColumn("version_number", lit(1))
        .withColumn("is_current", lit(True))
        .withColumn("start_date", current_timestamp())
        .withColumn("end_date", lit(None).cast("timestamp"))
    )

    final_df.write.mode("overwrite").parquet(scd2_vendors_path)

    audit_df = spark.createDataFrame(
        [
            {
                "run_ts": datetime.utcnow().isoformat(),
                "rows_in_golden": new_df.count(),
                "rows_inserted": final_df.count(),
                "rows_expired": 0,
                "note": "first_run",
            }
        ]
    )
    audit_df.write.mode("append").json(audit_path)

    spark.stop()


# -------------------------
# SCD2 change detection
# -------------------------
current_df = old_df.filter(col("is_current") == True)

# Join on business key; detect new or changed hash
joined = new_df.alias("n").join(current_df.alias("c"), on="vendor_code", how="left")

changes_df = joined.filter(
    col("c.vendor_code").isNull() | (col("n.hash_diff") != col("c.hash_diff"))
)

changed_keys_df = changes_df.select(col("vendor_code")).distinct()

# -------------------------
# Expire existing current records for changed keys
# -------------------------
expired_df = (
    old_df.alias("o")
    .join(changed_keys_df.alias("k"), on="vendor_code", how="left")
    .withColumn(
        "is_current",
        when(
            (col("o.is_current") == True) & col("k.vendor_code").isNotNull(), lit(False)
        ).otherwise(col("o.is_current")),
    )
    .withColumn(
        "end_date",
        when(
            (col("o.is_current") == True) & col("k.vendor_code").isNotNull(),
            current_timestamp(),
        ).otherwise(col("o.end_date")),
    )
)

# -------------------------
# Insert new versions
# -------------------------
new_versions_df = (
    changes_df.withColumn(
        "version_number",
        when(col("c.version_number").isNull(), lit(1)).otherwise(
            col("c.version_number") + 1
        ),
    )
    .withColumn("is_current", lit(True))
    .withColumn("start_date", current_timestamp())
    .withColumn("end_date", lit(None).cast("timestamp"))
)

# Align schema to old_df
final_cols = expired_df.columns
new_versions_df = new_versions_df.select(*final_cols)

final_df = expired_df.unionByName(new_versions_df)

final_df.write.mode("overwrite").parquet(scd2_vendors_path)

# -------------------------
# Audit
# -------------------------
rows_expired = expired_df.filter(col("is_current") == False).count()
rows_inserted = new_versions_df.count()

audit_df = spark.createDataFrame(
    [
        {
            "run_ts": datetime.utcnow().isoformat(),
            "rows_in_golden": new_df.count(),
            "rows_inserted": rows_inserted,
            "rows_expired": rows_expired,
            "rows_total_scd2": final_df.count(),
        }
    ]
)
audit_df.write.mode("append").json(audit_path)

spark.stop()
