# 02b_create_golden_zones.py

import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, trim, upper, when
from pyspark.sql.types import IntegerType

# -------------------------------------------------
# Glue arguments
# -------------------------------------------------
args = getResolvedOptions(
    sys.argv, ["BUCKET", "RAW_PREFIX", "MASTER_PREFIX", "AUDIT_PREFIX"]
)

bucket = args["BUCKET"]

raw_zones_path = f"s3://{bucket}/{args['RAW_PREFIX']}zones/"
golden_path = f"s3://{bucket}/{args['MASTER_PREFIX']}zones/"
audit_path = f"s3://{bucket}/{args['AUDIT_PREFIX']}mdm_zones/"

# -------------------------------------------------
# Spark
# -------------------------------------------------
spark = SparkSession.builder.appName("02b_create_golden_zones").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------
# Read raw zones
# -------------------------------------------------
zones = spark.read.option("header", True).csv(raw_zones_path)

rows_in = zones.count()

# -------------------------------------------------
# Schema enforcement
# -------------------------------------------------
required_cols = ["LocationID", "Zone", "Borough"]
missing = [c for c in required_cols if c not in zones.columns]
if missing:
    raise ValueError(f"Missing required columns in zones source: {missing}")

zones = (
    zones.withColumn("LocationID", col("LocationID").cast(IntegerType()))
    .withColumn("Zone", trim(col("Zone")))
    .withColumn("Borough", trim(col("Borough")))
)

# Fail-fast on missing business keys
null_keys = zones.filter(col("LocationID").isNull()).count()
if null_keys > 0:
    raise ValueError(f"Found {null_keys} rows with NULL LocationID")

# -------------------------------------------------
# Standardization rules
# -------------------------------------------------
zones = zones.withColumn("Zone", upper(col("zone"))).withColumn(
    "Borough",
    when(
        col("Borough").isNull() | (col("Borough") == ""), lit("Outside NYC")
    ).otherwise(upper(col("Borough"))),
)

# Normalize borough naming
zones = zones.withColumn(
    "Borough",
    when(col("Borough").isin("UNKNOWN", "N/A", "NA"), lit("Outside NYC")).otherwise(
        col("Borough")
    ),
)

# -------------------------------------------------
# Deduplication (Golden Record)
# -------------------------------------------------
golden = zones.dropDuplicates(["Borough", "Zone"])

rows_out = golden.count()

# -------------------------------------------------
# Write Golden Zones
# -------------------------------------------------
golden.coalesce(1).write.mode("overwrite").option("header", True).csv(golden_path)

# -------------------------------------------------
# Audit
# -------------------------------------------------
audit_df = spark.createDataFrame(
    [
        {
            "run_ts": datetime.utcnow().isoformat(),
            "rows_in": rows_in,
            "rows_out": rows_out,
            "duplicates_removed": rows_in - rows_out,
            "business_key": "LocationID",
            "deterministic_rules": True,
        }
    ]
)

audit_df.write.mode("append").json(audit_path)

spark.stop()
