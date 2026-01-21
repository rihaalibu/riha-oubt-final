# 02c_create_golden_vendors.py

import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, upper, create_map
from pyspark.sql.types import IntegerType

# -------------------------------------------------
# Glue arguments (ZONE-DRIVEN, NOT DATASET-DRIVEN)
# -------------------------------------------------
args = getResolvedOptions(
    sys.argv, ["BUCKET", "PROCESSED_PREFIX", "MASTER_PREFIX", "AUDIT_PREFIX"]
)

bucket = args["BUCKET"]

processed_trips_path = f"s3://{bucket}/{args['PROCESSED_PREFIX']}nyc_taxi/"
golden_path = f"s3://{bucket}/{args['MASTER_PREFIX']}vendors/"
audit_path = f"s3://{bucket}/{args['AUDIT_PREFIX']}mdm_vendors/"

# -------------------------------------------------
# Spark
# -------------------------------------------------
spark = SparkSession.builder.appName("02c_create_golden_vendors").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------
# Read processed trips
# -------------------------------------------------
trips = spark.read.parquet(processed_trips_path)

rows_in = trips.count()

# -------------------------------------------------
# Schema enforcement
# -------------------------------------------------
required_cols = ["VendorID"]
missing = [c for c in required_cols if c not in trips.columns]
if missing:
    raise ValueError(f"Missing required columns in trips source: {missing}")

vendors = trips.select("VendorID").withColumn(
    "VendorID", col("VendorID").cast(IntegerType())
)

# Fail-fast on missing business keys
null_keys = vendors.filter(col("VendorID").isNull()).count()
if null_keys > 0:
    raise ValueError(f"Found {null_keys} rows with NULL VendorID")

# -------------------------------------------------
# VendorID → vendor_name (TLC DATA DICTIONARY)
# -------------------------------------------------
vendor_map = create_map(
    lit(1),
    lit("CREATIVE MOBILE TECHNOLOGIES, LLC"),
    lit(2),
    lit("CURB MOBILITY, LLC"),
    lit(6),
    lit("MYLE TECHNOLOGIES INC"),
    lit(7),
    lit("HELIX"),
)

vendors = vendors.withColumn(
    "vendor_name",
    when(vendor_map[col("VendorID")].isNull(), lit("UNKNOWN")).otherwise(
        upper(vendor_map[col("VendorID")])
    ),
)

# -------------------------------------------------
# Deduplication (Golden Record)
# -------------------------------------------------
golden = vendors.dropDuplicates(["VendorID"])

rows_out = golden.count()

# -------------------------------------------------
# Write Golden Vendors (DATASET DIRECTORY)
# -------------------------------------------------
golden.coalesce(1).write.mode("overwrite").option("header", True).csv(golden_path)

# -------------------------------------------------
# Audit (aligned with zones golden)
# -------------------------------------------------
audit_df = spark.createDataFrame(
    [
        {
            "run_ts": datetime.utcnow().isoformat(),
            "rows_in": rows_in,
            "rows_out": rows_out,
            "duplicates_removed": rows_in - rows_out,
            "business_key": "VendorID",
            "deterministic_rules": True,
            "mapping_source": "TLC data dictionary",
        }
    ]
)

audit_df.write.mode("append").json(audit_path)

spark.stop()
