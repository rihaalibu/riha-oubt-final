# 02c_create_golden_vendors.py

import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, upper, create_map
from pyspark.sql.types import IntegerType

# -------------------------------------------------
# Glue arguments
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
# Extract business key
# -------------------------------------------------
vendors = trips.select("vendorid").withColumn(
    "vendorid", col("vendorid").cast(IntegerType())
)

if vendors.filter(col("vendorid").isNull()).count() > 0:
    raise ValueError("Found NULL VendorID in trips")

# -------------------------------------------------
# Deterministic matching (Vendor dictionary)
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
    when(vendor_map[col("vendorid")].isNull(), lit("UNKNOWN")).otherwise(
        upper(vendor_map[col("vendorid")])
    ),
)

# -------------------------------------------------
# Golden record (one per VendorID)
# -------------------------------------------------
golden = vendors.dropDuplicates(["vendorid"])
rows_out = golden.count()

# -------------------------------------------------
# Write Golden Vendors
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
            "business_key": "VendorID",
            "matching_type": "deterministic",
            "mapping_source": "TLC data dictionary",
        }
    ]
)

audit_df.write.mode("append").json(audit_path)
spark.stop()
