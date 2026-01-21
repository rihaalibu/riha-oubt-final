# 04_fact_trips.py
import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    lit,
    when,
)
from pyspark.sql.types import IntegerType

# -------------------------------------------------
# Glue arguments
# -------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "BUCKET",
        "PROCESSED_PREFIX",
        "SCD2_PREFIX",
        "CURATED_PREFIX",
        "AUDIT_PREFIX",
    ],
)

bucket = args["BUCKET"]

processed_path = f"s3://{bucket}/{args['PROCESSED_PREFIX']}nyc_taxi/"
zones_path = f"s3://{bucket}/{args['SCD2_PREFIX']}dim_zones/"
vendors_path = f"s3://{bucket}/{args['SCD2_PREFIX']}dim_vendors/"
curated_path = f"s3://{bucket}/{args['CURATED_PREFIX']}fact_trips/"
audit_path = f"s3://{bucket}/{args['AUDIT_PREFIX']}fact_trips/"

# -------------------------------------------------
# Spark
# -------------------------------------------------
spark = SparkSession.builder.appName("04_fact_trips").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------
# Read processed trips (FACT SOURCE)
# -------------------------------------------------
trips = spark.read.parquet(processed_path).alias("t")

# -------------------------------------------------
# Read CURRENT SCD2 dimensions (DELTA)
# -------------------------------------------------
zones = (
    spark.read.format("delta")
    .load(zones_path)
    .filter(col("is_current") == True)
    .select(
        col("LocationID").alias("pickup_location_id"),
        col("zone_key"),
    )
    .alias("z")
)

vendors = (
    spark.read.format("delta")
    .load(vendors_path)
    .filter(col("is_current") == True)
    .select(
        col("VendorID"),
        col("vendor_key"),
    )
    .alias("v")
)

# -------------------------------------------------
# Join facts to dimensions (SAFE, EXPLICIT)
# -------------------------------------------------
fact = trips.join(
    zones,
    col("t.PULocationID") == col("z.pickup_location_id"),
    "left",
).join(
    vendors,
    col("t.VendorID") == col("v.VendorID"),
    "left",
)

# -------------------------------------------------
# Orphan detection (DATA QUALITY)
# -------------------------------------------------
fact = fact.withColumn(
    "zone_orphan_flag",
    when(col("zone_key").isNull(), lit(True)).otherwise(lit(False)),
).withColumn(
    "vendor_orphan_flag",
    when(col("vendor_key").isNull(), lit(True)).otherwise(lit(False)),
)

# -------------------------------------------------
# Final curated fact projection
# -------------------------------------------------
final_fact = fact.select(
    # Natural keys (traceability)
    col("t.VendorID").cast(IntegerType()).alias("VendorID"),
    col("t.PULocationID").alias("pickup_location_id"),
    col("t.DOLocationID").alias("dropoff_location_id"),
    # Surrogate keys (star schema)
    col("vendor_key"),
    col("zone_key").alias("pickup_zone_key"),
    # Dates & times
    to_date(col("t.tpep_pickup_datetime")).alias("trip_date"),
    col("t.tpep_pickup_datetime"),
    col("t.tpep_dropoff_datetime"),
    # Measures
    col("t.passenger_count"),
    col("t.trip_distance"),
    col("t.fare_amount"),
    col("t.total_amount"),
    col("t.payment_type"),
    # Data quality
    col("zone_orphan_flag"),
    col("vendor_orphan_flag"),
)

# -------------------------------------------------
# Write curated facts
# -------------------------------------------------
final_fact.write.mode("overwrite").parquet(curated_path)

# -------------------------------------------------
# Audit metrics
# -------------------------------------------------
total_rows = final_fact.count()
zone_orphans = final_fact.filter(col("zone_orphan_flag") == True).count()
vendor_orphans = final_fact.filter(col("vendor_orphan_flag") == True).count()

audit_df = spark.createDataFrame(
    [
        {
            "run_ts": datetime.utcnow().isoformat(),
            "rows_total": total_rows,
            "zone_orphan_rate": (zone_orphans / total_rows) if total_rows else 0,
            "vendor_orphan_rate": (vendor_orphans / total_rows) if total_rows else 0,
        }
    ]
)

audit_df.write.mode("append").json(audit_path)

spark.stop()
