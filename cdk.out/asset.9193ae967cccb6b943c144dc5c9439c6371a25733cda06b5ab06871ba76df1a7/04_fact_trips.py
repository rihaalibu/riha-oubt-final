# 04_fact_trips.py
# Placeholder in CDK-only bundle.
import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit, when
from pyspark.sql.types import IntegerType, LongType

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

spark = SparkSession.builder.appName("04_fact_trips").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


trips = spark.read.parquet(processed_path)


zones = (
    spark.read.parquet(zones_path)
    .filter(col("is_current") == True)
    .select(
        col("LocationID").alias("pickup_location_id"), col("zone_key").cast(LongType())
    )
)

vendors = (
    spark.read.parquet(vendors_path)
    .filter(col("is_current") == True)
    .select(col("VendorID").cast(IntegerType()), col("vendor_key").cast(LongType()))
)

fact = trips.join(zones, trips.PULocationID == zones.pickup_location_id, "left").join(
    vendors, trips.VendorID == vendors.VendorID, "left"
)

fact = fact.withColumn(
    "zone_orphan_flag", when(col("zone_key").isNull(), lit(True)).otherwise(lit(False))
).withColumn(
    "vendor_orphan_flag",
    when(col("vendor_key").isNull(), lit(True)).otherwise(lit(False)),
)

final_fact = fact.select(
    col("VendorID").cast(IntegerType()).alias("VendorID"),
    col("vendor_key"),
    col("PULocationID").alias("pickup_location_id"),
    col("DOLocationID").alias("dropoff_location_id"),
    col("zone_key").alias("pickup_zone_key"),
    to_date(col("tpep_pickup_datetime")).alias("trip_date"),
    col("tpep_pickup_datetime"),
    col("tpep_dropoff_datetime"),
    col("passenger_count"),
    col("trip_distance"),
    col("fare_amount"),
    col("total_amount"),
    col("payment_type"),
    col("zone_orphan_flag"),
    col("vendor_orphan_flag"),
)

final_fact.write.mode("overwrite").parquet(curated_path)
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
