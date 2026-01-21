# 02_process_trips.py
# Placeholder in CDK-only bundle.
import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    lit,
    unix_timestamp,
    when,
    round as spark_round,
)
from pyspark.sql.types import IntegerType, DoubleType, TimestampType

# -------------------------------------------------
# Glue arguments
# -------------------------------------------------
args = getResolvedOptions(
    sys.argv, ["BUCKET", "VALIDATED_PREFIX", "PROCESSED_PREFIX", "AUDIT_PREFIX"]
)

bucket = args["BUCKET"]

validated_path = f"s3://{bucket}/{args['VALIDATED_PREFIX']}nyc_taxi/"
processed_path = f"s3://{bucket}/{args['PROCESSED_PREFIX']}nyc_taxi/"
audit_path = f"s3://{bucket}/{args['AUDIT_PREFIX']}process_trips/"

# -------------------------------------------------
# Spark
# -------------------------------------------------
spark = SparkSession.builder.appName("02_process_trips").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------
# Read validated data
# -------------------------------------------------
df = spark.read.parquet(validated_path)
rows_in = df.count()

# -------------------------------------------------
# Enforce types & normalize timestamps
# -------------------------------------------------
df = (
    df.withColumn(
        "tpep_pickup_datetime", col("tpep_pickup_datetime").cast(TimestampType())
    )
    .withColumn(
        "tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast(TimestampType())
    )
    .withColumn("passenger_count", col("passenger_count").cast(IntegerType()))
    .withColumn("trip_distance", col("trip_distance").cast(DoubleType()))
    .withColumn("fare_amount", col("fare_amount").cast(DoubleType()))
    .withColumn("total_amount", col("total_amount").cast(DoubleType()))
)

# -------------------------------------------------
# Drop invalid time relationships (defensive)
# -------------------------------------------------
df = df.filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))

# -------------------------------------------------
# Deduplicate
# -------------------------------------------------
df = df.dropDuplicates(["VendorID", "tpep_pickup_datetime", "PULocationID"])

rows_after_dedupe = df.count()

# -------------------------------------------------
# Derived columns
# -------------------------------------------------
df = (
    df.withColumn("trip_date", to_date(col("tpep_pickup_datetime")))
    .withColumn(
        "trip_duration_minutes",
        (
            unix_timestamp("tpep_dropoff_datetime")
            - unix_timestamp("tpep_pickup_datetime")
        )
        / 60.0,
    )
    .withColumn(
        "trip_duration_hours",
        (
            unix_timestamp("tpep_dropoff_datetime")
            - unix_timestamp("tpep_pickup_datetime")
        )
        / 3600.0,
    )
)

# Protect against divide-by-zero / bad durations
df = df.filter(col("trip_duration_minutes") > 0)

df = df.withColumn(
    "speed_mph",
    when(
        col("trip_duration_hours") > 0,
        spark_round(col("trip_distance") / col("trip_duration_hours"), 2),
    ).otherwise(lit(None)),
)

# -------------------------------------------------
# Final schema selection (explicit)
# -------------------------------------------------
final_df = df.select(
    "VendorID",
    "PULocationID",
    "DOLocationID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_date",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "payment_type",
    "trip_duration_minutes",
    "speed_mph",
)

rows_out = final_df.count()

# -------------------------------------------------
# Write processed data
# -------------------------------------------------
final_df.write.mode("overwrite").parquet(processed_path)

# -------------------------------------------------
# Audit
# -------------------------------------------------
audit_df = spark.createDataFrame(
    [
        {
            "run_ts": datetime.utcnow().isoformat(),
            "rows_in": rows_in,
            "rows_after_dedupe": rows_after_dedupe,
            "rows_out": rows_out,
            "dedupe_reduction_pct": (
                (rows_in - rows_after_dedupe) / rows_in if rows_in else 0.0
            ),
        }
    ]
)

audit_df.write.mode("append").json(audit_path)

spark.stop()
