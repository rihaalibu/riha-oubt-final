# 01_validate_clean.
import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, current_timestamp

# -------------------------------------------------
# Arguments
# -------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "BUCKET",
        "RAW_PREFIX",
        "VALIDATED_PREFIX",
        "QUARANTINE_PREFIX",
        "AUDIT_PREFIX",
        "BACKUP_PREFIX",
    ],
)

bucket = args["BUCKET"]

raw_path = f"s3://{bucket}/{args['RAW_PREFIX']}"
validated_path = f"s3://{bucket}/{args['VALIDATED_PREFIX']}nyc_taxi/"
quarantine_path = f"s3://{bucket}/{args['QUARANTINE_PREFIX']}validate_clean/"
audit_path = f"s3://{bucket}/{args['AUDIT_PREFIX']}validate_clean/"
backup_path = f"s3://{bucket}/{args['BACKUP_PREFIX']}raw_snapshot/"

spark = SparkSession.builder.appName("01_validate_clean").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(raw_path)

total_rows = df.count()

required_columns = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "PULocationID",
    "DOLocationID",
    "payment_type",
]

missing_cols = [c for c in required_columns if c not in df.columns]
if missing_cols:
    raise ValueError(f"Missing required columns: {missing_cols}")


validated = df.withColumn(
    "is_valid",
    when(col("tpep_pickup_datetime").isNull(), lit(False))
    .when(col("tpep_dropoff_datetime").isNull(), lit(False))
    .when(col("tpep_dropoff_datetime") <= col("tpep_pickup_datetime"), lit(False))
    .when(col("passenger_count") < 1, lit(False))
    .when(col("trip_distance") < 0, lit(False))
    .when(col("fare_amount") < 0, lit(False))
    .when(col("total_amount") < 0, lit(False))
    .otherwise(lit(True)),
)

valid_df = validated.filter(col("is_valid") == True).drop("is_valid")
invalid_df = validated.filter(col("is_valid") == False).drop("is_valid")

valid_count = valid_df.count()
invalid_count = invalid_df.count()


df.write.mode("append").parquet(backup_path)


valid_df.write.mode("overwrite").parquet(validated_path)
invalid_df.write.mode("overwrite").parquet(quarantine_path)

# -------------------------------------------------
# Audit record
# -------------------------------------------------
audit_record = spark.createDataFrame(
    [
        {
            "run_ts": datetime.utcnow().isoformat(),
            "rows_in": total_rows,
            "rows_valid": valid_count,
            "rows_quarantined": invalid_count,
            "valid_pct": (valid_count / total_rows) if total_rows else 0.0,
        }
    ]
)

audit_record.write.mode("append").json(audit_path)

# -------------------------------------------------
# Fail pipeline if quality breach
# -------------------------------------------------
# You can tune this threshold if required
if total_rows > 0 and (valid_count / total_rows) < 0.90:
    raise RuntimeError(
        f"Validation failed: valid_pct={(valid_count / total_rows):.2%} < 95%"
    )

spark.stop()
