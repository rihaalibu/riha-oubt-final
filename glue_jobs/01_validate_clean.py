# 01_validate_clean.py
import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

# Arguments
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

# Spark (UNCHANGED)
spark = SparkSession.builder.appName("01_validate_clean").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# READ RAW PARQUET (UNCHANGED)
df = spark.read.parquet(raw_path)

# REQUIRED COLUMNS
REQUIRED_COLS = [
    "vendorid",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "pulocationid",
    "dolocationid",
    "payment_type",
    "fare_amount",
    "total_amount",
]

# NORMALIZE TYPES
df = (
    df.withColumn("vendorid", col("vendorid").cast("int"))
    .withColumn("pulocationid", col("pulocationid").cast("int"))
    .withColumn("dolocationid", col("dolocationid").cast("int"))
    .withColumn("payment_type", col("payment_type").cast("int"))
    .withColumn("trip_distance", col("trip_distance").cast("double"))
    .withColumn("fare_amount", col("fare_amount").cast("double"))
    .withColumn("total_amount", col("total_amount").cast("double"))
)

total_rows = df.count()

# Validation rules
validated = df.withColumn(
    "is_valid",
    when(col("vendorid").isNull(), lit(False))
    .when(col("tpep_pickup_datetime").isNull(), lit(False))
    .when(col("tpep_dropoff_datetime").isNull(), lit(False))
    .when(col("tpep_dropoff_datetime") <= col("tpep_pickup_datetime"), lit(False))
    .when(col("trip_distance") < 0, lit(False))
    .when(col("fare_amount") < 0, lit(False))
    .when(col("total_amount") < 0, lit(False))
    .when(col("pulocationid").isNull(), lit(False))
    .when(col("dolocationid").isNull(), lit(False))
    .otherwise(lit(True)),
)

# Split valid / invalid (UNCHANGED LOGIC)
valid_rows = validated.filter(col("is_valid") == True)
invalid_rows = validated.filter(col("is_valid") == False)

# COLUMN PROJECTION
# Validated → ONLY required columns
valid_df = valid_rows.select(*REQUIRED_COLS)

# Quarantine → FULL original row
invalid_df = invalid_rows.drop("is_valid")

valid_count = valid_df.count()
invalid_count = invalid_df.count()

# Backup raw snapshot
df.write.mode("append").parquet(backup_path)

# Write outputs
valid_df.write.mode("overwrite").parquet(validated_path)
invalid_df.write.mode("overwrite").parquet(quarantine_path)

# Audit
audit_df = spark.createDataFrame(
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

audit_df.write.mode("append").json(audit_path)

# Fail if quality breach

if total_rows > 0 and (valid_count / total_rows) < 0.90:
    raise RuntimeError(
        f"Validation failed: valid_pct={(valid_count / total_rows):.2%} < 90%"
    )

spark.stop()
