# 02b_create_golden_zones.py

import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    upper,
    when,
    length,
    row_number,
)
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

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
zones = (
    zones.withColumn("LocationID", col("LocationID").cast(IntegerType()))
    .withColumn("Zone", trim(col("Zone")))
    .withColumn("Borough", trim(col("Borough")))
)

# Fail fast on missing business keys
if zones.filter(col("LocationID").isNull()).count() > 0:
    raise ValueError("Found NULL LocationID in zones")

# -------------------------------------------------
# Deterministic standardization (MATCHING ENGINE)
# -------------------------------------------------
zones = zones.withColumn("Zone_norm", upper(col("Zone"))).withColumn(
    "Borough_norm",
    when(
        col("Borough").isNull() | (col("Borough") == ""),
        lit("OUTSIDE NYC"),
    ).otherwise(upper(col("Borough"))),
)

zones = zones.withColumn(
    "Borough_norm",
    when(
        col("Borough_norm").isin("UNKNOWN", "N/A", "NA"), lit("OUTSIDE NYC")
    ).otherwise(col("Borough_norm")),
)

# -------------------------------------------------
# Survivorship rules (Golden Record selection)
# -------------------------------------------------
# Rule: prefer longest Zone name per LocationID
window_spec = Window.partitionBy("LocationID").orderBy(length(col("Zone_norm")).desc())

golden = (
    zones.withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .select(
        col("LocationID"),
        col("Zone_norm").alias("zone"),
        col("Borough_norm").alias("borough"),
    )
)

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
            "matching_type": "deterministic",
            "survivorship": "longest zone name",
        }
    ]
)

audit_df.write.mode("append").json(audit_path)
spark.stop()
