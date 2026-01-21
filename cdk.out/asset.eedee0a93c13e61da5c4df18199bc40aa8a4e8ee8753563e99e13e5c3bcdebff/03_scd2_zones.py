import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, sha2, concat_ws, when
from pyspark.sql.types import IntegerType
from datetime import datetime
from delta.tables import DeltaTable

# -----------------------------------------------------
# Glue arguments
# -----------------------------------------------------
args = getResolvedOptions(
    sys.argv, ["BUCKET", "MASTER_PREFIX", "SCD2_PREFIX", "AUDIT_PREFIX"]
)

bucket = args["BUCKET"]

golden_zones_path = f"s3://{bucket}/{args['MASTER_PREFIX']}zones/"
scd2_zones_path = f"s3://{bucket}/{args['SCD2_PREFIX']}dim_zones/"
audit_path = f"s3://{bucket}/{args['AUDIT_PREFIX']}scd2_zones/"

# -----------------------------------------------------
# Spark
# -----------------------------------------------------
spark = (
    SparkSession.builder.appName("03_scd2_zones")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -----------------------------------------------------
# Read Golden Zones (MDM)
# -----------------------------------------------------
new_df = spark.read.option("header", True).csv(golden_zones_path)

# Enforce types
new_df = (
    new_df.withColumn("LocationID", col("LocationID").cast(IntegerType()))
    .withColumn("zone", col("zone"))
    .withColumn("borough", col("borough"))
)

# Compute hash diff (business columns ONLY)
hash_cols = ["LocationID", "zone", "borough"]

new_df = new_df.withColumn("hash_diff", sha2(concat_ws("||", *hash_cols), 256))

# -----------------------------------------------------
# Try reading existing SCD2 (if first run → empty)
# -----------------------------------------------------
try:
    old_df = spark.read.parquet(scd2_zones_path)
    scd2_exists = True
except Exception:
    scd2_exists = False

# -----------------------------------------------------
# First run (no history yet)
# -----------------------------------------------------
if not scd2_exists:
    final_df = (
        new_df.withColumn("zone_key", lit(None).cast("long"))
        .withColumn("version_number", lit(1))
        .withColumn("is_current", lit(True))
        .withColumn("start_date", current_timestamp())
        .withColumn("end_date", lit(None).cast("timestamp"))
        .write.format("delta")
        .mode("overwrite")
        .save(scd2_zones_path)
    )

    # final_df.write.mode("overwrite").parquet(scd2_zones_path)

    spark.stop()
    sys.exit(0)

# -----------------------------------------------------
# SCD2 CHANGE DETECTION
# -----------------------------------------------------
current_df = old_df.filter(col("is_current") == True)

# Rows where hash changed or new LocationID
changes_df = (
    new_df.alias("n")
    .join(current_df.alias("c"), on="LocationID", how="left")
    .filter((col("c.hash_diff").isNull()) | (col("n.hash_diff") != col("c.hash_diff")))
)
#
delta_table = DeltaTable.forPath(spark, scd2_zones_path)

(
    delta_table.alias("t")
    .merge(changes_df.alias("s"), "t.LocationID = s.LocationID AND t.is_current = true")
    .whenMatchedUpdate(set={"is_current": lit(False), "end_date": current_timestamp()})
    .whenNotMatchedInsert(
        values={
            "LocationID": col("s.LocationID"),
            "zone": col("s.zone"),
            "borough": col("s.borough"),
            "hash_diff": col("s.hash_diff"),
            "version_number": lit(1),
            "is_current": lit(True),
            "start_date": current_timestamp(),
            "end_date": lit(None).cast("timestamp"),
        }
    )
    .execute()
)

# -----------------------------------------------------
# Expire existing records
# -----------------------------------------------------
expired_df = (
    old_df.alias("o")
    .join(changes_df.select("LocationID").distinct(), on="LocationID", how="left")
    .withColumn(
        "is_current",
        when(col("is_current") & col("LocationID").isNotNull(), lit(False)).otherwise(
            col("is_current")
        ),
    )
    .withColumn(
        "end_date",
        when(col("is_current") == False, current_timestamp()).otherwise(
            col("end_date")
        ),
    )
)

# -----------------------------------------------------
# Insert new versions
# -----------------------------------------------------
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
    .select(expired_df.columns)
)

# -----------------------------------------------------
# Final SCD2 table
# -----------------------------------------------------
final_df = expired_df.unionByName(new_versions_df)

final_df.write.mode("overwrite").parquet(scd2_zones_path)

# -----------------------------------------------------
# Audit
# -----------------------------------------------------
audit_df = spark.createDataFrame(
    [
        {
            "run_ts": datetime.utcnow().isoformat(),
            "new_rows": new_versions_df.count(),
            "total_rows": final_df.count(),
        }
    ]
)

audit_df.write.mode("append").json(audit_path)

spark.stop()
