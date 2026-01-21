# 06_governance_metrics.py
# Placeholder in CDK-only bundle.
import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DoubleType

# -------------------------------------------------
# Arguments
# -------------------------------------------------
args = getResolvedOptions(
    sys.argv, ["BUCKET", "AUDIT_PREFIX", "CURATED_PREFIX", "SCD2_PREFIX"]
)

bucket = args["BUCKET"]

audit_out_path = f"s3://{bucket}/{args['AUDIT_PREFIX']}governance_metrics/"
fact_path = f"s3://{bucket}/{args['CURATED_PREFIX']}fact_trips/"
zones_path = f"s3://{bucket}/{args['SCD2_PREFIX']}dim_zones/"
vendors_path = f"s3://{bucket}/{args['SCD2_PREFIX']}dim_vendors/"

# -------------------------------------------------
# Spark
# -------------------------------------------------
spark = SparkSession.builder.appName("06_governance_metrics").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------
# Load curated facts
# -------------------------------------------------
fact = spark.read.parquet(fact_path)
total_facts = fact.count()

# -------------------------------------------------
# Orphan rates
# -------------------------------------------------
zone_orphans = fact.filter(col("zone_orphan_flag") == True).count()
vendor_orphans = fact.filter(col("vendor_orphan_flag") == True).count()

zone_orphan_rate = (zone_orphans / total_facts) if total_facts else 0.0
vendor_orphan_rate = (vendor_orphans / total_facts) if total_facts else 0.0

# -------------------------------------------------
# Completeness checks
# -------------------------------------------------
required_fact_fields = [
    "vendor_key",
    "pickup_zone_key",
    "trip_date",
    "fare_amount",
    "total_amount",
]

null_expr = None
for f in required_fact_fields:
    expr = col(f).isNull()
    null_expr = expr if null_expr is None else (null_expr | expr)

null_rows = fact.filter(null_expr).count()
completeness_pct = 1.0 - (null_rows / total_facts) if total_facts else 1.0

# -------------------------------------------------
# Dimension health
# -------------------------------------------------
zones_current = spark.read.parquet(zones_path).filter(col("is_current") == True).count()

vendors_current = (
    spark.read.parquet(vendors_path).filter(col("is_current") == True).count()
)

# -------------------------------------------------
# Governance status
# -------------------------------------------------
pipeline_status = (
    "PASS"
    if completeness_pct >= 0.99
    and zone_orphan_rate <= 0.01
    and vendor_orphan_rate <= 0.01
    else "FAIL"
)

# -------------------------------------------------
# Final metrics record
# -------------------------------------------------
metrics = spark.createDataFrame(
    [
        {
            "run_ts_utc": datetime.utcnow().isoformat(),
            "fact_count": total_facts,
            "completeness_pct": float(completeness_pct),
            "zone_orphan_rate": float(zone_orphan_rate),
            "vendor_orphan_rate": float(vendor_orphan_rate),
            "current_zones": zones_current,
            "current_vendors": vendors_current,
            "pipeline_status": pipeline_status,
        }
    ]
)

# -------------------------------------------------
# Write governance metrics
# -------------------------------------------------
metrics.write.mode("append").json(audit_out_path)

spark.stop()
