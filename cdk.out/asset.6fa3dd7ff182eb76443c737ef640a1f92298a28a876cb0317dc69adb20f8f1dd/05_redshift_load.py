import sys
import time
import json
from datetime import datetime

import boto3
from awsglue.utils import getResolvedOptions

# -------------------------
# Args
# -------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "BUCKET",
        "SCD2_PREFIX",
        "CURATED_PREFIX",
        "AUDIT_PREFIX",
        "REDSHIFT_WORKGROUP",
        "REDSHIFT_DB",
        "GLUE_CATALOG_DB",
    ],
)

# Optional thresholds
orphan_rate_max = float(args.get("ORPHAN_RATE_MAX", "0.01"))  # 1%
completeness_min = float(args.get("COMPLETENESS_MIN", "0.99"))  # 99%

bucket = args["BUCKET"]

scd2_zones_path = f"s3://{bucket}/{args['SCD2_PREFIX']}dim_zones/"
scd2_vendors_path = f"s3://{bucket}/{args['SCD2_PREFIX']}dim_vendors/"
fact_trips_path = f"s3://{bucket}/{args['CURATED_PREFIX']}curated_trips/"

audit_s3_prefix = f"{args['AUDIT_PREFIX']}redshift_load/"
audit_s3_path = f"s3://{bucket}/{audit_s3_prefix}"

wg = args["REDSHIFT_WORKGROUP"]
db = args["REDSHIFT_DB"]
glue_catalog_db = args["GLUE_CATALOG_DB"]

rs = boto3.client("redshift-data")
s3 = boto3.client("s3")


# -------------------------
# Redshift Data API helpers
# -------------------------
def exec_sql(sql: str) -> str:
    resp = rs.execute_statement(WorkgroupName=wg, Database=db, Sql=sql)
    return resp["Id"]


def wait(stmt_id: str, sleep_s: float = 2.0) -> None:
    while True:
        d = rs.describe_statement(Id=stmt_id)
        status = d["Status"]
        if status in ("FINISHED", "FAILED", "ABORTED"):
            if status != "FINISHED":
                raise RuntimeError(
                    f"Redshift statement failed ({status}): {d.get('Error')}"
                )
            return
        time.sleep(sleep_s)


def run(sql: str) -> None:
    sid = exec_sql(sql)
    wait(sid)


def query_scalar(sql: str) -> float:
    """Runs a query and returns the first column of the first row as float."""
    sid = exec_sql(sql)
    wait(sid)
    res = rs.get_statement_result(Id=sid)
    records = res.get("Records", [])
    if not records or not records[0]:
        return 0.0
    cell = records[0][0]
    # Redshift Data API returns typed dicts
    if "longValue" in cell:
        return float(cell["longValue"])
    if "doubleValue" in cell:
        return float(cell["doubleValue"])
    if "stringValue" in cell:
        return float(cell["stringValue"])
    return 0.0


# -------------------------
# 1) Schema + Tables (dist/sort per your plan)
# -------------------------
run("CREATE SCHEMA IF NOT EXISTS nyc_analytics;")

run("""
CREATE TABLE IF NOT EXISTS nyc_analytics.zone_dim (
  zone_key BIGINT IDENTITY(1,1),
  location_id INT NOT NULL,
  zone VARCHAR(256),
  borough VARCHAR(128),
  version_number INT,
  is_current BOOLEAN,
  start_date TIMESTAMP,
  end_date TIMESTAMP,
  hash_diff VARCHAR(64)
)
DISTSTYLE ALL
SORTKEY(location_id);
""")

run("""
CREATE TABLE IF NOT EXISTS nyc_analytics.vendor_dim (
  vendor_key BIGINT IDENTITY(1,1),
  vendor_code INT NOT NULL,
  vendor_name VARCHAR(256),
  active_flag BOOLEAN,
  version_number INT,
  is_current BOOLEAN,
  start_date TIMESTAMP,
  end_date TIMESTAMP,
  hash_diff VARCHAR(64)
)
DISTSTYLE ALL
SORTKEY(vendor_code);
""")

run("""
CREATE TABLE IF NOT EXISTS nyc_analytics.fact_trips (
  trip_id VARCHAR(64),
  vendor_code INT,
  pickup_location_id INT,
  dropoff_location_id INT,
  pickup_zone_key BIGINT,
  dropoff_zone_key BIGINT,
  trip_date DATE,
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  passenger_count INT,
  trip_distance DOUBLE PRECISION,
  fare_amount DOUBLE PRECISION,
  total_amount DOUBLE PRECISION,
  payment_type INT
)
DISTSTYLE KEY
DISTKEY(vendor_code)
SORTKEY(trip_date, pickup_zone_key);
""")

# -------------------------
# 2) Spectrum external schema (Glue Catalog)
# -------------------------
# This enables queries over curated data in S3 without loading, using Data Catalog.
# It is safe to run repeatedly.
run(f"""
CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum_curated
FROM DATA CATALOG
DATABASE '{glue_catalog_db}'
IAM_ROLE DEFAULT
CREATE EXTERNAL DATABASE IF NOT EXISTS;
""")

# -------------------------
# 3) Staging tables for idempotent loads
# -------------------------
run("DROP TABLE IF EXISTS nyc_analytics.zone_dim_stg;")
run("CREATE TABLE nyc_analytics.zone_dim_stg (LIKE nyc_analytics.zone_dim);")

run("DROP TABLE IF EXISTS nyc_analytics.vendor_dim_stg;")
run("CREATE TABLE nyc_analytics.vendor_dim_stg (LIKE nyc_analytics.vendor_dim);")

run("DROP TABLE IF EXISTS nyc_analytics.fact_trips_stg;")
run("CREATE TABLE nyc_analytics.fact_trips_stg (LIKE nyc_analytics.fact_trips);")

# -------------------------
# 4) COPY from explicit S3 paths (NO ambiguity)
# -------------------------
run(f"""
COPY nyc_analytics.zone_dim_stg
FROM '{scd2_zones_path}'
IAM_ROLE DEFAULT
FORMAT AS PARQUET;
""")

run(f"""
COPY nyc_analytics.vendor_dim_stg
FROM '{scd2_vendors_path}'
IAM_ROLE DEFAULT
FORMAT AS PARQUET;
""")

run(f"""
COPY nyc_analytics.fact_trips_stg
FROM '{fact_trips_path}'
IAM_ROLE DEFAULT
FORMAT AS PARQUET;
""")

# -------------------------
# 5) Swap staging -> prod inside a transaction
# -------------------------
run("BEGIN;")
run("TRUNCATE TABLE nyc_analytics.zone_dim;")
run("INSERT INTO nyc_analytics.zone_dim SELECT * FROM nyc_analytics.zone_dim_stg;")

run("TRUNCATE TABLE nyc_analytics.vendor_dim;")
run("INSERT INTO nyc_analytics.vendor_dim SELECT * FROM nyc_analytics.vendor_dim_stg;")

run("TRUNCATE TABLE nyc_analytics.fact_trips;")
run("INSERT INTO nyc_analytics.fact_trips SELECT * FROM nyc_analytics.fact_trips_stg;")
run("COMMIT;")

# -------------------------
# 6) DQ checks (fail job if governance thresholds violated)
# -------------------------
zone_cnt = int(query_scalar("SELECT COUNT(*) FROM nyc_analytics.zone_dim;"))
vendor_cnt = int(query_scalar("SELECT COUNT(*) FROM nyc_analytics.vendor_dim;"))
fact_cnt = int(query_scalar("SELECT COUNT(*) FROM nyc_analytics.fact_trips;"))

# Completeness: % fact rows with total_amount not null
completeness = query_scalar("""
SELECT
  CASE WHEN COUNT(*) = 0 THEN 0
       ELSE (SUM(CASE WHEN total_amount IS NOT NULL THEN 1 ELSE 0 END)::float / COUNT(*)::float)
  END
FROM nyc_analytics.fact_trips;
""")

# Orphan rate: facts referencing non-existent pickup_zone_key
orphan_rate = query_scalar("""
SELECT
  CASE WHEN COUNT(*) = 0 THEN 0
       ELSE (SUM(CASE WHEN z.zone_key IS NULL THEN 1 ELSE 0 END)::float / COUNT(*)::float)
  END
FROM nyc_analytics.fact_trips f
LEFT JOIN nyc_analytics.zone_dim z
  ON f.pickup_zone_key = z.zone_key;
""")

dq_failures = []
if zone_cnt == 0:
    dq_failures.append("zone_dim is empty")
if vendor_cnt == 0:
    dq_failures.append("vendor_dim is empty")
if fact_cnt == 0:
    dq_failures.append("fact_trips is empty")
if completeness < completeness_min:
    dq_failures.append(f"completeness {completeness:.4f} < {completeness_min}")
if orphan_rate > orphan_rate_max:
    dq_failures.append(f"orphan_rate {orphan_rate:.4f} > {orphan_rate_max}")

# -------------------------
# 7) Write audit JSON to S3
# -------------------------
audit_doc = {
    "run_ts_utc": datetime.utcnow().isoformat(),
    "sources": {
        "scd2_zones_path": scd2_zones_path,
        "scd2_vendors_path": scd2_vendors_path,
        "fact_trips_path": fact_trips_path,
    },
    "targets": {
        "schema": "nyc_analytics",
        "tables": ["zone_dim", "vendor_dim", "fact_trips"],
        "spectrum_schema": "spectrum_curated",
        "glue_catalog_db": glue_catalog_db,
    },
    "counts": {
        "zone_dim": zone_cnt,
        "vendor_dim": vendor_cnt,
        "fact_trips": fact_cnt,
    },
    "dq": {
        "completeness": completeness,
        "completeness_min": completeness_min,
        "orphan_rate": orphan_rate,
        "orphan_rate_max": orphan_rate_max,
        "failures": dq_failures,
    },
    "status": "FAILED" if dq_failures else "SUCCESS",
}

audit_key = f"{audit_s3_prefix}run_ts={audit_doc['run_ts_utc']}.json"
s3.put_object(
    Bucket=bucket,
    Key=audit_key,
    Body=json.dumps(audit_doc, indent=2).encode("utf-8"),
    ContentType="application/json",
)

# Fail the Glue job if DQ failed (after writing audit)
if dq_failures:
    raise RuntimeError("DQ check failures: " + "; ".join(dq_failures))

print("Redshift load + Spectrum + DQ checks complete.")
