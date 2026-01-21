import sys
import time
import json
from datetime import datetime

import boto3
from awsglue.utils import getResolvedOptions

# Args
args = getResolvedOptions(
    sys.argv,
    [
        "BUCKET",
        "SCD2_PREFIX",
        "CURATED_PREFIX",
        "AUDIT_PREFIX",
        "REDSHIFT_WORKGROUP",
        "REDSHIFT_DB",
        "REDSHIFT_IAM_ROLE_ARN",
    ],
)

bucket = args["BUCKET"]
wg = args["REDSHIFT_WORKGROUP"]
db = args["REDSHIFT_DB"]
rs_role = args["REDSHIFT_IAM_ROLE_ARN"]

scd2_zones_path = f"s3://{bucket}/{args['SCD2_PREFIX']}dim_zones/"
scd2_vendors_path = f"s3://{bucket}/{args['SCD2_PREFIX']}dim_vendors/"
fact_trips_path = f"s3://{bucket}/{args['CURATED_PREFIX']}curated_trips/"
audit_path = f"s3://{bucket}/{args['AUDIT_PREFIX']}redshift_load/"

rs = boto3.client("redshift-data")
s3 = boto3.client("s3")


# Helpers
def exec_sql(sql):
    return rs.execute_statement(
        WorkgroupName=wg,
        Database=db,
        Sql=sql,
    )["Id"]


def wait(stmt_id):
    while True:
        d = rs.describe_statement(Id=stmt_id)
        if d["Status"] in ("FINISHED", "FAILED", "ABORTED"):
            if d["Status"] != "FINISHED":
                raise RuntimeError(d.get("Error"))
            return
        time.sleep(2)


def run(sql):
    wait(exec_sql(sql))


def scalar(sql):
    sid = exec_sql(sql)
    wait(sid)
    res = rs.get_statement_result(Id=sid)
    if not res["Records"]:
        return 0
    cell = res["Records"][0][0]
    return float(list(cell.values())[0])


# Schema & Tables
run("CREATE SCHEMA IF NOT EXISTS nyc_analytics;")

run("""
CREATE TABLE IF NOT EXISTS nyc_analytics.zone_dim (
  zone_key VARCHAR(64),
  location_id INT,
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
  vendor_key VARCHAR(64),
  vendorid INT,
  vendor_name VARCHAR(256),
  version_number INT,
  is_current BOOLEAN,
  start_date TIMESTAMP,
  end_date TIMESTAMP,
  hash_diff VARCHAR(64)
)
DISTSTYLE ALL
SORTKEY(vendorid);
""")

run("""
CREATE TABLE IF NOT EXISTS nyc_analytics.fact_trips (
  vendorid INT,
  pickup_location_id INT,
  dropoff_location_id INT,
  vendor_key VARCHAR(64),
  pickup_zone_key VARCHAR(64),
  trip_date DATE,
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  trip_distance DOUBLE PRECISION,
  fare_amount DOUBLE PRECISION,
  total_amount DOUBLE PRECISION,
  payment_type INT,
  trip_duration_minutes DOUBLE PRECISION,
  speed_mph DOUBLE PRECISION,
  zone_orphan_flag BOOLEAN,
  vendor_orphan_flag BOOLEAN
)
DISTSTYLE KEY
DISTKEY(vendorid)
SORTKEY(trip_date);
""")

# Staging tables
run("DROP TABLE IF EXISTS nyc_analytics.zone_dim_stg;")
run("CREATE TABLE nyc_analytics.zone_dim_stg (LIKE nyc_analytics.zone_dim);")

run("DROP TABLE IF EXISTS nyc_analytics.vendor_dim_stg;")
run("CREATE TABLE nyc_analytics.vendor_dim_stg (LIKE nyc_analytics.vendor_dim);")

run("DROP TABLE IF EXISTS nyc_analytics.fact_trips_stg;")
run("CREATE TABLE nyc_analytics.fact_trips_stg (LIKE nyc_analytics.fact_trips);")

# COPY from S3 (Parquet)
run(f"""
COPY nyc_analytics.zone_dim_stg
FROM '{scd2_zones_path}'
IAM_ROLE '{rs_role}'
FORMAT AS PARQUET;
""")

run(f"""
COPY nyc_analytics.vendor_dim_stg
FROM '{scd2_vendors_path}'
IAM_ROLE '{rs_role}'
FORMAT AS PARQUET;
""")

run(f"""
COPY nyc_analytics.fact_trips_stg
FROM '{fact_trips_path}'
IAM_ROLE '{rs_role}'
FORMAT AS PARQUET;
""")

# Atomic swap
run("BEGIN;")
run("TRUNCATE nyc_analytics.zone_dim;")
run("INSERT INTO nyc_analytics.zone_dim SELECT * FROM nyc_analytics.zone_dim_stg;")

run("TRUNCATE nyc_analytics.vendor_dim;")
run("INSERT INTO nyc_analytics.vendor_dim SELECT * FROM nyc_analytics.vendor_dim_stg;")

run("TRUNCATE nyc_analytics.fact_trips;")
run("INSERT INTO nyc_analytics.fact_trips SELECT * FROM nyc_analytics.fact_trips_stg;")
run("COMMIT;")

# DQ checks
zone_cnt = scalar("SELECT COUNT(*) FROM nyc_analytics.zone_dim;")
vendor_cnt = scalar("SELECT COUNT(*) FROM nyc_analytics.vendor_dim;")
fact_cnt = scalar("SELECT COUNT(*) FROM nyc_analytics.fact_trips;")

orphan_rate = scalar("""
SELECT
  SUM(CASE WHEN zone_orphan_flag THEN 1 ELSE 0 END)::float
  / NULLIF(COUNT(*),0)
FROM nyc_analytics.fact_trips;
""")

status = "SUCCESS"
errors = []

if zone_cnt == 0:
    errors.append("zone_dim empty")
if vendor_cnt == 0:
    errors.append("vendor_dim empty")
if fact_cnt == 0:
    errors.append("fact_trips empty")
if orphan_rate > 0.01:
    errors.append(f"zone orphan rate {orphan_rate:.4f}")

if errors:
    status = "FAILED"

# Audit
audit = {
    "run_ts_utc": datetime.utcnow().isoformat(),
    "counts": {
        "zone_dim": int(zone_cnt),
        "vendor_dim": int(vendor_cnt),
        "fact_trips": int(fact_cnt),
    },
    "zone_orphan_rate": orphan_rate,
    "status": status,
    "errors": errors,
}

s3.put_object(
    Bucket=bucket,
    Key=f"{args['AUDIT_PREFIX']}redshift_load/run_ts={audit['run_ts_utc']}.json",
    Body=json.dumps(audit, indent=2),
    ContentType="application/json",
)

if status == "FAILED":
    raise RuntimeError(str(errors))

print("Redshift load completed successfully")
