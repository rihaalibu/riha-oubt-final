#!/usr/bin/env python3
# -------------------------------------------------------
# 8) Rebuild aggregate tables (post-DQ)
# -------------------------------------------------------
cloudwatch = boto3.client("cloudwatch")

run("TRUNCATE TABLE nyc_analytics.agg_daily_zone_revenue;")

run("""
INSERT INTO nyc_analytics.agg_daily_zone_revenue
SELECT
  trip_date,
  pickup_zone_key,
  COUNT(*) AS trip_count,
  SUM(total_amount) AS total_revenue,
  AVG(fare_amount) AS avg_fare
FROM nyc_analytics.fact_trips
GROUP BY trip_date, pickup_zone_key;
""")

run("TRUNCATE TABLE nyc_analytics.agg_daily_vendor_perf;")

run("""
INSERT INTO nyc_analytics.agg_daily_vendor_perf
SELECT
  trip_date,
  vendor_code,
  COUNT(*) AS trip_count,
  SUM(total_amount) AS total_revenue,
  AVG(trip_distance) AS avg_trip_distance
FROM nyc_analytics.fact_trips
GROUP BY trip_date, vendor_code;
""")


def put_metric(name, value, unit="None"):
    cloudwatch.put_metric_data(
        Namespace="NYCTaxi/Governance",
        MetricData=[{"MetricName": name, "Value": value, "Unit": unit}],
    )


put_metric("FactRowCount", fact_cnt, "Count")
put_metric("ZoneDimRowCount", zone_cnt, "Count")
put_metric("VendorDimRowCount", vendor_cnt, "Count")

put_metric("CompletenessPct", completeness * 100, "Percent")
put_metric("OrphanRatePct", orphan_rate * 100, "Percent")

# Binary health indicator
put_metric("PipelineDQStatus", 0 if dq_failures else 1, "Count")
