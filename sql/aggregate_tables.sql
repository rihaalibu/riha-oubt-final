-- =========================================
-- DAILY REVENUE BY ZONE
-- =========================================
CREATE TABLE IF NOT EXISTS riha_analytics.agg_daily_zone_revenue (
  trip_date DATE,
  pickup_zone_key BIGINT,
  trip_count BIGINT,
  total_revenue DOUBLE PRECISION,
  avg_fare DOUBLE PRECISION
)
DISTSTYLE KEY
DISTKEY(pickup_zone_key)
SORTKEY(trip_date);

-- =========================================
-- DAILY VENDOR PERFORMANCE
-- =========================================
CREATE TABLE IF NOT EXISTS riha_analytics.agg_daily_vendor_perf (
  trip_date DATE,
  vendor_code INT,
  trip_count BIGINT,
  total_revenue DOUBLE PRECISION,
  avg_trip_distance DOUBLE PRECISION
)
DISTSTYLE KEY
DISTKEY(vendor_code)
SORTKEY(trip_date);
