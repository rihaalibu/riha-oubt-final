CREATE SCHEMA IF NOT EXISTS riha_analytics;

CREATE TABLE riha_analytics.zone_dim (
  zone_key BIGINT IDENTITY,
  location_id INT,
  zone VARCHAR(200),
  borough VARCHAR(100),
  is_current BOOLEAN,
  start_date TIMESTAMP,
  end_date TIMESTAMP
)
DISTSTYLE ALL
SORTKEY(location_id);

CREATE TABLE riha_analytics.fact_trips (
  trip_id VARCHAR(64),
  vendor_code INT,
  pickup_zone_key BIGINT,
  trip_date DATE,
  total_amount FLOAT
)
DISTKEY(vendor_code)
SORTKEY(trip_date, pickup_zone_key);
