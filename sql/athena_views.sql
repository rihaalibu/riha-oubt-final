CREATE OR REPLACE VIEW riha_analytics.curated_daily_revenue AS
SELECT trip_date, SUM(total_amount) revenue
FROM riha_analytics.curated_trips
GROUP BY trip_date;
