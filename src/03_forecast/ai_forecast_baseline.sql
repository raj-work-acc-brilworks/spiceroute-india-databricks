-- Baseline forecast with Databricks built-in ai_forecast() (runs on the SQL warehouse).
-- Same grain/horizon as the Prophet model so the two can be compared side by side.
CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.ml.ai_forecast_baseline')
COMMENT 'Weekly SKU x zone units forecast from ai_forecast() - SQL-native baseline for the Prophet model'
AS SELECT split(series_id, '\\|')[0] AS sku_id, split(series_id, '\\|')[1] AS zone, CAST(week_start AS DATE) AS week_start,
          round(greatest(units_forecast, 0), 1) AS forecast_units, round(greatest(units_lower, 0), 1) AS forecast_lower,
          round(greatest(units_upper, 0), 1) AS forecast_upper, current_timestamp() AS forecast_created_at
FROM ai_forecast(
  TABLE(SELECT concat(sku_id, '|', zone) AS series_id, CAST(week_start AS TIMESTAMP) AS week_start, CAST(units AS DOUBLE) AS units
        FROM IDENTIFIER(:catalog || '.gold.agg_sales_weekly_sku_zone') WHERE week_start <= DATE'2026-09-21'),
  horizon => '2026-12-21',
  time_col => 'week_start',
  value_col => 'units',
  group_col => 'series_id',
  frequency => 'week',
  prediction_interval_width => 0.8
);
