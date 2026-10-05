"""Turn the zone-level demand forecast into warehouse x SKU reorder recommendations and stockout risk.

Zone forecast is split across the warehouses serving that zone by their share of the last 12 weeks of demand.
reorder_qty = forecast demand over (lead time + 4-week review) + safety stock - (on hand + open POs)
"""
import argparse

from pyspark.sql import SparkSession

p = argparse.ArgumentParser()
p.add_argument("--catalog", default="spiceroute")
args, _ = p.parse_known_args()
CAT = args.catalog
spark = SparkSession.builder.getOrCreate()
AS_OF = "2026-09-30"

spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.ml.reorder_recommendation
COMMENT 'Warehouse x SKU reorder recommendations and stockout risk as of {AS_OF}, from the Prophet demand forecast'
AS
WITH wh_share AS (           -- share of each zone's demand served by each warehouse (last 12 weeks)
  SELECT f.zone, f.warehouse_id, f.sku_id, sum(f.quantity) units
  FROM {CAT}.gold.fact_sales_line f
  WHERE f.order_date > date_sub(DATE'{AS_OF}', 84) AND f.order_status = 'Delivered'
  GROUP BY ALL),
share AS (SELECT *, units / sum(units) OVER (PARTITION BY zone, sku_id) AS share FROM wh_share),
fc AS (                      -- weekly forecast allocated to warehouses
  SELECT s.warehouse_id, d.sku_id, d.week_start, sum(d.forecast_units * s.share) AS fc_units,
         sum(d.forecast_upper * s.share) AS fc_upper
  FROM {CAT}.ml.demand_forecast d JOIN share s ON d.zone = s.zone AND d.sku_id = s.sku_id
  WHERE d.week_start >= DATE'2026-09-28'
  GROUP BY ALL),
hist AS (                    -- weekly demand variability (last 12 weeks)
  SELECT warehouse_id, sku_id, stddev(demand) sd_week, avg(demand) avg_week FROM (
    SELECT warehouse_id, sku_id, date_trunc('week', snapshot_date) wk, sum(demand_units) demand
    FROM {CAT}.gold.fact_inventory_daily WHERE snapshot_date > date_sub(DATE'{AS_OF}', 84) GROUP BY ALL) GROUP BY ALL),
stock AS (
  SELECT warehouse_id, sku_id, closing_units AS on_hand_units, stock_value_inr
  FROM {CAT}.gold.fact_inventory_daily WHERE snapshot_date = DATE'{AS_OF}'),
open_po AS (
  SELECT warehouse_id, sku_id, sum(ordered_units) open_po_units, min(expected_date) next_po_eta
  FROM {CAT}.gold.fact_purchase_orders WHERE received_date IS NULL OR received_date > DATE'{AS_OF}' GROUP BY ALL),
lt AS (
  SELECT warehouse_id, sku_id, greatest(1, round(avg(coalesce(actual_lead_days, 14)) / 7.0, 1)) lead_time_weeks
  FROM {CAT}.gold.fact_purchase_orders WHERE po_date > date_sub(DATE'{AS_OF}', 365) GROUP BY ALL),
agg AS (
  SELECT fc.warehouse_id, fc.sku_id,
         sum(CASE WHEN fc.week_start < date_add(DATE'2026-09-28', 28) THEN fc_units END) fc_next_4w,
         sum(CASE WHEN fc.week_start < date_add(DATE'2026-09-28', 84) THEN fc_units END) fc_next_12w,
         avg(fc_units) fc_avg_week,
         collect_list(struct(fc.week_start, fc_units)) weekly
  FROM fc GROUP BY ALL)
SELECT a.warehouse_id, w.warehouse_name, a.sku_id, p.product_name, p.base_spice, p.category,
       coalesce(s.on_hand_units, 0) on_hand_units, coalesce(o.open_po_units, 0) open_po_units, o.next_po_eta,
       round(a.fc_next_4w, 0) forecast_next_4w_units, round(a.fc_next_12w, 0) forecast_next_12w_units,
       coalesce(l.lead_time_weeks, 2) lead_time_weeks,
       round(1.65 * coalesce(h.sd_week, 0) * sqrt(coalesce(l.lead_time_weeks, 2)), 0) safety_stock_units,
       round(coalesce(s.on_hand_units, 0) / nullif(a.fc_avg_week, 0), 1) weeks_of_cover,
       greatest(0, round(a.fc_avg_week * (coalesce(l.lead_time_weeks, 2) + 4)
                         + 1.65 * coalesce(h.sd_week, 0) * sqrt(coalesce(l.lead_time_weeks, 2))
                         - coalesce(s.on_hand_units, 0) - coalesce(o.open_po_units, 0), 0)) recommended_order_units,
       CASE WHEN coalesce(s.on_hand_units, 0) / nullif(a.fc_avg_week, 0) < coalesce(l.lead_time_weeks, 2) THEN 'High'
            WHEN coalesce(s.on_hand_units, 0) / nullif(a.fc_avg_week, 0) < coalesce(l.lead_time_weeks, 2) + 2 THEN 'Medium'
            ELSE 'Low' END stockout_risk,
       date_add(DATE'{AS_OF}', CAST(7 * coalesce(s.on_hand_units, 0) / nullif(a.fc_avg_week, 0) AS INT)) projected_stockout_date,
       round(a.fc_next_4w * p.mrp_inr * 0.7 / (1 + p.gst_rate), 0) forecast_revenue_4w_inr,
       DATE'{AS_OF}' as_of_date
FROM agg a
JOIN {CAT}.gold.dim_product p ON a.sku_id = p.sku_id
JOIN {CAT}.gold.dim_warehouse w ON a.warehouse_id = w.warehouse_id
LEFT JOIN stock s ON a.warehouse_id = s.warehouse_id AND a.sku_id = s.sku_id
LEFT JOIN open_po o ON a.warehouse_id = o.warehouse_id AND a.sku_id = o.sku_id
LEFT JOIN hist h ON a.warehouse_id = h.warehouse_id AND a.sku_id = h.sku_id
LEFT JOIN lt l ON a.warehouse_id = l.warehouse_id AND a.sku_id = l.sku_id
""")

# Planner overrides captured by the Demand Planner app (kept across runs)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CAT}.ml.reorder_overrides (
  warehouse_id STRING, sku_id STRING, recommended_order_units DOUBLE, override_units DOUBLE,
  decision STRING COMMENT 'Approved / Rejected / Modified', comment STRING, decided_by STRING, decided_at TIMESTAMP)
COMMENT 'Planner decisions on reorder recommendations (written by the SpiceRoute Demand Planner app)'
""")
print(spark.sql(f"SELECT stockout_risk, count(*) n FROM {CAT}.ml.reorder_recommendation GROUP BY 1").collect())
