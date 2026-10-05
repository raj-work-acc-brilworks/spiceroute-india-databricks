-- =====================================================================================
-- GOLD AGGREGATES — what dashboards, Genie, the app and the forecast read.
-- All exclude cancelled orders.
-- =====================================================================================

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.agg_sales_daily
COMMENT 'Grain: day x SKU x zone x channel. Main source for dashboards and Genie.'
CLUSTER BY (order_date, zone)
AS SELECT f.order_date, f.sku_id, f.category, f.base_spice, f.zone, f.channel_code, c.channel_name, c.channel_group,
          count(*) AS lines, count(DISTINCT f.order_id) AS orders, sum(f.quantity) AS units, round(sum(f.volume_kg), 2) AS volume_kg,
          round(sum(f.gross_amount_incl_gst), 2) AS gross_amount_incl_gst, round(sum(f.net_revenue_inr), 2) AS net_revenue_inr,
          round(sum(f.discount_inr), 2) AS discount_inr, round(sum(f.cogs_inr), 2) AS cogs_inr,
          round(sum(f.gross_margin_inr), 2) AS gross_margin_inr, round(sum(f.gst_inr), 2) AS gst_inr,
          round(sum(CASE WHEN f.promo_code IS NOT NULL THEN f.net_revenue_inr ELSE 0 END), 2) AS promo_revenue_inr,
          sum(CASE WHEN f.is_returned THEN 1 ELSE 0 END) AS returned_lines
FROM ${catalog}.gold.fact_sales_line f JOIN ${catalog}.silver.channels c USING (channel_code)
WHERE f.order_status = 'Delivered'
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.agg_sales_monthly
COMMENT 'Grain: month x category x state x channel. Executive trends.'
AS SELECT CAST(trunc(order_date, 'month') AS DATE) AS month, d.fiscal_year, f.category, f.zone, f.state, f.channel_code,
          count(DISTINCT f.order_id) AS orders, sum(f.quantity) AS units, round(sum(f.volume_kg), 2) AS volume_kg,
          round(sum(f.net_revenue_inr), 2) AS net_revenue_inr, round(sum(f.gross_margin_inr), 2) AS gross_margin_inr,
          round(sum(f.discount_inr), 2) AS discount_inr
FROM ${catalog}.gold.fact_sales_line f JOIN ${catalog}.gold.dim_date d ON f.order_date = d.date
WHERE f.order_status = 'Delivered'
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.agg_sales_weekly_sku_zone
COMMENT 'Grain: ISO week (Monday) x SKU x zone. Training input for demand forecasting (units).'
AS SELECT CAST(date_trunc('week', order_date) AS DATE) AS week_start, sku_id, zone,
          sum(units) AS units, round(sum(net_revenue_inr), 2) AS net_revenue_inr,
          round(sum(promo_revenue_inr) / nullif(sum(net_revenue_inr), 0), 4) AS promo_share
FROM ${catalog}.gold.agg_sales_daily GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.agg_promo_effectiveness
COMMENT 'Per promotion: daily revenue during promo vs 28-day pre-promo baseline in the same channel'
AS WITH ch_day AS (
  SELECT order_date, channel_code, sum(net_revenue_inr) rev, sum(discount_inr) disc
  FROM ${catalog}.gold.agg_sales_daily GROUP BY ALL),
p AS (SELECT * FROM ${catalog}.silver.promotions WHERE start_date <= DATE'2026-09-30')
SELECT p.promo_id, p.promo_code, p.promo_name, p.promo_type, p.channel_code, p.category_scope, p.festival_name, p.discount_pct,
       p.start_date, p.end_date,
       round(avg(CASE WHEN d.order_date BETWEEN p.start_date AND p.end_date THEN d.rev END), 2) AS promo_daily_revenue_inr,
       round(avg(CASE WHEN d.order_date BETWEEN date_sub(p.start_date, 28) AND date_sub(p.start_date, 1) THEN d.rev END), 2) AS baseline_daily_revenue_inr,
       round(sum(CASE WHEN d.order_date BETWEEN p.start_date AND p.end_date THEN d.disc END), 2) AS discount_cost_inr,
       round(100 * (avg(CASE WHEN d.order_date BETWEEN p.start_date AND p.end_date THEN d.rev END) /
             nullif(avg(CASE WHEN d.order_date BETWEEN date_sub(p.start_date, 28) AND date_sub(p.start_date, 1) THEN d.rev END), 0) - 1), 1) AS revenue_lift_pct
FROM p JOIN ch_day d ON d.channel_code = p.channel_code AND d.order_date BETWEEN date_sub(p.start_date, 28) AND p.end_date
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.agg_festival_uplift
COMMENT 'Per festival occurrence x zone x base spice: avg daily revenue in the 10 days up to the festival vs days -60..-31 baseline'
AS WITH sp AS (SELECT order_date, zone, base_spice, category, sum(net_revenue_inr) rev FROM ${catalog}.gold.agg_sales_daily GROUP BY ALL)
SELECT f.festival_name, f.festival_date, f.year, f.region_relevance, s.zone, s.base_spice, s.category,
       round(sum(CASE WHEN s.order_date BETWEEN date_sub(f.festival_date, 9) AND f.festival_date THEN s.rev END) / 10, 2) AS festival_daily_revenue_inr,
       round(sum(CASE WHEN s.order_date BETWEEN date_sub(f.festival_date, 60) AND date_sub(f.festival_date, 31) THEN s.rev END) / 30, 2) AS baseline_daily_revenue_inr,
       round(100 * ((sum(CASE WHEN s.order_date BETWEEN date_sub(f.festival_date, 9) AND f.festival_date THEN s.rev END) / 10) /
             nullif(sum(CASE WHEN s.order_date BETWEEN date_sub(f.festival_date, 60) AND date_sub(f.festival_date, 31) THEN s.rev END) / 30, 0) - 1), 1) AS uplift_pct
FROM ${catalog}.silver.festivals f
JOIN sp s ON s.order_date BETWEEN date_sub(f.festival_date, 60) AND f.festival_date
WHERE f.festival_date BETWEEN DATE'2023-12-01' AND DATE'2026-09-30'
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.agg_customer_rfm
COMMENT 'RFM segmentation of identified consumers as of 2026-09-30'
AS WITH c AS (
  SELECT customer_id, max(order_date) last_order, min(order_date) first_order, count(*) orders, sum(net_revenue_inr) monetary
  FROM ${catalog}.gold.fact_orders WHERE customer_id IS NOT NULL AND order_status = 'Delivered' GROUP BY 1),
s AS (SELECT *, datediff(DATE'2026-09-30', last_order) recency_days,
             ntile(5) OVER (ORDER BY datediff(DATE'2026-09-30', last_order) DESC) r, ntile(5) OVER (ORDER BY orders) fq,
             ntile(5) OVER (ORDER BY monetary) m FROM c)
SELECT s.customer_id, s.first_order, s.last_order, s.recency_days, s.orders AS frequency, round(s.monetary, 2) AS monetary_inr,
       s.r AS r_score, s.fq AS f_score, s.m AS m_score,
       CASE WHEN s.r >= 4 AND s.fq >= 4 THEN 'Champions' WHEN s.r >= 3 AND s.fq >= 3 THEN 'Loyal'
            WHEN s.r >= 4 AND s.fq <= 2 THEN 'New / Promising' WHEN s.r <= 2 AND s.fq >= 3 THEN 'At Risk'
            WHEN s.r <= 2 AND s.fq <= 2 THEN 'Hibernating' ELSE 'Needs Attention' END AS rfm_segment,
       dc.zone, dc.city, dc.loyalty_tier, dc.age_band, dc.gender
FROM s LEFT JOIN ${catalog}.gold.dim_customer dc USING (customer_id);

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.agg_inventory_weekly
COMMENT 'Grain: week x warehouse x SKU — stockout days, fill rate, average cover'
AS SELECT CAST(date_trunc('week', i.snapshot_date) AS DATE) AS week_start, i.warehouse_id, i.sku_id, p.base_spice, p.category,
          sum(i.demand_units) demand_units, sum(i.shipped_units) shipped_units, sum(i.unfulfilled_units) unfulfilled_units,
          sum(CASE WHEN i.is_stockout THEN 1 ELSE 0 END) stockout_days, round(sum(i.shipped_units) / nullif(sum(i.demand_units), 0), 4) fill_rate,
          round(avg(i.days_of_cover), 1) avg_days_of_cover, max_by(i.closing_units, i.snapshot_date) closing_units,
          round(max_by(i.stock_value_inr, i.snapshot_date), 2) closing_stock_value_inr
FROM ${catalog}.gold.fact_inventory_daily i JOIN ${catalog}.silver.products p USING (sku_id)
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.agg_margin_monthly
COMMENT 'Grain: month x base spice. Raw-material index vs realised gross margin (shows the 2024 chilli squeeze)'
AS SELECT CAST(trunc(f.order_date, 'month') AS DATE) AS month, p.base_spice_code, f.base_spice, f.category,
          round(sum(f.net_revenue_inr), 2) AS net_revenue_inr, round(sum(f.gross_margin_inr), 2) AS gross_margin_inr,
          round(100 * sum(f.gross_margin_inr) / nullif(sum(f.net_revenue_inr), 0), 2) AS gross_margin_pct,
          any_value(rm.price_index) AS raw_material_index, any_value(rm.price_per_kg_inr) AS raw_material_price_per_kg_inr
FROM ${catalog}.gold.fact_sales_line f
JOIN ${catalog}.silver.products p ON f.sku_id = p.sku_id
LEFT JOIN ${catalog}.silver.raw_material_prices rm ON rm.base_spice_code = p.base_spice_code AND rm.month = trunc(f.order_date, 'month')
WHERE f.order_status = 'Delivered'
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.dq_summary
COMMENT 'Data-quality summary: quarantined rows by table/reason and late-arriving lines'
AS SELECT 'sales_lines' AS source_table, dq_reason, count(*) AS rows FROM ${catalog}.silver.sales_lines_quarantine GROUP BY ALL
UNION ALL SELECT 'orders', dq_reason, count(*) FROM ${catalog}.silver.orders_quarantine GROUP BY ALL
UNION ALL SELECT 'sales_lines', 'late-arriving (accepted)', count(*) FROM ${catalog}.silver.sales_lines WHERE arrived_late
UNION ALL SELECT 'stores', 'state name corrected (accepted)', count(*) FROM ${catalog}.silver.stores WHERE state_was_corrected;
