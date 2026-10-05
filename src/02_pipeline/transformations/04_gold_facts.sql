-- =====================================================================================
-- GOLD FACTS
-- Revenue conventions (Indian FMCG): MRP is GST-inclusive.
--   gross_amount_incl_gst = qty x unit_price (MRP less channel partner margin)
--   line_amount_incl_gst  = gross_amount_incl_gst x (1 - promo discount)
--   net_revenue_inr       = line_amount_incl_gst / (1 + gst_rate)   <- "revenue" everywhere
-- =====================================================================================

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.fact_sales_line
COMMENT 'Grain: one SKU line of a customer/trade order (~25M rows). Cancelled orders are kept with order_status = Cancelled.'
CLUSTER BY (order_date, sku_id)
AS SELECT l.line_id, l.order_id, l.line_no, l.order_date, CAST(date_format(l.order_date, 'yyyyMMdd') AS INT) AS date_key,
          o.order_ts, l.sku_id, p.category, p.base_spice, o.store_id, o.channel_code, o.customer_id, s.warehouse_id, s.zone, s.state, s.city,
          l.promo_code, o.order_status, o.payment_mode,
          l.quantity, round(l.quantity * p.pack_size_g / 1000.0, 3) AS volume_kg,
          l.unit_mrp_inr, l.unit_price_inr, l.discount_pct,
          round(l.quantity * l.unit_price_inr, 2) AS gross_amount_incl_gst,
          l.line_amount_inr AS line_amount_incl_gst,
          round(l.line_amount_inr / (1 + p.gst_rate), 2) AS net_revenue_inr,
          round(l.line_amount_inr - l.line_amount_inr / (1 + p.gst_rate), 2) AS gst_inr,
          round(l.quantity * l.unit_price_inr * l.discount_pct / (1 + p.gst_rate), 2) AS discount_inr,
          round(l.quantity * c.unit_cost_inr, 2) AS cogs_inr,
          round(l.line_amount_inr / (1 + p.gst_rate) - l.quantity * c.unit_cost_inr, 2) AS gross_margin_inr,
          r.return_id IS NOT NULL AS is_returned
FROM ${catalog}.silver.sales_lines l
JOIN ${catalog}.silver.orders o ON l.order_id = o.order_id
JOIN ${catalog}.silver.products p ON l.sku_id = p.sku_id
JOIN ${catalog}.silver.stores s ON o.store_id = s.store_id
LEFT JOIN ${catalog}.silver.product_costs c ON l.sku_id = c.sku_id AND c.month = trunc(l.order_date, 'month')
LEFT JOIN (SELECT line_id, min(return_id) AS return_id FROM ${catalog}.silver.returns GROUP BY line_id) r
  ON l.line_id = r.line_id;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.fact_orders
COMMENT 'Grain: one order (header) with basket metrics'
CLUSTER BY (order_date)
AS SELECT order_id, any_value(order_ts) order_ts, order_date, any_value(store_id) store_id, any_value(channel_code) channel_code,
          any_value(customer_id) customer_id, any_value(zone) zone, any_value(state) state, any_value(city) city,
          any_value(order_status) order_status, any_value(payment_mode) payment_mode,
          count(*) AS line_count, sum(quantity) AS units, round(sum(volume_kg), 3) AS volume_kg,
          round(sum(net_revenue_inr), 2) AS net_revenue_inr, round(sum(gross_margin_inr), 2) AS gross_margin_inr,
          max(promo_code IS NOT NULL) AS used_promo
FROM ${catalog}.gold.fact_sales_line GROUP BY order_id, order_date;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.fact_returns COMMENT 'Grain: one return'
AS SELECT r.*, f.order_date, f.channel_code, f.zone, f.warehouse_id, p.category, p.base_spice,
          datediff(r.return_date, f.order_date) AS days_to_return
FROM ${catalog}.silver.returns r
JOIN ${catalog}.gold.fact_sales_line f ON r.line_id = f.line_id
JOIN ${catalog}.silver.products p ON r.sku_id = p.sku_id;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.fact_inventory_daily
COMMENT 'Grain: warehouse x SKU x day. is_stockout = demand could not be fully shipped.'
CLUSTER BY (snapshot_date, warehouse_id)
AS SELECT i.*, i.demand_units - i.shipped_units AS unfulfilled_units, i.shipped_units < i.demand_units AS is_stockout,
          i.closing_units < i.safety_stock_units AS below_safety_stock,
          round(i.closing_units / nullif(avg(i.demand_units) OVER (PARTITION BY i.warehouse_id, i.sku_id ORDER BY i.snapshot_date
                ROWS BETWEEN 27 PRECEDING AND CURRENT ROW), 0), 1) AS days_of_cover,
          round(i.closing_units * c.unit_cost_inr, 2) AS stock_value_inr
FROM ${catalog}.silver.inventory_snapshots i
LEFT JOIN ${catalog}.silver.product_costs c ON i.sku_id = c.sku_id AND c.month = trunc(i.snapshot_date, 'month');

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.fact_purchase_orders COMMENT 'Grain: one purchase order line'
AS SELECT po.*, s.supplier_name, s.supplier_tier, p.base_spice, p.category,
          datediff(po.received_date, po.po_date) AS actual_lead_days, datediff(po.received_date, po.expected_date) AS delay_days,
          round(po.received_units / po.ordered_units, 3) AS fill_rate
FROM ${catalog}.silver.purchase_orders po
LEFT JOIN ${catalog}.silver.suppliers s USING (supplier_id)
LEFT JOIN ${catalog}.silver.products p USING (sku_id);
