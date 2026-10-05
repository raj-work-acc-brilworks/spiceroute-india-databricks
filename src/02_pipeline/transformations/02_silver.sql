-- =====================================================================================
-- SILVER: typed, cleaned, de-duplicated, conformed. Expectations drop bad rows and the
-- rejects land in *_quarantine tables so data-quality can be monitored.
-- =====================================================================================

-- ---------- reference ----------
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.silver.geography (
  CONSTRAINT valid_zone EXPECT (zone IN ('North','South','East','West','Central','North-East')) ON VIOLATION DROP ROW
) COMMENT 'Cities with state, zone, tier and coordinates'
AS SELECT city_id, city, state, state_code, zone, CAST(tier AS INT) AS tier, CAST(is_metro AS BOOLEAN) AS is_metro,
          CAST(lat AS DOUBLE) AS lat, CAST(lon AS DOUBLE) AS lon, CAST(population_m AS DOUBLE) AS population_m
FROM bronze.geography_raw;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.silver.warehouses COMMENT 'Distribution centres'
AS SELECT warehouse_id, warehouse_name, city, state, zone, CAST(lat AS DOUBLE) lat, CAST(lon AS DOUBLE) lon,
          CAST(capacity_tonnes AS INT) capacity_tonnes
FROM bronze.warehouses_raw;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.silver.channels COMMENT 'Sales channels and partner margins'
AS SELECT channel_code, channel_name, channel_group, CAST(partner_margin_pct AS DOUBLE) partner_margin_pct FROM bronze.channels_raw;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.silver.festivals COMMENT 'Festival occurrences'
AS SELECT festival_name, CAST(festival_date AS DATE) festival_date, CAST(year AS INT) year, region_relevance,
          CAST(lead_days AS INT) lead_days, CAST(expected_uplift_pct AS DOUBLE) expected_uplift_pct, key_products
FROM bronze.festivals_raw;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.silver.products (
  CONSTRAINT sku_present EXPECT (sku_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_mrp EXPECT (mrp_inr > 0) ON VIOLATION DROP ROW
) COMMENT 'Product (SKU) master with current MRP'
AS SELECT sku_id, product_name, base_spice_code, base_spice, category, sub_category, CAST(pack_size_g AS INT) pack_size_g,
          CAST(is_organic AS BOOLEAN) is_organic, CAST(mrp_inr AS DOUBLE) mrp_inr, CAST(gst_rate AS DOUBLE) gst_rate,
          CAST(shelf_life_days AS INT) shelf_life_days, CAST(launch_date AS DATE) launch_date
FROM bronze.products_raw;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.silver.product_prices COMMENT 'MRP history (SCD2 source)'
AS SELECT sku_id, CAST(effective_from AS DATE) effective_from, CAST(effective_to AS DATE) effective_to, CAST(mrp_inr AS DOUBLE) mrp_inr
FROM bronze.product_prices_raw;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.silver.product_costs COMMENT 'Monthly unit standard cost per SKU'
AS SELECT sku_id, CAST(month AS DATE) month, CAST(unit_cost_inr AS DOUBLE) unit_cost_inr FROM bronze.product_costs_raw;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.silver.raw_material_prices COMMENT 'Monthly raw-material price index per base spice'
AS SELECT base_spice_code, CAST(month AS DATE) month, CAST(price_index AS DOUBLE) price_index, CAST(price_per_kg_inr AS DOUBLE) price_per_kg_inr
FROM bronze.raw_material_prices_raw;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.silver.suppliers COMMENT 'Supplier master'
AS SELECT supplier_id, supplier_name, origin_town, origin_state, primary_spice_code, CAST(reliability_score AS DOUBLE) reliability_score,
          CAST(lead_time_days AS INT) lead_time_days, supplier_tier
FROM bronze.suppliers_raw;

-- state-name standardisation (raw store master has messy spellings)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.silver.stores (
  CONSTRAINT store_id_present EXPECT (store_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT state_resolved EXPECT (zone IS NOT NULL)
) COMMENT 'Stores / trade accounts with standardised state names, zone and tier'
AS WITH s AS (
  SELECT *, CASE trim(state)
      WHEN 'Tamilnadu' THEN 'Tamil Nadu' WHEN 'TN' THEN 'Tamil Nadu' WHEN 'Tamil nadu' THEN 'Tamil Nadu'
      WHEN 'Maharastra' THEN 'Maharashtra' WHEN 'MH' THEN 'Maharashtra'
      WHEN 'NCT of Delhi' THEN 'Delhi' WHEN 'Delhi NCR' THEN 'Delhi' WHEN 'New Delhi' THEN 'Delhi'
      WHEN 'West Bangal' THEN 'West Bengal' WHEN 'WB' THEN 'West Bengal'
      WHEN 'Karnatka' THEN 'Karnataka' WHEN 'KA' THEN 'Karnataka' WHEN 'Orissa' THEN 'Odisha'
      WHEN 'Uttaranchal' THEN 'Uttarakhand' WHEN 'UP' THEN 'Uttar Pradesh' WHEN 'Uttar pradesh' THEN 'Uttar Pradesh'
      WHEN 'Telengana' THEN 'Telangana' WHEN 'Gujrat' THEN 'Gujarat' ELSE trim(state) END AS state_std
  FROM bronze.stores_raw)
SELECT s.store_id, s.store_name, s.store_type, s.channel_code, s.city, s.state_std AS state, g.zone, g.tier, g.city_id,
       s.servicing_warehouse_id AS warehouse_id, CAST(s.opened_date AS DATE) opened_date, CAST(s.closed_date AS DATE) closed_date,
       s.size_band, s.sales_rep_id, s.state <> s.state_std AS state_was_corrected
FROM s LEFT JOIN ${catalog}.silver.geography g ON s.city = g.city AND s.state_std = g.state;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.silver.promotions COMMENT 'Promotions calendar'
AS SELECT promo_id, promo_code, promo_name, promo_type, CAST(discount_pct AS DOUBLE) discount_pct, CAST(start_date AS DATE) start_date,
          CAST(end_date AS DATE) end_date, channel_code, category_scope, funded_by, festival_name
FROM bronze.promotions_raw;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.silver.customers (
  CONSTRAINT customer_id_present EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW
) COMMENT 'D2C / loyalty customers (deduplicated)'
AS SELECT customer_id, full_name, gender, age_band, city, state, CAST(signup_date AS DATE) signup_date, loyalty_tier,
          preferred_language, customer_type
FROM bronze.customers_raw
QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY _ingest_ts DESC) = 1;

-- ---------- transactional ----------
CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.orders (
  CONSTRAINT store_id_present EXPECT (store_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_status EXPECT (order_status IN ('Delivered','Cancelled')) ON VIOLATION DROP ROW
) COMMENT 'Order headers; orders without a store are quarantined'
AS SELECT order_id, CAST(order_ts AS TIMESTAMP) order_ts, CAST(order_ts AS DATE) order_date, store_id, channel_code,
          customer_id, payment_mode, order_status, delivery_city, _ingest_ts
FROM STREAM(bronze.orders_raw);

CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.orders_quarantine COMMENT 'Rejected order headers (missing store)'
AS SELECT *, 'missing store_id' AS dq_reason FROM STREAM(bronze.orders_raw) WHERE store_id IS NULL;

-- Lines: de-duplicate (exact duplicate rows in raw), validate SKU / qty / price
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.silver.sales_lines (
  CONSTRAINT positive_quantity EXPECT (quantity > 0) ON VIOLATION DROP ROW,
  CONSTRAINT known_sku EXPECT (sku_known) ON VIOLATION DROP ROW,
  CONSTRAINT price_present EXPECT (unit_price_inr IS NOT NULL) ON VIOLATION DROP ROW
) COMMENT 'Clean order lines: deduplicated, valid SKU, positive quantity, priced'
CLUSTER BY (order_date, sku_id)
AS SELECT l.line_id, l.order_id, CAST(l.line_no AS INT) line_no, CAST(l.order_date AS DATE) order_date, l.sku_id,
          CAST(l.quantity AS INT) quantity, CAST(l.unit_mrp_inr AS DOUBLE) unit_mrp_inr, CAST(l.unit_price_inr AS DOUBLE) unit_price_inr,
          l.promo_code, CAST(l.discount_pct AS DOUBLE) discount_pct, CAST(l.line_amount_inr AS DOUBLE) line_amount_inr,
          p.sku_id IS NOT NULL AS sku_known, l.batch AS landing_batch,
          l.batch <> date_format(l.order_date, 'yyyy-MM') AS arrived_late
FROM bronze.sales_lines_raw l LEFT JOIN ${catalog}.silver.products p ON l.sku_id = p.sku_id
QUALIFY row_number() OVER (PARTITION BY l.line_id ORDER BY l._ingest_ts) = 1;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.silver.sales_lines_quarantine COMMENT 'Rejected / duplicate order lines with reason'
AS WITH ranked AS (
  SELECT l.*, row_number() OVER (PARTITION BY l.line_id ORDER BY l._ingest_ts) AS rn, p.sku_id IS NOT NULL AS sku_known
  FROM bronze.sales_lines_raw l LEFT JOIN ${catalog}.silver.products p ON l.sku_id = p.sku_id)
SELECT line_id, order_id, order_date, sku_id, quantity, unit_price_inr, line_amount_inr, batch, _source_file,
       CASE WHEN rn > 1 THEN 'duplicate line' WHEN quantity <= 0 THEN 'non-positive quantity'
            WHEN NOT sku_known THEN 'unknown sku' WHEN unit_price_inr IS NULL THEN 'missing price' END AS dq_reason
FROM ranked WHERE rn > 1 OR quantity <= 0 OR NOT sku_known OR unit_price_inr IS NULL;

CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.returns (
  CONSTRAINT positive_return_qty EXPECT (return_qty > 0) ON VIOLATION DROP ROW
) COMMENT 'Product returns'
AS SELECT return_id, line_id, order_id, sku_id, CAST(return_date AS DATE) return_date, CAST(return_qty AS INT) return_qty,
          return_reason, CAST(refund_amount_inr AS DOUBLE) refund_amount_inr
FROM STREAM(bronze.returns_raw);

CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.inventory_snapshots (
  CONSTRAINT non_negative_stock EXPECT (closing_units >= 0) ON VIOLATION DROP ROW
) COMMENT 'Daily warehouse x SKU stock positions'
AS SELECT warehouse_id, sku_id, CAST(snapshot_date AS DATE) snapshot_date, opening_units, receipts_units, demand_units,
          shipped_units, closing_units, reorder_point_units, safety_stock_units
FROM STREAM(bronze.inventory_snapshots_raw);

CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.purchase_orders (
  CONSTRAINT positive_order_qty EXPECT (ordered_units > 0) ON VIOLATION DROP ROW
) COMMENT 'Purchase orders to suppliers'
AS SELECT po_id, CAST(po_date AS DATE) po_date, supplier_id, warehouse_id, sku_id, ordered_units, received_units, ordered_kg,
          unit_cost_inr, po_value_inr, CAST(expected_date AS DATE) expected_date, CAST(received_date AS DATE) received_date, quality_grade
FROM STREAM(bronze.purchase_orders_raw);
