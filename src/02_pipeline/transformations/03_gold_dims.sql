-- =====================================================================================
-- GOLD DIMENSIONS (star schema). Business keys are used as join keys (sku_id, store_id,
-- customer_id, ...) so Genie / analysts can read the model without surrogate-key lookups.
-- =====================================================================================

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.dim_date
COMMENT 'Calendar incl. Indian fiscal year (Apr-Mar), seasons, festivals and wedding season; extends past actuals for forecasting'
AS WITH d AS (SELECT explode(sequence(DATE'2023-10-01', DATE'2026-12-31', INTERVAL 1 DAY)) AS date),
fest AS (SELECT festival_date, array_join(collect_set(festival_name), ' / ') AS festival_name FROM ${catalog}.silver.festivals GROUP BY 1)
SELECT CAST(date_format(d.date, 'yyyyMMdd') AS INT) AS date_key, d.date,
       date_format(d.date, 'EEEE') AS day_name, dayofweek(d.date) AS day_of_week, dayofweek(d.date) IN (1, 7) AS is_weekend,
       CAST(date_trunc('week', d.date) AS DATE) AS week_start, weekofyear(d.date) AS iso_week,
       month(d.date) AS month, date_format(d.date, 'MMM') AS month_name, CAST(trunc(d.date, 'month') AS DATE) AS month_start,
       quarter(d.date) AS calendar_quarter, year(d.date) AS calendar_year,
       CASE WHEN month(d.date) >= 4 THEN concat('FY', right(CAST(year(d.date) AS STRING), 2), '-', right(CAST(year(d.date) + 1 AS STRING), 2))
            ELSE concat('FY', right(CAST(year(d.date) - 1 AS STRING), 2), '-', right(CAST(year(d.date) AS STRING), 2)) END AS fiscal_year,
       concat('Q', CAST(floor(((month(d.date) + 8) % 12) / 3) + 1 AS INT)) AS fiscal_quarter,
       CASE WHEN month(d.date) IN (3, 4, 5) THEN 'Summer' WHEN month(d.date) IN (6, 7, 8, 9) THEN 'Monsoon'
            WHEN month(d.date) IN (10, 11) THEN 'Post-Monsoon' ELSE 'Winter' END AS season,
       f.festival_name IS NOT NULL AS is_festival_day, f.festival_name,
       (date_format(d.date, 'MM-dd') >= '11-15' OR date_format(d.date, 'MM-dd') <= '02-28'
        OR date_format(d.date, 'MM-dd') BETWEEN '04-15' AND '05-31') AS is_wedding_season,
       dayofmonth(d.date) <= 7 AS is_salary_week,
       d.date <= DATE'2026-09-30' AS is_actuals_period
FROM d LEFT JOIN fest f ON d.date = f.festival_date;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.dim_geography COMMENT 'City -> state -> zone hierarchy'
AS SELECT * FROM ${catalog}.silver.geography;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.dim_product COMMENT 'SKU master (current attributes + current MRP)'
AS SELECT p.*, CASE WHEN p.pack_size_g <= 50 THEN 'Small (<=50g)' WHEN p.pack_size_g <= 200 THEN 'Medium (100-200g)'
                     ELSE 'Bulk (500g-1kg)' END AS pack_band
FROM ${catalog}.silver.products p;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.dim_product_price_history
COMMENT 'SCD Type 2 MRP history per SKU (valid_from inclusive, valid_to exclusive; NULL = current)'
AS SELECT sku_id, effective_from AS valid_from, effective_to AS valid_to, mrp_inr, effective_to IS NULL AS is_current
FROM ${catalog}.silver.product_prices;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.dim_channel COMMENT 'Sales channels'
AS SELECT * FROM ${catalog}.silver.channels;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.dim_store COMMENT 'Stores, trade accounts and virtual (online) storefronts'
AS SELECT s.store_id, s.store_name, s.store_type, s.channel_code, c.channel_name, s.city, s.state, s.zone, s.tier,
          s.warehouse_id, s.opened_date, s.closed_date, s.closed_date IS NULL AS is_active, s.size_band, s.sales_rep_id
FROM ${catalog}.silver.stores s LEFT JOIN ${catalog}.silver.channels c USING (channel_code);

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.dim_customer COMMENT 'Identified consumers (D2C, marketplace, quick commerce, loyalty)'
AS SELECT c.*, g.zone, g.tier FROM ${catalog}.silver.customers c
LEFT JOIN ${catalog}.silver.geography g ON c.city = g.city AND c.state = g.state;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.dim_promotion COMMENT 'Promotions / trade schemes'
AS SELECT *, datediff(end_date, start_date) + 1 AS duration_days FROM ${catalog}.silver.promotions;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.dim_warehouse COMMENT 'Distribution centres'
AS SELECT * FROM ${catalog}.silver.warehouses;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.dim_supplier COMMENT 'Spice suppliers'
AS SELECT * FROM ${catalog}.silver.suppliers;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.dim_festival COMMENT 'Festival calendar'
AS SELECT * FROM ${catalog}.silver.festivals;
