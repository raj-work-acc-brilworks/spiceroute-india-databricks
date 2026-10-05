-- Governed KPI definitions (Unity Catalog metric views) used by the dashboard, Genie and the app.
-- Run on a SQL warehouse with :catalog bound (sql_task parameter). Query with MEASURE(`Net Revenue`).

CREATE OR REPLACE VIEW IDENTIFIER(:catalog || '.semantic.sales_metrics')
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
comment: "SpiceRoute sales KPIs. Revenue = net revenue excluding GST, delivered orders only. Grain of source: day x SKU x zone x channel."
source: spiceroute.gold.agg_sales_daily
joins:
  - name: dim_date
    source: spiceroute.gold.dim_date
    'on': source.order_date = dim_date.date
  - name: dim_product
    source: spiceroute.gold.dim_product
    'on': source.sku_id = dim_product.sku_id
dimensions:
  - name: Order Date
    expr: source.order_date
  - name: Week Start
    expr: dim_date.week_start
  - name: Month
    expr: dim_date.month_start
  - name: Fiscal Year
    expr: dim_date.fiscal_year
    comment: "Indian fiscal year April-March, e.g. FY25-26"
    synonyms: [FY, financial year]
  - name: Fiscal Quarter
    expr: dim_date.fiscal_quarter
  - name: Season
    expr: dim_date.season
  - name: Festival
    expr: dim_date.festival_name
  - name: Zone
    expr: source.zone
    synonyms: [region]
  - name: Channel
    expr: source.channel_name
  - name: Channel Group
    expr: source.channel_group
    comment: "Offline / Online / B2B"
  - name: Category
    expr: source.category
  - name: Base Spice
    expr: source.base_spice
    synonyms: [spice, product family]
  - name: SKU
    expr: source.sku_id
  - name: Product Name
    expr: dim_product.product_name
  - name: Pack Band
    expr: dim_product.pack_band
  - name: Is Organic
    expr: dim_product.is_organic
measures:
  - name: Net Revenue
    expr: SUM(source.net_revenue_inr)
    comment: "Net revenue in INR excluding GST, after channel margin and promotional discount"
    synonyms: [revenue, sales, turnover]
  - name: Net Revenue Cr
    expr: SUM(source.net_revenue_inr) / 1e7
    comment: "Net revenue in INR crores (1 crore = 10,000,000)"
  - name: Gross Margin
    expr: SUM(source.gross_margin_inr)
    synonyms: [gross profit, GM]
  - name: Gross Margin Pct
    expr: SUM(source.gross_margin_inr) / NULLIF(SUM(source.net_revenue_inr), 0)
    comment: "Gross margin as a fraction of net revenue"
  - name: COGS
    expr: SUM(source.cogs_inr)
  - name: Units
    expr: SUM(source.units)
    synonyms: [packs, quantity]
  - name: Volume Kg
    expr: SUM(source.volume_kg)
    synonyms: [tonnage, kg]
  - name: Discount
    expr: SUM(source.discount_inr)
  - name: Discount Rate
    expr: SUM(source.discount_inr) / NULLIF(SUM(source.net_revenue_inr) + SUM(source.discount_inr), 0)
  - name: Promo Revenue Share
    expr: SUM(source.promo_revenue_inr) / NULLIF(SUM(source.net_revenue_inr), 0)
  - name: Return Rate
    expr: SUM(source.returned_lines) / NULLIF(SUM(source.lines), 0)
    comment: "Share of order lines that were (partly) returned"
  - name: Order Lines
    expr: SUM(source.lines)
$$;

CREATE OR REPLACE VIEW IDENTIFIER(:catalog || '.semantic.order_metrics')
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
comment: "Order-level KPIs (basket size, AOV, active customers). Delivered orders only."
source: spiceroute.gold.fact_orders
filter: order_status = 'Delivered'
joins:
  - name: dim_date
    source: spiceroute.gold.dim_date
    'on': source.order_date = dim_date.date
  - name: dim_channel
    source: spiceroute.gold.dim_channel
    'on': source.channel_code = dim_channel.channel_code
dimensions:
  - name: Order Date
    expr: source.order_date
  - name: Month
    expr: dim_date.month_start
  - name: Fiscal Year
    expr: dim_date.fiscal_year
  - name: Zone
    expr: source.zone
  - name: State
    expr: source.state
  - name: City
    expr: source.city
  - name: Channel
    expr: dim_channel.channel_name
  - name: Channel Group
    expr: dim_channel.channel_group
  - name: Payment Mode
    expr: source.payment_mode
  - name: Used Promo
    expr: source.used_promo
measures:
  - name: Orders
    expr: COUNT(1)
  - name: Net Revenue
    expr: SUM(source.net_revenue_inr)
  - name: Average Order Value
    expr: SUM(source.net_revenue_inr) / COUNT(1)
    synonyms: [AOV, basket value]
  - name: Lines per Order
    expr: AVG(source.line_count)
  - name: Active Customers
    expr: COUNT(DISTINCT source.customer_id)
    comment: "Distinct identified consumers (D2C, marketplace, quick commerce, loyalty)"
$$;

CREATE OR REPLACE VIEW IDENTIFIER(:catalog || '.semantic.inventory_metrics')
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
comment: "Warehouse service-level KPIs: fill rate, stockouts, stock value"
source: spiceroute.gold.fact_inventory_daily
joins:
  - name: dim_product
    source: spiceroute.gold.dim_product
    'on': source.sku_id = dim_product.sku_id
  - name: dim_warehouse
    source: spiceroute.gold.dim_warehouse
    'on': source.warehouse_id = dim_warehouse.warehouse_id
dimensions:
  - name: Snapshot Date
    expr: source.snapshot_date
  - name: Month
    expr: DATE_TRUNC('MONTH', source.snapshot_date)
  - name: Warehouse
    expr: dim_warehouse.warehouse_name
  - name: Warehouse Zone
    expr: dim_warehouse.zone
  - name: Base Spice
    expr: dim_product.base_spice
  - name: Category
    expr: dim_product.category
  - name: SKU
    expr: source.sku_id
measures:
  - name: Demand Units
    expr: SUM(source.demand_units)
  - name: Shipped Units
    expr: SUM(source.shipped_units)
  - name: Fill Rate
    expr: SUM(source.shipped_units) / NULLIF(SUM(source.demand_units), 0)
    synonyms: [service level]
  - name: Stockout Days
    expr: COUNT(1) FILTER (WHERE source.is_stockout)
  - name: Stockout Rate
    expr: COUNT(1) FILTER (WHERE source.is_stockout) / COUNT(1)
  - name: Unfulfilled Units
    expr: SUM(source.unfulfilled_units)
$$;
