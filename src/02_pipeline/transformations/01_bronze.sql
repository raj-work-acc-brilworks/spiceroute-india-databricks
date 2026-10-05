-- =====================================================================================
-- BRONZE: raw files from /Volumes/<catalog>/bronze/raw_landing, ingested as-is + lineage
-- Large/append-only sources use Auto Loader streaming tables; small reference files are
-- re-read on every update as materialized views (they are overwritten in place).
-- =====================================================================================

-- ---------- streaming (Auto Loader) ----------
CREATE OR REFRESH STREAMING TABLE orders_raw
COMMENT 'Raw order headers (Parquet, monthly batches) via Auto Loader'
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingest_ts
FROM STREAM read_files('/Volumes/${catalog}/bronze/raw_landing/orders', format => 'parquet');

CREATE OR REFRESH STREAMING TABLE sales_lines_raw
COMMENT 'Raw order lines (~25M, Parquet, monthly batches incl. late arrivals and duplicates) via Auto Loader'
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingest_ts
FROM STREAM read_files('/Volumes/${catalog}/bronze/raw_landing/sales_lines', format => 'parquet');

CREATE OR REFRESH STREAMING TABLE returns_raw
COMMENT 'Raw product returns'
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingest_ts
FROM STREAM read_files('/Volumes/${catalog}/bronze/raw_landing/returns', format => 'parquet');

CREATE OR REFRESH STREAMING TABLE inventory_snapshots_raw
COMMENT 'Raw daily warehouse x SKU inventory snapshots'
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingest_ts
FROM STREAM read_files('/Volumes/${catalog}/bronze/raw_landing/inventory_snapshots', format => 'parquet');

CREATE OR REFRESH STREAMING TABLE purchase_orders_raw
COMMENT 'Raw purchase orders to spice suppliers'
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingest_ts
FROM STREAM read_files('/Volumes/${catalog}/bronze/raw_landing/purchase_orders', format => 'parquet');

CREATE OR REFRESH STREAMING TABLE customers_raw
COMMENT 'Raw D2C / loyalty customer master (1M)'
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingest_ts
FROM STREAM read_files('/Volumes/${catalog}/bronze/raw_landing/customers', format => 'parquet');

-- ---------- reference / master files (small, re-read each update) ----------
CREATE OR REFRESH MATERIALIZED VIEW stores_raw
COMMENT 'Raw store/account master (JSON) - contains inconsistent state spellings'
AS SELECT *, _metadata.file_path AS _source_file FROM read_files('/Volumes/${catalog}/bronze/raw_landing/stores', format => 'json');

CREATE OR REFRESH MATERIALIZED VIEW promotions_raw
COMMENT 'Raw promotions calendar (JSON)'
AS SELECT *, _metadata.file_path AS _source_file FROM read_files('/Volumes/${catalog}/bronze/raw_landing/promotions', format => 'json');

CREATE OR REFRESH MATERIALIZED VIEW products_raw
COMMENT 'Raw product master (CSV)'
AS SELECT *, _metadata.file_path AS _source_file FROM read_files('/Volumes/${catalog}/bronze/raw_landing/products', format => 'csv', header => true);

CREATE OR REFRESH MATERIALIZED VIEW product_prices_raw
COMMENT 'Raw MRP revision history (CSV)'
AS SELECT *, _metadata.file_path AS _source_file FROM read_files('/Volumes/${catalog}/bronze/raw_landing/product_prices', format => 'csv', header => true);

CREATE OR REFRESH MATERIALIZED VIEW product_costs_raw
COMMENT 'Raw monthly standard unit cost per SKU from ERP (CSV)'
AS SELECT *, _metadata.file_path AS _source_file FROM read_files('/Volumes/${catalog}/bronze/raw_landing/product_costs', format => 'csv', header => true);

CREATE OR REFRESH MATERIALIZED VIEW raw_material_prices_raw
COMMENT 'Raw monthly mandi price index per base spice (CSV)'
AS SELECT *, _metadata.file_path AS _source_file FROM read_files('/Volumes/${catalog}/bronze/raw_landing/raw_material_prices', format => 'csv', header => true);

CREATE OR REFRESH MATERIALIZED VIEW suppliers_raw
COMMENT 'Raw supplier master (CSV)'
AS SELECT *, _metadata.file_path AS _source_file FROM read_files('/Volumes/${catalog}/bronze/raw_landing/suppliers', format => 'csv', header => true);

CREATE OR REFRESH MATERIALIZED VIEW geography_raw
COMMENT 'Raw city/state/zone reference (CSV)'
AS SELECT *, _metadata.file_path AS _source_file FROM read_files('/Volumes/${catalog}/bronze/raw_landing/reference/geography', format => 'csv', header => true);

CREATE OR REFRESH MATERIALIZED VIEW warehouses_raw
COMMENT 'Raw warehouse reference (CSV)'
AS SELECT *, _metadata.file_path AS _source_file FROM read_files('/Volumes/${catalog}/bronze/raw_landing/reference/warehouses', format => 'csv', header => true);

CREATE OR REFRESH MATERIALIZED VIEW channels_raw
COMMENT 'Raw sales channel reference (CSV)'
AS SELECT *, _metadata.file_path AS _source_file FROM read_files('/Volumes/${catalog}/bronze/raw_landing/reference/channels', format => 'csv', header => true);

CREATE OR REFRESH MATERIALIZED VIEW festivals_raw
COMMENT 'Raw Indian festival calendar (CSV)'
AS SELECT *, _metadata.file_path AS _source_file FROM read_files('/Volumes/${catalog}/bronze/raw_landing/reference/festivals', format => 'csv', header => true);
