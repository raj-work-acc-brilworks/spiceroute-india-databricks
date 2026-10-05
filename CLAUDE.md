# CLAUDE.md — SpiceRoute India (Databricks Free Edition)

Project memory for Claude Code. Read `README.md` for the full design. This file captures the conventions, decisions and gotchas you need before changing anything.

## What this is
An end-to-end retail lakehouse for a fictional Indian spice company. It covers:
- synthetic data (about 25M sales lines);
- a medallion pipeline;
- metric views;
- hierarchical Prophet forecasting with reorder recommendations;
- an AI/BI dashboard, a Genie space and a Streamlit Databricks App.

Everything is one Declarative Automation Bundle: `databricks.yml` plus `resources/*.yml`.

## Workspace & conventions
- **CLI profile:** `brilworks` (a dedicated Free Edition workspace). Always pass `--profile brilworks`. Never use `DEFAULT`, `DEV` or `raj.s`; `raj.s` holds an unrelated retail project that must not be touched.
- **Catalog:** `spiceroute` only. Generators refuse `dev_retail_catalog`, `workspace`, `samples` and `system`.
- **Schemas:**
  - `bronze`, `silver`, `gold`: pipeline-owned, do not write manually;
  - `ml`: forecast job outputs, plus `reorder_overrides` written by the app;
  - `semantic`: metric views;
  - `synthetic`: generator helper tables.
- **Compute:**
  - serverless only (jobs use `environments` client "4"; the pipeline is `serverless: true`);
  - SQL warehouse "Serverless Starter Warehouse" `9cd430b8a1739112`;
  - **no schedules**: everything is triggered manually to save the Free Edition quota; alerts are deployed PAUSED.
- **Revenue:** always `net_revenue_inr`, which excludes GST. MRP includes GST. Fiscal year is April–March (`FY25-26`). Actuals end 2026-09-30.
- **Join keys:** business keys (`sku_id`, `store_id`, `customer_id`), not surrogate keys.
- **Dashboard and Genie are code-generated:**
  - edit `dashboards/build_dashboard.py` or `src/05_genie/build_genie_space.py`, never the generated JSON;
  - run `--test` first: it executes every SQL query on the warehouse;
  - after a dashboard change, run `bundle deploy`, then `lakeview publish`.
- **Commits:** do not add "Co-Authored-By: Claude" or any other Claude attribution to commits, PRs or files.

## Key IDs
- Dashboard: `01f1c0bd2fb31f4fa957edeceee74365`
- Genie space "Ask SpiceRoute": `01f1c0bd7bd6175ba0b691784fd34971`
- App: `spiceroute-demand-planner`. Its service principal has `SELECT` on `gold`, `ml` and `semantic`, plus `MODIFY` on `ml.reorder_overrides`. Grant `SELECT` again on any new `ml` table the app reads.

## Commands
```bash
databricks bundle validate --strict --profile brilworks && databricks bundle deploy --profile brilworks
databricks bundle run spiceroute_end_to_end --profile brilworks                    # pipeline → metric views → forecast → DQ gate
databricks bundle run spiceroute_end_to_end --params regenerate=true --profile brilworks   # rebuild 25M rows + full refresh
databricks bundle run spiceroute_generate_data --params target_lines=500000 --profile brilworks  # quick test data
python3 dashboards/build_dashboard.py --test
python3 src/05_genie/build_genie_space.py --test
GENIE_SPACE_ID=01f1c0bd7bd6175ba0b691784fd34971 python3 src/05_genie/build_genie_space.py --deploy
databricks lakeview publish 01f1c0bd2fb31f4fa957edeceee74365 --warehouse-id 9cd430b8a1739112 --embed-credentials --profile brilworks
```

## Gotchas learned the hard way
- **Prophet on serverless:** pin `cmdstanpy==1.2.5`. Version 1.3.x rejects the CmdStan bundled in prophet 1.1.6 ("missing makefile"), which surfaces as `'Prophet' object has no attribute 'stan_backend'`.
- **Metric-view YAML:** run it through the SQL warehouse (sql_task or the Statement Execution API). `databricks experimental aitools tools query` strips indentation and breaks the YAML.
- **Regenerating data:** raw files are overwritten in place, so the pipeline needs a full refresh. The end-to-end job does this when `regenerate=true`.
- **SDP SQL:**
  - qualify columns when joining (one ambiguity error already bit us);
  - cross-schema targets use `${catalog}.silver.x` via the pipeline `configuration`;
  - a failed production update auto-retries: stop it before re-running.
- **Python script tasks:** sibling imports work via the `sys.path` insert from `sys.argv[0]`/`__file__` (see `src/01_generate/*.py`).
- **Dashboard JSON:**
  - bare gold table names, with `ml.`/`bronze.`/`silver.` prefixes for other schemas;
  - `scale.sort` only supports documented values (no `x-reversed`): use query `orders` instead.
  - a dataset measure (`columns[].displayName`) must not share a name with a column of that dataset; names are case-insensitive, so measure `Orders` vs column `orders` made the tile show "Unable to render visualization".
- **Forecast accuracy:** at SKU × zone × week the noise floor is about 25% WAPE (bulk distributor orders). Evaluate and plan at base spice × zone (champion is about 17%). Champion selection happens automatically in `train_forecast.py`.

## Data stories baked into the generator (keep them intact)
- festival uplifts by zone and spice;
- regional taste;
- the 2024 Andhra/Telangana chilli crop failure (Jun–Oct 2024): cost spike, refused allocations, grade-C alternate supplier, quality returns, South/West stockouts, margin squeeze, then an MRP hike of +12% in July 2024;
- online channel growth;
- the organic line launched in August 2024;
- injected DQ issues: duplicates, nulls, unknown SKUs, negative quantities, messy state names, late files.
