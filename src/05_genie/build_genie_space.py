"""Builds and (optionally) creates/updates the "Ask SpiceRoute" Genie space.

python src/05_genie/build_genie_space.py            -> writes genie_space.json
python src/05_genie/build_genie_space.py --test     -> also runs every example SQL on the warehouse
python src/05_genie/build_genie_space.py --deploy   -> creates the space (or updates it if GENIE_SPACE_ID is set)
"""
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CAT = "spiceroute"
PROFILE = "brilworks"
WH = "9cd430b8a1739112"
_id = [0]


def nid(prefix):
    _id[0] += 1
    return f"{prefix}{_id[0]:031x}"[-32:]


TABLES = [
    ("gold.agg_sales_daily", "Daily sales by SKU x zone x channel (delivered orders). Primary table for revenue/volume/margin questions."),
    ("gold.agg_sales_monthly", "Monthly sales by category x state x channel; use for state-level questions."),
    ("gold.agg_festival_uplift", "Festival effect: avg daily revenue in the 10 days up to a festival vs a days -60..-31 baseline, per zone and spice."),
    ("gold.agg_promo_effectiveness", "Promotion lift: avg daily channel revenue during a promotion vs the 28 days before."),
    ("gold.agg_inventory_weekly", "Weekly warehouse x SKU service level: demand, shipped, stockout days, fill rate."),
    ("gold.agg_margin_monthly", "Monthly revenue and gross margin per base spice with the raw-material price index."),
    ("gold.agg_customer_rfm", "RFM segment per identified consumer."),
    ("gold.dim_product", "SKU master: base spice, category, pack size, organic flag, MRP."),
    ("gold.dim_date", "Calendar with Indian fiscal year (Apr-Mar), season and festival names."),
    ("gold.dim_warehouse", "Distribution centres (DCs)."),
    ("gold.dim_store", "Stores / trade accounts / online storefronts."),
    ("gold.fact_returns", "Product returns with reason."),
    ("gold.fact_purchase_orders", "Purchase orders to spice suppliers incl. quality grade and delays."),
    ("ml.demand_forecast", "Prophet 12-week weekly unit forecast per SKU x zone (from week of 2026-09-28)."),
    ("ml.forecast_accuracy", "Backtest accuracy per SKU x zone: WAPE of Prophet vs seasonal naive."),
    ("ml.reorder_recommendation", "Reorder recommendation and stockout risk per DC x SKU as of 2026-09-30."),
]
METRIC_VIEWS = ["semantic.inventory_metrics", "semantic.order_metrics", "semantic.sales_metrics"]

TEXT = """SpiceRoute India is a fictional Indian packaged-spice company. Data covers 2023-10-01 to 2026-09-30 (actuals); forecasts extend 12 weeks after.
Definitions:
- Revenue / sales = net revenue in INR EXCLUDING GST (column net_revenue_inr, or MEASURE(`Net Revenue`) in spiceroute.semantic.sales_metrics). Only delivered orders count.
- Gross margin = net revenue minus COGS. Gross margin % = gross margin / net revenue.
- Amounts: 1 lakh = 100,000 INR; 1 crore = 10,000,000 INR. When the user says "in crores" divide by 1e7 and round to 2 decimals.
- Fiscal year is April to March, labelled like FY25-26 (= 2025-04-01 to 2026-03-31). Use gold.dim_date.fiscal_year. "This year" means FY26-27 (partial, Apr-Sep 2026); "last year" means FY25-26.
- Zones: North, South, East, West, Central, North-East. Channels: General Trade, Modern Trade, Own Retail, HoReCa, Quick Commerce, Marketplace, D2C Website. Channel groups: Offline, Online, B2B.
- A "spice" usually means base_spice (e.g. 'Red Chilli Powder', 'Garam Masala'); a "product" or "SKU" means sku_id / product_name (base spice + pack size).
- Chilli products are base_spice_code IN ('RCH','KCH') (Red Chilli Powder, Kashmiri Chilli Powder). In 2024 (Jun-Oct) a chilli crop failure caused stockouts, a low-grade alternate supplier (quality grade C), more quality returns and a margin squeeze.
- Stockout risk questions: use spiceroute.ml.reorder_recommendation (stockout_risk High/Medium/Low, weeks_of_cover, recommended_order_units). Warehouses are called DCs; match names with LIKE, e.g. warehouse_name LIKE '%Chennai%'.
- Forecast questions: use spiceroute.ml.demand_forecast (forecast_units per week_start) joined to gold.dim_product on sku_id.
- Prefer the metric views (MEASURE() syntax) for KPI questions; prefer agg tables for festival, promo, inventory and forecast questions. Never query fact_sales_line (25M rows) when an aggregate answers the question."""

EXAMPLES = [
    ("What was net revenue by zone in FY25-26, in crores?",
     f"""SELECT `Zone`, ROUND(MEASURE(`Net Revenue`) / 1e7, 2) AS net_revenue_cr
FROM {CAT}.semantic.sales_metrics WHERE `Fiscal Year` = 'FY25-26' GROUP BY `Zone` ORDER BY net_revenue_cr DESC"""),
    ("How did revenue and gross margin trend by fiscal year?",
     f"""SELECT `Fiscal Year`, ROUND(MEASURE(`Net Revenue`) / 1e7, 2) AS net_revenue_cr, ROUND(MEASURE(`Gross Margin Pct`) * 100, 1) AS gross_margin_pct
FROM {CAT}.semantic.sales_metrics GROUP BY `Fiscal Year` ORDER BY `Fiscal Year`"""),
    ("Which spices grew the most during Diwali 2025 compared with Diwali 2024?",
     f"""WITH d AS (
  SELECT base_spice, year, SUM(festival_daily_revenue_inr) AS festival_daily_revenue_inr
  FROM {CAT}.gold.agg_festival_uplift WHERE festival_name = 'Diwali' AND year IN (2024, 2025) GROUP BY ALL)
SELECT base_spice,
       ROUND(MAX(CASE WHEN year = 2024 THEN festival_daily_revenue_inr END)) AS diwali_2024_daily_inr,
       ROUND(MAX(CASE WHEN year = 2025 THEN festival_daily_revenue_inr END)) AS diwali_2025_daily_inr,
       ROUND(100 * (MAX(CASE WHEN year = 2025 THEN festival_daily_revenue_inr END) / MAX(CASE WHEN year = 2024 THEN festival_daily_revenue_inr END) - 1), 1) AS growth_pct
FROM d GROUP BY base_spice ORDER BY growth_pct DESC LIMIT 10"""),
    ("Which SKUs are at high risk of stockout at the Chennai DC?",
     f"""SELECT product_name, on_hand_units, open_po_units, forecast_next_4w_units, weeks_of_cover, recommended_order_units, projected_stockout_date
FROM {CAT}.ml.reorder_recommendation
WHERE warehouse_name LIKE '%Chennai%' AND stockout_risk = 'High' ORDER BY weeks_of_cover"""),
    ("How did the 2024 chilli shortage affect gross margin?",
     f"""SELECT month, CASE WHEN base_spice_code IN ('RCH', 'KCH') THEN 'Chilli' ELSE 'Other spices' END AS spice_group,
       ROUND(100 * SUM(gross_margin_inr) / SUM(net_revenue_inr), 1) AS gross_margin_pct, ROUND(AVG(raw_material_index), 2) AS raw_material_index
FROM {CAT}.gold.agg_margin_monthly WHERE month BETWEEN '2024-01-01' AND '2025-06-01' GROUP BY ALL ORDER BY month, spice_group"""),
    ("What were the top 5 promotions by revenue lift?",
     f"""SELECT promo_name, channel_code, discount_pct, revenue_lift_pct, discount_cost_inr
FROM {CAT}.gold.agg_promo_effectiveness WHERE baseline_daily_revenue_inr IS NOT NULL ORDER BY revenue_lift_pct DESC LIMIT 5"""),
    ("What is the forecast demand for Garam Masala in the North over the next 12 weeks?",
     f"""SELECT f.week_start, ROUND(SUM(f.forecast_units)) AS forecast_units, ROUND(SUM(f.forecast_lower)) AS lower_80, ROUND(SUM(f.forecast_upper)) AS upper_80
FROM {CAT}.ml.demand_forecast f JOIN {CAT}.gold.dim_product p ON f.sku_id = p.sku_id
WHERE p.base_spice = 'Garam Masala' AND f.zone = 'North' GROUP BY f.week_start ORDER BY f.week_start"""),
    ("What share of revenue comes from online channels each fiscal year?",
     f"""SELECT `Fiscal Year`, `Channel Group`, ROUND(MEASURE(`Net Revenue`) / 1e7, 2) AS net_revenue_cr
FROM {CAT}.semantic.sales_metrics GROUP BY ALL ORDER BY `Fiscal Year`, `Channel Group`"""),
]
SAMPLE_QUESTIONS = [
    "What was net revenue by zone in FY25-26, in crores?",
    "Which spices grew the most during Diwali 2025 vs 2024?",
    "Which SKUs are at high risk of stockout at the Chennai DC?",
    "How did the 2024 chilli shortage affect gross margin?",
    "What is the forecast demand for Garam Masala in the North over the next 12 weeks?",
]
CATEGORICAL = {"gold.agg_sales_daily": ["base_spice", "category", "channel_group", "channel_name", "zone"],
               "gold.dim_product": ["base_spice", "category"], "ml.reorder_recommendation": ["base_spice", "stockout_risk", "warehouse_name"],
               "gold.dim_warehouse": ["warehouse_name", "zone"], "gold.agg_festival_uplift": ["base_spice", "festival_name", "zone"]}


def build():
    tables = []
    for ident, desc in sorted(TABLES):
        t = {"identifier": f"{CAT}.{ident}", "description": [desc]}
        if ident in CATEGORICAL:
            t["column_configs"] = [{"column_name": c, "enable_format_assistance": True, "enable_entity_matching": True}
                                   for c in sorted(CATEGORICAL[ident])]
        tables.append(t)
    space = {
        "version": 2,
        "config": {"sample_questions": sorted([{"id": nid("1"), "question": [q]} for q in SAMPLE_QUESTIONS], key=lambda x: x["id"])},
        "data_sources": {"tables": tables, "metric_views": [{"identifier": f"{CAT}.{m}"} for m in sorted(METRIC_VIEWS)]},
        "instructions": {
            "text_instructions": [{"id": nid("3"), "content": [l + "\n" for l in TEXT.split("\n")]}],
            "example_question_sqls": sorted([{"id": nid("2"), "question": [q], "sql": [s]} for q, s in EXAMPLES], key=lambda x: x["id"]),
        },
        "benchmarks": {"questions": sorted([{"id": nid("4"), "question": [q], "answer": [{"format": "SQL", "content": [s]}]}
                                            for q, s in EXAMPLES], key=lambda x: x["id"])},
    }
    return space


def run_sql(sql):
    body = json.dumps({"warehouse_id": WH, "statement": sql, "wait_timeout": "50s"})
    r = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "--json", body, "--profile", PROFILE], capture_output=True, text=True)
    j = json.loads(r.stdout or "{}")
    return j.get("status", {}).get("state"), j.get("status", {}).get("error", {}).get("message", ""), len(j.get("result", {}).get("data_array", []) or [])


if __name__ == "__main__":
    space = build()
    with open(os.path.join(HERE, "genie_space.json"), "w") as f:
        json.dump(space, f, indent=1)
    if "--test" in sys.argv:
        for q, s in EXAMPLES:
            st, err, n = run_sql(s)
            print(f"{st:10s} rows={n:<4d} {q} {err[:200]}")
    if "--deploy" in sys.argv:
        ser = json.dumps(space)
        sid = os.environ.get("GENIE_SPACE_ID")
        if sid:
            cmd = ["databricks", "genie", "update-space", sid, "--json", json.dumps({"serialized_space": ser}), "--profile", PROFILE]
        else:
            payload = {"title": "Ask SpiceRoute", "warehouse_id": WH, "parent_path": "/Workspace/Users/raj.s@brilworks.com/spiceroute",
                       "description": "Ask questions about SpiceRoute India sales, festivals, promotions, margins, inventory and demand forecasts.",
                       "serialized_space": ser}
            cmd = ["databricks", "genie", "create-space", "--json", json.dumps(payload), "--profile", PROFILE, "-o", "json"]
        r = subprocess.run(cmd, capture_output=True, text=True)
        print(r.stdout[-600:], r.stderr[-1500:])
