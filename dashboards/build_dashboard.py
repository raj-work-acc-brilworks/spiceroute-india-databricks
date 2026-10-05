"""Builds dashboards/spiceroute_command_center.lvdash.json (AI/BI dashboard).

Queries use bare gold table names (catalog/schema come from the bundle's dataset_catalog/dataset_schema)
and `ml.`-qualified names for model outputs. Run:  python dashboards/build_dashboard.py [--test]
--test executes every dataset query against the SQL warehouse via the Statement Execution API.
"""
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))

# ------------------------------------------------------------------ datasets
DATASETS = {
    "ds_sales": ("Sales (day x zone x channel x spice)", """
SELECT a.order_date, d.fiscal_year, d.month_start, d.season, a.zone, a.channel_name, a.channel_group, a.category, a.base_spice,
       SUM(a.net_revenue_inr) AS net_revenue_inr, SUM(a.gross_margin_inr) AS gross_margin_inr, SUM(a.units) AS units,
       SUM(a.volume_kg) AS volume_kg, SUM(a.discount_inr) AS discount_inr, SUM(a.promo_revenue_inr) AS promo_revenue_inr,
       SUM(a.lines) AS lines, SUM(a.returned_lines) AS returned_lines
FROM agg_sales_daily a JOIN dim_date d ON a.order_date = d.date
GROUP BY ALL""", [
        ("Net Revenue", "SUM(`net_revenue_inr`)"),
        ("Gross Margin %", "SUM(`gross_margin_inr`) / SUM(`net_revenue_inr`)"),
        ("Volume Tonnes", "SUM(`volume_kg`) / 1000"),
        ("Promo Share", "SUM(`promo_revenue_inr`) / SUM(`net_revenue_inr`)"),
        ("Return Rate", "SUM(`returned_lines`) / SUM(`lines`)"),
    ]),
    "ds_orders": ("Orders (day x zone x channel)", """
SELECT o.order_date, o.zone, c.channel_name, c.channel_group, COUNT(*) AS orders, SUM(o.net_revenue_inr) AS net_revenue_inr
FROM fact_orders o JOIN dim_channel c ON o.channel_code = c.channel_code
WHERE o.order_status = 'Delivered'
GROUP BY ALL""", [
        ("Total Orders", "SUM(`orders`)"),  # must not share a name with the `orders` column (names are case-insensitive)
        ("AOV", "SUM(`net_revenue_inr`) / SUM(`orders`)"),
    ]),
    "ds_state_map": ("Revenue by state (monthly)", """
SELECT m.month AS order_date, m.state, m.zone, c.channel_name, m.category, g.lat, g.lon, SUM(m.net_revenue_inr) AS net_revenue_inr,
       SUM(m.gross_margin_inr) AS gross_margin_inr
FROM agg_sales_monthly m
JOIN dim_channel c ON m.channel_code = c.channel_code
JOIN (SELECT state, AVG(lat) AS lat, AVG(lon) AS lon FROM dim_geography GROUP BY state) g ON m.state = g.state
GROUP BY ALL""", []),
    "ds_festival": ("Festival uplift", """
SELECT festival_name, festival_date, year, zone, base_spice, category, festival_daily_revenue_inr, baseline_daily_revenue_inr,
       uplift_pct / 100.0 AS uplift
FROM agg_festival_uplift
WHERE baseline_daily_revenue_inr > 20000""", []),
    "ds_festival_daily": ("Daily revenue around festivals", """
SELECT a.order_date, a.zone, SUM(a.net_revenue_inr) AS net_revenue_inr
FROM agg_sales_daily a
WHERE a.order_date >= DATE'2024-08-01' AND a.order_date <= DATE'2025-11-30'
GROUP BY ALL""", []),
    "ds_promo": ("Promotion effectiveness", """
SELECT p.promo_code, p.promo_name, p.promo_type, c.channel_name, p.category_scope, p.festival_name, p.discount_pct, p.start_date,
       p.promo_daily_revenue_inr, p.baseline_daily_revenue_inr, p.discount_cost_inr, p.revenue_lift_pct / 100.0 AS revenue_lift
FROM agg_promo_effectiveness p JOIN dim_channel c ON p.channel_code = c.channel_code
WHERE p.baseline_daily_revenue_inr IS NOT NULL""", []),
    "ds_forecast": ("Actuals + 12-week Prophet forecast (week x zone x category)", """
WITH act AS (
  SELECT w.week_start, w.zone, p.category, SUM(w.units) AS actual_units
  FROM agg_sales_weekly_sku_zone w JOIN dim_product p ON w.sku_id = p.sku_id
  WHERE w.week_start BETWEEN DATE'2024-10-07' AND DATE'2026-09-21' GROUP BY ALL),
fc AS (
  SELECT f.week_start, f.zone, p.category, SUM(f.forecast_units) AS forecast_units, SUM(f.forecast_lower) AS forecast_lower,
         SUM(f.forecast_upper) AS forecast_upper
  FROM ml.demand_forecast f JOIN dim_product p ON f.sku_id = p.sku_id GROUP BY ALL)
SELECT week_start, zone, category, actual_units, CAST(NULL AS DOUBLE) AS forecast_units, CAST(NULL AS DOUBLE) AS forecast_lower,
       CAST(NULL AS DOUBLE) AS forecast_upper FROM act
UNION ALL
SELECT week_start, zone, category, actual_units, actual_units, actual_units, actual_units FROM act WHERE week_start = DATE'2026-09-21'
UNION ALL
SELECT week_start, zone, category, CAST(NULL AS DOUBLE), forecast_units, forecast_lower, forecast_upper FROM fc""", []),
    "ds_model_compare": ("Next 12 weeks: Prophet vs ai_forecast vs seasonal naive", """
SELECT f.week_start, f.zone, p.category, SUM(f.forecast_units) AS prophet_units, SUM(f.prophet_sku_units) AS prophet_direct_units,
       SUM(f.seasonal_naive_units) AS seasonal_naive_units,
       SUM(a.forecast_units) AS ai_forecast_units
FROM ml.demand_forecast f
JOIN dim_product p ON f.sku_id = p.sku_id
LEFT JOIN ml.ai_forecast_baseline a ON a.sku_id = f.sku_id AND a.zone = f.zone AND a.week_start = f.week_start
GROUP BY ALL""", []),
    "ds_accuracy": ("Backtest accuracy (per series)", """
SELECT sku_id, zone, category, base_spice, model, actual_units, wape_model, wape_prophet_sku, wape_seasonal_naive, bias_model,
       interval_coverage, wape_model * actual_units AS abs_err_model, wape_prophet_sku * actual_units AS abs_err_prophet,
       wape_seasonal_naive * actual_units AS abs_err_snaive, interval_coverage * backtest_weeks AS covered_weeks, backtest_weeks
FROM ml.forecast_accuracy WHERE actual_units > 0""", [
        ("WAPE Model", "SUM(`abs_err_model`) / SUM(`actual_units`)"),
        ("WAPE Prophet Direct", "SUM(`abs_err_prophet`) / SUM(`actual_units`)"),
        ("Interval Coverage", "SUM(`covered_weeks`) / SUM(`backtest_weeks`)"),
        ("WAPE Seasonal Naive", "SUM(`abs_err_snaive`) / SUM(`actual_units`)"),
    ]),
    "ds_candidates": ("Forecast candidate comparison", """
SELECT level, candidate, wape, is_champion FROM ml.forecast_model_comparison""", []),
    "ds_reorder": ("Reorder recommendations", """
SELECT r.warehouse_name, w.zone, r.sku_id, r.product_name, base_spice, category, on_hand_units, open_po_units, forecast_next_4w_units,
       weeks_of_cover, lead_time_weeks, safety_stock_units, recommended_order_units, stockout_risk, projected_stockout_date,
       forecast_revenue_4w_inr
FROM ml.reorder_recommendation r JOIN dim_warehouse w ON r.warehouse_id = w.warehouse_id""", []),
    "ds_inventory": ("Inventory service level (week x warehouse x spice)", """
SELECT i.week_start, w.warehouse_name, w.zone, i.base_spice, i.category,
       CASE WHEN i.base_spice LIKE '%Chilli%' THEN 'Chilli' ELSE 'Other spices' END AS spice_group,
       SUM(i.demand_units) AS demand_units, SUM(i.shipped_units) AS shipped_units, SUM(i.stockout_days) AS stockout_days,
       SUM(i.closing_stock_value_inr) AS stock_value_inr
FROM agg_inventory_weekly i JOIN dim_warehouse w ON i.warehouse_id = w.warehouse_id
GROUP BY ALL""", [
        ("Fill Rate", "SUM(`shipped_units`) / SUM(`demand_units`)"),
    ]),
    "ds_margin": ("Raw material vs margin (month x spice group)", """
SELECT month, CASE WHEN base_spice_code IN ('RCH', 'KCH') THEN 'Chilli' ELSE 'Other spices' END AS spice_group,
       SUM(net_revenue_inr) AS net_revenue_inr, SUM(gross_margin_inr) AS gross_margin_inr,
       AVG(raw_material_index) AS raw_material_index
FROM agg_margin_monthly GROUP BY ALL""", [
        ("GM %", "SUM(`gross_margin_inr`) / SUM(`net_revenue_inr`)"),
    ]),
    "ds_supplier": ("Supplier performance", """
SELECT supplier_name, supplier_tier, base_spice, quality_grade, COUNT(*) AS pos, SUM(po_value_inr) AS po_value_inr,
       AVG(fill_rate) AS avg_fill_rate, AVG(delay_days) AS avg_delay_days
FROM fact_purchase_orders GROUP BY ALL""", []),
    "ds_rfm": ("Customer RFM segments", """
SELECT rfm_segment, zone, loyalty_tier, COUNT(*) AS customers, SUM(monetary_inr) AS monetary_inr, AVG(frequency) AS avg_orders,
       AVG(recency_days) AS avg_recency_days
FROM agg_customer_rfm GROUP BY ALL""", []),
    "ds_dq": ("Data quality summary", """
SELECT source_table, dq_reason, rows,
       CASE WHEN dq_reason LIKE '%accepted%' THEN 'Accepted (flagged)' ELSE 'Rejected (quarantined)' END AS outcome
FROM dq_summary""", []),
    "ds_layers": ("Row counts by layer", """
SELECT 'Bronze raw lines' AS layer, 1 AS ord, COUNT(*) AS rows FROM bronze.sales_lines_raw
UNION ALL SELECT 'Silver clean lines', 2, COUNT(*) FROM silver.sales_lines
UNION ALL SELECT 'Gold fact lines', 3, COUNT(*) FROM fact_sales_line
UNION ALL SELECT 'Gold orders', 4, COUNT(*) FROM fact_orders""", []),
}


def ds_json(name, display, sql, cols):
    lines = [l + "\n" for l in sql.strip().split("\n")]
    d = {"name": name, "displayName": display, "queryLines": lines}
    if cols:
        d["columns"] = [{"displayName": n, "expression": e} for n, e in cols]
    return d


# ------------------------------------------------------------------ widget helpers
INR = {"type": "number-currency", "currencyCode": "INR", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 1}}
PCT = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}
NUM = {"type": "number", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 1}}
BAD, GOOD, WARN = "#E5533D", "#2A9D8F", "#F4A261"
_n = [0]


def wname(prefix):
    _n[0] += 1
    return f"{prefix}-{_n[0]}"


def fld(name, expr):
    return {"name": name, "expression": expr}


def q(ds, fields, disagg=False, orders=None):
    qq = {"datasetName": ds, "fields": fields, "disaggregated": disagg}
    if orders:
        qq["orders"] = orders
    return [{"name": "main_query", "query": qq}]


def place(widget, x, y, w, h):
    return {"widget": widget, "position": {"x": x, "y": y, "width": w, "height": h}}


def text(md, x, y, w, h):
    return place({"name": wname("text"), "multilineTextboxSpec": {"lines": [md]}}, x, y, w, h)


def counter(ds, title, measure_name, expr, fmt, x, y, w=3, h=3, period=None, desc=None, template=None):
    fields = [fld(measure_name, expr)]
    enc = {"value": {"fieldName": measure_name, "displayName": title, "format": fmt}}
    if template:
        enc["value"]["formatTemplate"] = template
    if period:
        fields.append(fld(f"monthly({period})", f'DATE_TRUNC("MONTH", `{period}`)'))
        enc["period"] = {"fieldName": f"monthly({period})"}
    frame = {"title": title, "showTitle": True}
    if desc:
        frame.update({"showDescription": True, "description": desc})
    return place({"name": wname("kpi"), "queries": q(ds, fields),
                  "spec": {"version": 2, "widgetType": "counter", "encodings": enc, "frame": frame}}, x, y, w, h)


def chart(kind, ds, title, x_enc, y_enc, fields, x, y, w, h, color=None, mark=None, desc=None, annotations=None, orders=None, disagg=False):
    enc = {"x": x_enc, "y": y_enc}
    if color:
        enc["color"] = color
    spec = {"version": 3, "widgetType": kind, "encodings": enc, "frame": {"title": title, "showTitle": True}}
    if desc:
        spec["frame"].update({"showDescription": True, "description": desc})
    if mark:
        spec["mark"] = mark
    if annotations:
        spec["annotations"] = annotations
    return place({"name": wname(kind), "queries": q(ds, fields, disagg, orders), "spec": spec}, x, y, w, h)


def table(ds, title, columns, x, y, w, h, orders=None, desc=None):
    fields = [fld(c["fieldName"], f"`{c['fieldName']}`") for c in columns]
    frame = {"title": title, "showTitle": True}
    if desc:
        frame.update({"showDescription": True, "description": desc})
    return place({"name": wname("table"), "queries": q(ds, fields, True, orders),
                  "spec": {"version": 2, "widgetType": "table", "encodings": {"columns": columns}, "frame": frame}}, x, y, w, h)


def vline(date, label, color=BAD):
    return {"type": "vertical-line", "encodings": {"x": {"dataValue": f"{date}T00:00:00.000", "dataType": "DATETIME"},
                                                   "label": {"value": label}, "color": {"value": {"hex": color}}}}


def temporal(fieldname, name=None):
    return {"fieldName": fieldname, "scale": {"type": "temporal"}, "displayName": name or fieldname}


def quant(fieldname, name, fmt=None):
    d = {"fieldName": fieldname, "scale": {"type": "quantitative"}, "displayName": name}
    if fmt:
        d["format"] = fmt
    return d


def cat(fieldname, name, sort_by_value=False):
    d = {"fieldName": fieldname, "scale": {"type": "categorical"}, "displayName": name}
    if sort_by_value:
        d["scale"]["sort"] = {"by": "y-reversed"}
    return d


M = lambda n: fld(f"measure({n})", f"MEASURE(`{n}`)")  # noqa: E731
MON = lambda c: fld(f"monthly({c})", f'DATE_TRUNC("MONTH", `{c}`)')  # noqa: E731
WK = lambda c: fld(f"weekly({c})", f'DATE_TRUNC("WEEK", `{c}`)')  # noqa: E731
SUM = lambda c: fld(f"sum({c})", f"SUM(`{c}`)")  # noqa: E731
F = lambda c: fld(c, f"`{c}`")  # noqa: E731

CHANNEL_GROUP_COLORS = [{"value": "Offline", "color": "#264653"}, {"value": "Online", "color": "#E76F51"}, {"value": "B2B", "color": "#E9C46A"}]
RISK_COLORS = [{"value": "High", "color": BAD}, {"value": "Medium", "color": WARN}, {"value": "Low", "color": GOOD}]

# ------------------------------------------------------------------ pages
pages = []

# 1. Executive overview
L = [
    text("# 🌶️ SpiceRoute India — Commercial Command Center\n"
         "Net revenue is **excluding GST**, after channel margin and promotional discounts; delivered orders only. "
         "Indian fiscal year runs April–March. Use the **Filters** page to slice every page by date, zone, channel and category.", 0, 0, 12, 2),
    counter("ds_sales", "Net Revenue", "measure(Net Revenue)", "MEASURE(`Net Revenue`)", INR, 0, 2, period="order_date"),
    counter("ds_sales", "Gross Margin %", "measure(Gross Margin %)", "MEASURE(`Gross Margin %`)", PCT, 3, 2, period="order_date"),
    counter("ds_orders", "Orders", "measure(Total Orders)", "MEASURE(`Total Orders`)", NUM, 6, 2, period="order_date"),
    counter("ds_orders", "Avg Order Value", "measure(AOV)", "MEASURE(`AOV`)",
            {"type": "number", "decimalPlaces": {"type": "exact", "places": 0}}, 9, 2, period="order_date", template="₹{{ @formatted }}"),
    chart("bar", "ds_sales", "Monthly net revenue by channel group", temporal("monthly(order_date)", "Month"),
          quant("sum(net_revenue_inr)", "Net revenue", INR), [MON("order_date"), SUM("net_revenue_inr"), F("channel_group")],
          0, 5, 8, 6, color={"fieldName": "channel_group", "scale": {"type": "categorical", "mappings": CHANNEL_GROUP_COLORS}, "displayName": "Channel group"},
          annotations=[vline("2024-11-01", "Diwali 2024", "#6A4C93"), vline("2025-10-20", "Diwali 2025", "#6A4C93")]),
    place({"name": wname("pie"), "queries": q("ds_sales", [SUM("net_revenue_inr"), F("channel_name")]),
           "spec": {"version": 3, "widgetType": "pie", "frame": {"title": "Channel mix", "showTitle": True},
                    "encodings": {"angle": quant("sum(net_revenue_inr)", "Net revenue", INR),
                                  "color": cat("channel_name", "Channel"), "label": {"show": True}}}}, 8, 5, 4, 6),
    chart("bar", "ds_sales", "Net revenue by zone", cat("zone", "Zone"), quant("sum(net_revenue_inr)", "Net revenue", INR),
          [F("zone"), SUM("net_revenue_inr")], 0, 11, 4, 6, orders=[{"direction": "DESC", "expression": "SUM(`net_revenue_inr`)"}]),
    chart("bar", "ds_sales", "Top base spices by revenue", quant("sum(net_revenue_inr)", "Net revenue", INR),
          {"fieldName": "base_spice", "scale": {"type": "categorical"}, "displayName": "Spice"},
          [F("base_spice"), SUM("net_revenue_inr")], 4, 11, 4, 6, orders=[{"direction": "DESC", "expression": "SUM(`net_revenue_inr`)"}]),
    chart("line", "ds_sales", "Gross margin % by category", temporal("monthly(order_date)", "Month"),
          quant("measure(Gross Margin %)", "Gross margin", PCT), [MON("order_date"), M("Gross Margin %"), F("category")],
          8, 11, 4, 6, color=cat("category", "Category")),
]
pages.append(("overview", "1 · Executive Overview", L))

# 2. Product & region
L = [
    text("## Product & Region\nRegional taste is strong: sambar/rasam in the **South**, panch phoron in the **East**, goda masala in the **West**, garam masala in the **North**.", 0, 0, 12, 1),
    place({"name": wname("heatmap"), "queries": q("ds_sales", [F("zone"), F("category"), SUM("net_revenue_inr")]),
           "spec": {"version": 3, "widgetType": "heatmap", "frame": {"title": "Revenue: category × zone", "showTitle": True},
                    "encodings": {"x": cat("zone", "Zone"), "y": cat("category", "Category"),
                                  "color": {"fieldName": "sum(net_revenue_inr)", "displayName": "Net revenue", "format": INR,
                                            "scale": {"type": "quantitative", "colorRamp": {"mode": "custom-sequential", "colors": {"start": "#FFF3E0", "end": "#B23A48"}}}},
                                  "label": {"show": True}}}}, 0, 1, 6, 6),
    place({"name": wname("map"), "queries": q("ds_state_map", [F("lat"), F("lon"), F("state"), SUM("net_revenue_inr")]),
           "spec": {"version": 2, "widgetType": "symbol-map", "frame": {"title": "Revenue by state", "showTitle": True},
                    "mark": {"opacity": 0.75},
                    "encodings": {"coordinates": {"latitude": {"fieldName": "lat"}, "longitude": {"fieldName": "lon"}},
                                  "size": {"fieldName": "sum(net_revenue_inr)", "scale": {"type": "quantitative"}, "displayName": "Net revenue"},
                                  "color": {"fieldName": "sum(net_revenue_inr)", "displayName": "Net revenue",
                                            "scale": {"type": "quantitative", "colorRamp": {"mode": "custom-sequential", "colors": {"start": "#F4A261", "end": "#9B2226"}}}}}}},
          6, 1, 6, 6),
    place({"name": wname("pivot"), "queries": q("ds_sales", [F("base_spice"), F("zone"), SUM("volume_kg")]),
           "spec": {"version": 3, "widgetType": "pivot", "frame": {"title": "Volume (kg): spice × zone", "showTitle": True},
                    "encodings": {"rows": [{"fieldName": "base_spice", "displayName": "Spice"}],
                                  "columns": [{"fieldName": "zone", "displayName": "Zone"}],
                                  "cell": {"type": "multi-cell", "fields": [{"fieldName": "sum(volume_kg)", "cellType": "color-scale", "displayName": "kg",
                                                                             "format": NUM}]}}}}, 0, 7, 8, 8),
    chart("bar", "ds_sales", "Volume by season and category", cat("season", "Season"), quant("sum(volume_kg)", "Volume (kg)", NUM),
          [F("season"), SUM("volume_kg"), F("category")], 8, 7, 4, 8, color=cat("category", "Category")),
]
pages.append(("product_region", "2 · Product & Region", L))

# 3. Festivals & promotions
L = [
    text("## Festivals & Promotions\nFestivals shift both volume and mix. B2B sell-in rises ~10 days before consumer sell-out. "
         "Promo lift = average daily channel revenue during the promo vs the 28 days before.", 0, 0, 12, 1),
    chart("line", "ds_festival_daily", "Daily net revenue by zone around the festive seasons", temporal("order_date", "Day"),
          quant("sum(net_revenue_inr)", "Net revenue", INR), [F("order_date"), SUM("net_revenue_inr"), F("zone")], 0, 1, 12, 6,
          color=cat("zone", "Zone"),
          annotations=[vline("2024-09-07", "Ganesh Chaturthi", "#6A4C93"), vline("2024-10-10", "Durga Puja", "#6A4C93"),
                       vline("2024-11-01", "Diwali", BAD), vline("2025-01-14", "Pongal", "#6A4C93"), vline("2025-03-14", "Holi", "#6A4C93"),
                       vline("2025-03-31", "Eid al-Fitr", "#6A4C93"), vline("2025-10-20", "Diwali", BAD)]),
    table("ds_festival", "Biggest festival uplifts (spice × zone)", [
        {"fieldName": "festival_name", "displayName": "Festival"}, {"fieldName": "year", "displayName": "Year"},
        {"fieldName": "zone", "displayName": "Zone"}, {"fieldName": "base_spice", "displayName": "Spice"},
        {"fieldName": "festival_daily_revenue_inr", "displayName": "Festival ₹/day", "format": INR},
        {"fieldName": "baseline_daily_revenue_inr", "displayName": "Baseline ₹/day", "format": INR},
        {"fieldName": "uplift", "displayName": "Uplift", "format": PCT}],
        0, 7, 6, 8, orders=[{"direction": "DESC", "expression": "`uplift`"}]),
    table("ds_promo", "Promotion effectiveness", [
        {"fieldName": "promo_name", "displayName": "Promotion"}, {"fieldName": "channel_name", "displayName": "Channel"},
        {"fieldName": "discount_pct", "displayName": "Discount", "format": PCT},
        {"fieldName": "revenue_lift", "displayName": "Revenue lift", "format": PCT,
         "style": {"type": "basic", "rules": [{"condition": {"operand": {"type": "data-value", "value": "0"}, "operator": "<"}, "foregroundColor": {"hex": BAD}}]}},
        {"fieldName": "discount_cost_inr", "displayName": "Discount cost", "format": INR}],
        6, 7, 6, 8, orders=[{"direction": "DESC", "expression": "`revenue_lift`"}]),
    chart("bar", "ds_sales", "Share of revenue sold on promotion, by channel", cat("channel_name", "Channel"),
          quant("measure(Promo Share)", "Promo share", PCT), [F("channel_name"), M("Promo Share")], 0, 15, 12, 5),
]
pages.append(("festivals", "3 · Festivals & Promotions", L))

# 4. Demand forecast
L = [
    text("## Demand Forecast — next 12 weeks\nProphet with Indian festivals as holidays and promo intensity as a regressor, fitted per **base spice × zone** "
         "and split to 882 SKU series by recent mix, blended 70/30 with seasonal naive — the champion of 6 candidates in a 3 × 12-week rolling backtest. "
         "Intervals are split-conformal (calibrated on backtest errors).", 0, 0, 12, 2),
    counter("ds_accuracy", "WAPE · champion model", "measure(WAPE Model)", "MEASURE(`WAPE Model`)", PCT, 0, 2, 3, 3,
            desc="SKU x zone x week backtest error (lower is better)"),
    counter("ds_accuracy", "WAPE · Seasonal naive", "measure(WAPE Seasonal Naive)", "MEASURE(`WAPE Seasonal Naive`)", PCT, 3, 2, 3, 3,
            desc="Same week last year baseline"),
    counter("ds_accuracy", "80% interval coverage", "measure(Interval Coverage)", "MEASURE(`Interval Coverage`)", PCT, 6, 2, 3, 3,
            desc="Share of backtest weeks inside the interval (target ~80%)"),
    counter("ds_model_compare", "Forecast units (next 12 wks)", "sum(prophet_units)", "SUM(`prophet_units`)", NUM, 9, 2, 3, 3),
    place({"name": wname("forecast"), "queries": q("ds_forecast", [
        F("week_start"), SUM("actual_units"), SUM("forecast_units"), SUM("forecast_lower"), SUM("forecast_upper")]),
        "spec": {"version": 1, "widgetType": "forecast-line", "frame": {"title": "Weekly units — actuals + champion forecast (calibrated 80% interval)", "showTitle": True},
                 "encodings": {"x": {"fieldName": "week_start", "scale": {"type": "temporal"}},
                               "y": {"scale": {"type": "quantitative", "domainMin": 0},
                                     "original": {"fieldName": "sum(actual_units)", "displayName": "Actual units"},
                                     "prediction": {"fieldName": "sum(forecast_units)", "displayName": "Forecast"},
                                     "predictionUpper": {"fieldName": "sum(forecast_upper)"},
                                     "predictionLower": {"fieldName": "sum(forecast_lower)"}}},
                 "annotations": [vline("2026-11-08", "Diwali 2026", "#6A4C93")]}}, 0, 5, 12, 7),
    chart("line", "ds_model_compare", "Model comparison over the horizon", temporal("week_start", "Week"),
          {"scale": {"type": "quantitative"}, "fields": [
              {"fieldName": "sum(prophet_units)", "displayName": "Champion (hierarchical)"},
              {"fieldName": "sum(prophet_direct_units)", "displayName": "Prophet direct (SKU)"},
              {"fieldName": "sum(ai_forecast_units)", "displayName": "ai_forecast()"},
              {"fieldName": "sum(seasonal_naive_units)", "displayName": "Seasonal naive"}]},
          [F("week_start"), SUM("prophet_units"), SUM("prophet_direct_units"), SUM("ai_forecast_units"), SUM("seasonal_naive_units")], 0, 12, 6, 6),
    chart("bar", "ds_accuracy", "Backtest WAPE by category", cat("category", "Category"),
          {"scale": {"type": "quantitative"}, "fields": [
              {"fieldName": "measure(WAPE Model)", "displayName": "Champion", "format": PCT},
              {"fieldName": "measure(WAPE Prophet Direct)", "displayName": "Prophet direct", "format": PCT},
              {"fieldName": "measure(WAPE Seasonal Naive)", "displayName": "Seasonal naive", "format": PCT}]},
          [F("category"), M("WAPE Model"), M("WAPE Prophet Direct"), M("WAPE Seasonal Naive")], 6, 12, 6, 6, mark={"layout": "group"}),
    table("ds_candidates", "Candidate models — backtest WAPE", [
        {"fieldName": "level", "displayName": "Evaluated at"}, {"fieldName": "candidate", "displayName": "Candidate"},
        {"fieldName": "wape", "displayName": "WAPE", "format": PCT}, {"fieldName": "is_champion", "displayName": "Champion"}],
        0, 18, 12, 6, orders=[{"direction": "ASC", "expression": "`level`"}, {"direction": "ASC", "expression": "`wape`"}]),
]
pages.append(("forecast", "4 · Demand Forecast", L))

# 5. Inventory & supply
L = [
    text("## Inventory & Supply\nThe **2024 Andhra/Telangana chilli crop failure** (Jun–Oct 2024) is visible here: refused supplier allocations, "
         "a low-grade alternate supplier, stockouts in South/West DCs, and a margin squeeze until the July 2024 MRP increase.", 0, 0, 12, 2),
    counter("ds_inventory", "Fill rate (3 yrs)", "measure(Fill Rate)", "MEASURE(`Fill Rate`)", PCT, 0, 2, 4, 3),
    counter("ds_reorder", "SKU×DC at HIGH stockout risk", "count(sku_id)", "COUNT(`sku_id`)", NUM, 4, 2, 4, 3,
            desc="Use the risk table filter below"),
    counter("ds_reorder", "Recommended order units", "sum(recommended_order_units)", "SUM(`recommended_order_units`)", NUM, 8, 2, 4, 3),
    chart("line", "ds_inventory", "Weekly fill rate — chilli vs other spices", temporal("week_start", "Week"),
          quant("measure(Fill Rate)", "Fill rate", PCT), [F("week_start"), M("Fill Rate"), F("spice_group")], 0, 5, 6, 6,
          color={"fieldName": "spice_group", "scale": {"type": "categorical", "mappings": [{"value": "Chilli", "color": BAD}, {"value": "Other spices", "color": GOOD}]}, "displayName": "Group"},
          annotations=[vline("2024-06-01", "Crop failure", BAD)]),
    chart("line", "ds_margin", "Gross margin % — chilli vs other spices", temporal("month", "Month"),
          quant("measure(GM %)", "Gross margin", PCT), [F("month"), M("GM %"), F("spice_group")], 6, 5, 6, 6,
          color={"fieldName": "spice_group", "scale": {"type": "categorical", "mappings": [{"value": "Chilli", "color": BAD}, {"value": "Other spices", "color": GOOD}]}, "displayName": "Group"},
          annotations=[vline("2024-07-01", "MRP +12% (chilli)", "#6A4C93")]),
    chart("bar", "ds_inventory", "Stockout days by DC", cat("warehouse_name", "Warehouse"), quant("sum(stockout_days)", "Stockout days", NUM),
          [F("warehouse_name"), SUM("stockout_days"), F("spice_group")], 0, 11, 6, 6,
          color={"fieldName": "spice_group", "scale": {"type": "categorical", "mappings": [{"value": "Chilli", "color": BAD}, {"value": "Other spices", "color": GOOD}]}, "displayName": "Group"}),
    table("ds_supplier", "Supplier performance", [
        {"fieldName": "supplier_name", "displayName": "Supplier"}, {"fieldName": "supplier_tier", "displayName": "Tier"},
        {"fieldName": "base_spice", "displayName": "Spice"}, {"fieldName": "quality_grade", "displayName": "Grade"},
        {"fieldName": "pos", "displayName": "POs"}, {"fieldName": "avg_fill_rate", "displayName": "Fill rate", "format": PCT},
        {"fieldName": "avg_delay_days", "displayName": "Avg delay (d)", "format": {"type": "number", "decimalPlaces": {"type": "max", "places": 1}}}],
        6, 11, 6, 6, orders=[{"direction": "ASC", "expression": "`avg_fill_rate`"}]),
    table("ds_reorder", "Reorder recommendations (as of 30 Sep 2026)", [
        {"fieldName": "stockout_risk", "displayName": "Risk",
         "style": {"type": "basic", "rules": [
             {"condition": {"operand": {"type": "data-value", "value": "High"}, "operator": "="}, "backgroundColor": {"hex": BAD}, "foregroundColor": {"hex": "#FFFFFF"}},
             {"condition": {"operand": {"type": "data-value", "value": "Medium"}, "operator": "="}, "backgroundColor": {"hex": WARN}}]}},
        {"fieldName": "warehouse_name", "displayName": "DC"}, {"fieldName": "product_name", "displayName": "Product"},
        {"fieldName": "on_hand_units", "displayName": "On hand", "format": NUM}, {"fieldName": "open_po_units", "displayName": "Open PO", "format": NUM},
        {"fieldName": "forecast_next_4w_units", "displayName": "Fcst 4w", "format": NUM},
        {"fieldName": "weeks_of_cover", "displayName": "Weeks cover"},
        {"fieldName": "recommended_order_units", "displayName": "Order qty", "format": NUM},
        {"fieldName": "projected_stockout_date", "displayName": "Stockout by"}],
        0, 17, 12, 8, orders=[{"direction": "ASC", "expression": "`weeks_of_cover`"}]),
]
pages.append(("inventory", "5 · Inventory & Supply", L))

# 6. Customers & data health
L = [
    text("## Customers & Data Health\nRFM segments for identified consumers (D2C, marketplace, quick commerce, loyalty). "
         "Data-quality issues injected at source are caught by pipeline expectations and quarantined.", 0, 0, 12, 1),
    chart("bar", "ds_rfm", "Customers by RFM segment", cat("rfm_segment", "Segment"), quant("sum(customers)", "Customers", NUM),
          [F("rfm_segment"), SUM("customers")], 0, 1, 6, 6, orders=[{"direction": "DESC", "expression": "SUM(`customers`)"}]),
    chart("bar", "ds_rfm", "Customer value by segment", cat("rfm_segment", "Segment"), quant("sum(monetary_inr)", "Lifetime revenue", INR),
          [F("rfm_segment"), SUM("monetary_inr"), F("loyalty_tier")], 6, 1, 6, 6, color=cat("loyalty_tier", "Loyalty tier")),
    chart("bar", "ds_layers", "Order lines through the medallion layers", cat("layer", "Layer"), quant("sum(rows)", "Rows", NUM),
          [F("layer"), SUM("rows")], 0, 7, 6, 6),
    chart("bar", "ds_dq", "Data-quality outcomes by reason", quant("sum(rows)", "Rows", NUM),
          {"fieldName": "dq_reason", "scale": {"type": "categorical"}, "displayName": "Reason"},
          [F("dq_reason"), SUM("rows"), F("outcome")], 6, 7, 6, 6,
          color={"fieldName": "outcome", "scale": {"type": "categorical", "mappings": [{"value": "Rejected (quarantined)", "color": BAD}, {"value": "Accepted (flagged)", "color": GOOD}]}, "displayName": "Outcome"}),
]
pages.append(("customers_dq", "6 · Customers & Data Health", L))


# ------------------------------------------------------------------ global filters
def filt(kind, title, binds, x, y, w=3, h=2):
    queries, fields = [], []
    for i, (ds, col) in enumerate(binds):
        qn = f"f_{ds}_{col}"
        queries.append({"name": qn, "query": {"datasetName": ds, "fields": [F(col)], "disaggregated": False}})
        fields.append({"fieldName": col, "queryName": qn})
    return place({"name": wname("filter"), "queries": queries,
                  "spec": {"version": 2, "widgetType": kind, "encodings": {"fields": fields}, "frame": {"title": title, "showTitle": True}}}, x, y, w, h)


filters = [
    filt("filter-date-range-picker", "Date", [("ds_sales", "order_date"), ("ds_orders", "order_date"), ("ds_state_map", "order_date"),
                                               ("ds_festival_daily", "order_date")], 0, 0),
    filt("filter-multi-select", "Zone", [("ds_sales", "zone"), ("ds_orders", "zone"), ("ds_state_map", "zone"), ("ds_festival", "zone"),
                                         ("ds_festival_daily", "zone"), ("ds_forecast", "zone"), ("ds_model_compare", "zone"),
                                         ("ds_accuracy", "zone"), ("ds_reorder", "zone"), ("ds_inventory", "zone"), ("ds_rfm", "zone")], 3, 0),
    filt("filter-multi-select", "Channel", [("ds_sales", "channel_name"), ("ds_orders", "channel_name"), ("ds_state_map", "channel_name"),
                                            ("ds_promo", "channel_name")], 6, 0),
    filt("filter-multi-select", "Category", [("ds_sales", "category"), ("ds_state_map", "category"), ("ds_festival", "category"),
                                             ("ds_forecast", "category"), ("ds_model_compare", "category"), ("ds_accuracy", "category"),
                                             ("ds_reorder", "category"), ("ds_inventory", "category")], 9, 0),
    filt("filter-single-select", "Stockout risk", [("ds_reorder", "stockout_risk")], 0, 2),
    filt("filter-single-select", "Festival", [("ds_festival", "festival_name")], 3, 2),
    filt("filter-multi-select", "Fiscal year", [("ds_sales", "fiscal_year")], 6, 2),
    filt("filter-multi-select", "Warehouse", [("ds_reorder", "warehouse_name"), ("ds_inventory", "warehouse_name")], 9, 2),
]

dash = {
    "datasets": [ds_json(k, *v) for k, v in DATASETS.items()],
    "pages": [{"name": n, "displayName": d, "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1", "layout": lay} for n, d, lay in pages]
    + [{"name": "filters", "displayName": "Filters", "pageType": "PAGE_TYPE_GLOBAL_FILTERS", "layoutVersion": "GRID_V1", "layout": filters}],
    "uiSettings": {"theme": {
        "canvasBackgroundColor": {"light": "#FBF8F3", "dark": "#1C1A17"},
        "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#26231F"},
        "widgetBorderColor": {"light": "#FFFFFF", "dark": "#26231F"},
        "fontColor": {"light": "#2B2118", "dark": "#F3EDE4"},
        "selectionColor": {"light": "#2272B4", "dark": "#8ACAFF"},
        "visualizationColors": ["#B23A48", "#E9C46A", "#2A9D8F", "#264653", "#F4A261", "#6A4C93", "#8AB17D", "#E76F51"],
        "widgetHeaderAlignment": "LEFT", "widgetCornerRadius": 10}},
}
genie_id = os.environ.get("GENIE_SPACE_ID", "01f1c0bd7bd6175ba0b691784fd34971")  # "Ask SpiceRoute"
if genie_id:
    dash["uiSettings"]["genieSpace"] = {"isEnabled": True, "overrideId": genie_id, "enablementMode": "ENABLED"}

out = os.path.join(HERE, "spiceroute_command_center.lvdash.json")
with open(out, "w") as f:
    json.dump(dash, f, indent=1)
print("wrote", out)

if "--test" in sys.argv:
    bad = 0
    for k, (disp, sql, _) in DATASETS.items():
        body = json.dumps({"warehouse_id": os.environ.get("WH", "9cd430b8a1739112"), "catalog": "spiceroute", "schema": "gold",
                           "statement": f"SELECT COUNT(*) AS n FROM ({sql}) t", "wait_timeout": "50s"})
        r = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "--json", body, "--profile", "brilworks"],
                           capture_output=True, text=True)
        j = json.loads(r.stdout or "{}")
        st = j.get("status", {})
        rows = j.get("result", {}).get("data_array", [["?"]])[0][0] if st.get("state") == "SUCCEEDED" else None
        if st.get("state") != "SUCCEEDED" or rows in ("0", None):
            bad += 1
        print(f"{k:20s} {st.get('state')} rows={rows} {st.get('error', {}).get('message', '')[:200]}")
    sys.exit(1 if bad else 0)
