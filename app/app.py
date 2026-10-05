"""SpiceRoute Demand Planner — Databricks App (Streamlit).

Tabs: Forecast explorer · Reorder planner (writes planner decisions) · Ask Genie.
Reads spiceroute.gold / spiceroute.ml via the SQL warehouse as the app service principal.
"""
import os
from datetime import datetime, timezone

import altair as alt
import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

st.set_page_config(page_title="SpiceRoute Demand Planner", page_icon="🌶️", layout="wide")

CAT = os.getenv("SPICEROUTE_CATALOG", "spiceroute")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID")
cfg = Config()


@st.cache_resource
def connection():
    return sql.connect(server_hostname=cfg.host, http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
                       credentials_provider=lambda: cfg.authenticate)


@st.cache_resource
def workspace():
    return WorkspaceClient()


def query(q, params=None) -> pd.DataFrame:
    with connection().cursor() as cur:
        cur.execute(q, params or {})
        return cur.fetchall_arrow().to_pandas()


@st.cache_data(ttl=600)
def cached(q, params_items=()):
    return query(q, dict(params_items))


def current_user():
    try:
        h = st.context.headers
        return h.get("X-Forwarded-Email") or h.get("X-Forwarded-Preferred-Username") or "unknown"
    except Exception:
        return "unknown"


st.title("🌶️ SpiceRoute Demand Planner")
st.caption(f"Actuals to 30 Sep 2026 · 12-week Prophet forecast · catalog `{CAT}` · signed in as {current_user()}")

tab_fc, tab_ro, tab_genie = st.tabs(["📈 Forecast explorer", "📦 Reorder planner", "💬 Ask Genie"])

# ---------------------------------------------------------------- forecast explorer
with tab_fc:
    spices = cached(f"SELECT DISTINCT base_spice FROM {CAT}.gold.dim_product ORDER BY 1")["base_spice"].tolist()
    c1, c2, c3 = st.columns([1, 2, 2])
    zone = c1.selectbox("Zone", ["North", "South", "East", "West", "Central", "North-East"])
    spice = c2.selectbox("Base spice", spices, index=spices.index("Garam Masala") if "Garam Masala" in spices else 0)
    skus = cached(f"SELECT sku_id, product_name FROM {CAT}.gold.dim_product WHERE base_spice = :s ORDER BY pack_size_g", (("s", spice),))
    sku_opt = c3.selectbox("SKU (optional)", ["All pack sizes"] + skus["product_name"].tolist())
    sku_filter = "" if sku_opt == "All pack sizes" else "AND p.product_name = :sku"
    params = {"z": zone, "s": spice}
    if sku_filter:
        params["sku"] = sku_opt

    actual = query(f"""
        SELECT w.week_start, SUM(w.units) AS units FROM {CAT}.gold.agg_sales_weekly_sku_zone w
        JOIN {CAT}.gold.dim_product p ON w.sku_id = p.sku_id
        WHERE w.zone = :z AND p.base_spice = :s {sku_filter} AND w.week_start BETWEEN '2025-01-06' AND '2026-09-21'
        GROUP BY 1 ORDER BY 1""", params)
    fc = query(f"""
        SELECT f.week_start, SUM(f.forecast_units) AS prophet, SUM(f.forecast_lower) AS lower, SUM(f.forecast_upper) AS upper,
               SUM(f.seasonal_naive_units) AS seasonal_naive, SUM(a.forecast_units) AS ai_forecast
        FROM {CAT}.ml.demand_forecast f JOIN {CAT}.gold.dim_product p ON f.sku_id = p.sku_id
        LEFT JOIN {CAT}.ml.ai_forecast_baseline a ON a.sku_id = f.sku_id AND a.zone = f.zone AND a.week_start = f.week_start
        WHERE f.zone = :z AND p.base_spice = :s {sku_filter} GROUP BY 1 ORDER BY 1""", params)
    acc = query(f"""
        SELECT SUM(a.wape_prophet * a.actual_units) / SUM(a.actual_units) AS wape_prophet,
               SUM(a.wape_seasonal_naive * a.actual_units) / SUM(a.actual_units) AS wape_snaive
        FROM {CAT}.ml.forecast_accuracy a JOIN {CAT}.gold.dim_product p ON a.sku_id = p.sku_id
        WHERE a.zone = :z AND p.base_spice = :s {sku_filter}""", params)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Next 4 weeks (units)", f"{fc['prophet'].head(5).tail(4).sum():,.0f}")
    m2.metric("Next 12 weeks (units)", f"{fc['prophet'].sum():,.0f}")
    if not acc.empty and acc["wape_prophet"].notna().all():
        m3.metric("Backtest WAPE · Prophet", f"{acc['wape_prophet'][0]:.1%}")
        m4.metric("Backtest WAPE · seasonal naive", f"{acc['wape_snaive'][0]:.1%}",
                  delta=f"{(acc['wape_snaive'][0] - acc['wape_prophet'][0]):.1%} better" if acc['wape_prophet'][0] < acc['wape_snaive'][0] else None)

    long = pd.concat([
        actual.rename(columns={"units": "value"}).assign(series="Actual"),
        fc[["week_start", "prophet"]].rename(columns={"prophet": "value"}).assign(series="Prophet forecast"),
        fc[["week_start", "ai_forecast"]].rename(columns={"ai_forecast": "value"}).assign(series="ai_forecast()"),
        fc[["week_start", "seasonal_naive"]].rename(columns={"seasonal_naive": "value"}).assign(series="Seasonal naive"),
    ])
    long["week_start"] = pd.to_datetime(long["week_start"])
    fc_band = fc.assign(week_start=pd.to_datetime(fc["week_start"]))
    colors = alt.Scale(domain=["Actual", "Prophet forecast", "ai_forecast()", "Seasonal naive"],
                       range=["#264653", "#B23A48", "#2A9D8F", "#B0A89E"])
    band = alt.Chart(fc_band).mark_area(opacity=0.18, color="#B23A48").encode(x="week_start:T", y="lower:Q", y2="upper:Q")
    lines = alt.Chart(long.dropna()).mark_line(point=True).encode(
        x=alt.X("week_start:T", title="Week"), y=alt.Y("value:Q", title="Units"),
        color=alt.Color("series:N", scale=colors, title=None),
        strokeDash=alt.condition(alt.datum.series == "Actual", alt.value([1, 0]), alt.value([5, 3])),
        tooltip=["series", "week_start:T", alt.Tooltip("value:Q", format=",.0f")])
    st.altair_chart((band + lines).properties(height=380), use_container_width=True)
    st.caption("Shaded band = Prophet 80% interval. Diwali 2026 falls on 8 Nov (week of 2 Nov).")

# ---------------------------------------------------------------- reorder planner
with tab_ro:
    wh = cached(f"SELECT DISTINCT warehouse_name FROM {CAT}.ml.reorder_recommendation ORDER BY 1")["warehouse_name"].tolist()
    c1, c2 = st.columns([2, 1])
    sel_wh = c1.multiselect("Distribution centre", wh, default=[w for w in wh if "Chennai" in w] or wh[:1])
    sel_risk = c2.multiselect("Stockout risk", ["High", "Medium", "Low"], default=["High"])
    if sel_wh and sel_risk:
        ph_wh = ", ".join(f":w{i}" for i in range(len(sel_wh)))
        ph_rk = ", ".join(f":r{i}" for i in range(len(sel_risk)))
        p = {**{f"w{i}": v for i, v in enumerate(sel_wh)}, **{f"r{i}": v for i, v in enumerate(sel_risk)}}
        recs = query(f"""
            SELECT r.warehouse_id, r.warehouse_name, r.sku_id, r.product_name, r.stockout_risk, r.on_hand_units, r.open_po_units,
                   r.forecast_next_4w_units, r.weeks_of_cover, r.recommended_order_units, r.projected_stockout_date,
                   o.decision AS last_decision, o.override_units AS last_override
            FROM {CAT}.ml.reorder_recommendation r
            LEFT JOIN (SELECT * FROM {CAT}.ml.reorder_overrides
                       QUALIFY row_number() OVER (PARTITION BY warehouse_id, sku_id ORDER BY decided_at DESC) = 1) o
              ON r.warehouse_id = o.warehouse_id AND r.sku_id = o.sku_id
            WHERE r.warehouse_name IN ({ph_wh}) AND r.stockout_risk IN ({ph_rk})
            ORDER BY r.weeks_of_cover""", p)
        k1, k2, k3 = st.columns(3)
        k1.metric("Items", len(recs))
        k2.metric("Recommended units", f"{recs['recommended_order_units'].sum():,.0f}")
        k3.metric("Already decided", int(recs["last_decision"].notna().sum()))
        edit = recs.assign(decision=recs["last_decision"].fillna(""), override_units=recs["last_override"],
                           comment="")[["warehouse_name", "product_name", "stockout_risk", "on_hand_units", "open_po_units",
                                         "forecast_next_4w_units", "weeks_of_cover", "recommended_order_units",
                                         "projected_stockout_date", "decision", "override_units", "comment"]]
        edited = st.data_editor(
            edit, hide_index=True, use_container_width=True, key="ro_editor",
            disabled=[c for c in edit.columns if c not in ("decision", "override_units", "comment")],
            column_config={
                "decision": st.column_config.SelectboxColumn("Decision", options=["", "Approved", "Modified", "Rejected"]),
                "override_units": st.column_config.NumberColumn("Override qty", min_value=0, step=1),
                "weeks_of_cover": st.column_config.NumberColumn("Weeks cover", format="%.1f"),
                "recommended_order_units": st.column_config.NumberColumn("Recommended", format="%d"),
            })
        changed = edited[(edited["decision"] != "") & (edited["decision"] != edit["decision"].where(edit["decision"] != "", "∅"))]
        if st.button(f"💾 Save {len(changed)} decision(s)", disabled=changed.empty, type="primary"):
            user, now = current_user(), datetime.now(timezone.utc).replace(tzinfo=None)
            with connection().cursor() as cur:
                for i, row in changed.iterrows():
                    src = recs.loc[i]
                    cur.execute(f"""INSERT INTO {CAT}.ml.reorder_overrides VALUES
                                    (:wh, :sku, :rec, :ovr, :dec, :cmt, :usr, :ts)""",
                                {"wh": src.warehouse_id, "sku": src.sku_id, "rec": float(src.recommended_order_units),
                                 "ovr": None if pd.isna(row.override_units) else float(row.override_units),
                                 "dec": row.decision, "cmt": row.comment or None, "usr": user, "ts": now})
            st.success(f"Saved {len(changed)} decision(s) to {CAT}.ml.reorder_overrides")
            st.rerun()

# ---------------------------------------------------------------- Ask Genie
with tab_genie:
    st.write("Ask about sales, festivals, promotions, margins, inventory or forecasts. Answers come from the **Ask SpiceRoute** Genie space; "
             "the generated SQL is shown so you can verify it.")
    examples = ["Which SKUs are at high risk of stockout at the Chennai DC?", "How did the 2024 chilli shortage affect gross margin?",
                "What was net revenue by zone in FY25-26, in crores?"]
    ex = st.pills("Examples", examples) if hasattr(st, "pills") else None
    question = st.chat_input("Ask SpiceRoute…") or ex
    if "genie_conv" not in st.session_state:
        st.session_state.genie_conv = None
    if question and GENIE_SPACE_ID:
        st.chat_message("user").write(question)
        with st.chat_message("assistant"), st.spinner("Genie is thinking…"):
            w = workspace()
            try:
                if st.session_state.genie_conv:
                    msg = w.genie.create_message_and_wait(GENIE_SPACE_ID, st.session_state.genie_conv, question)
                else:
                    msg = w.genie.start_conversation_and_wait(GENIE_SPACE_ID, question)
                st.session_state.genie_conv = msg.conversation_id
                for att in msg.attachments or []:
                    if att.text and att.text.content:
                        st.markdown(att.text.content)
                    if att.query:
                        if att.query.description:
                            st.markdown(att.query.description)
                        res = w.genie.get_message_attachment_query_result(GENIE_SPACE_ID, msg.conversation_id, msg.id, att.attachment_id)
                        sr = res.statement_response
                        cols = [c.name for c in sr.manifest.schema.columns]
                        st.dataframe(pd.DataFrame(sr.result.data_array or [], columns=cols), hide_index=True, use_container_width=True)
                        with st.expander("Generated SQL"):
                            st.code(att.query.query, language="sql")
            except Exception as e:  # surface API problems to the user rather than crashing the app
                st.error(f"Genie request failed: {e}")
    elif question:
        st.warning("GENIE_SPACE_ID is not configured for this app.")
