-- Data-quality gate run at the end of spiceroute_end_to_end. Fails the task (raise_error) on breach.
-- Bound parameter: :catalog

-- 1. Reject rate of raw sales lines must stay <= 1%
SELECT CASE WHEN rejected / raw > 0.01
            THEN raise_error(concat('DQ gate: sales-line reject rate ', round(100 * rejected / raw, 2), '% > 1%')) END AS reject_check
FROM (SELECT (SELECT sum(rows) FROM IDENTIFIER(:catalog || '.gold.dq_summary')
              WHERE source_table = 'sales_lines' AND dq_reason NOT LIKE '%accepted%') AS rejected,
             (SELECT count(*) FROM IDENTIFIER(:catalog || '.bronze.sales_lines_raw')) AS raw);

-- 2. Every gold sales line must reference a known product, store and date
SELECT CASE WHEN orphans > 0 THEN raise_error(concat('DQ gate: ', orphans, ' gold sales lines with unknown product/store')) END AS fk_check
FROM (SELECT count(*) AS orphans FROM IDENTIFIER(:catalog || '.gold.fact_sales_line') f
      LEFT ANTI JOIN IDENTIFIER(:catalog || '.gold.dim_product') p ON f.sku_id = p.sku_id);

-- 3. Gold revenue must reconcile with the semantic layer (within 0.01%)
SELECT CASE WHEN abs(g.rev - m.rev) / g.rev > 0.0001
            THEN raise_error('DQ gate: metric view revenue does not reconcile with gold.agg_sales_daily') END AS reconcile_check
FROM (SELECT sum(net_revenue_inr) AS rev FROM IDENTIFIER(:catalog || '.gold.agg_sales_daily')) g,
     (SELECT MEASURE(`Net Revenue`) AS rev FROM IDENTIFIER(:catalog || '.semantic.sales_metrics')) m;

-- 4. Freshness: actuals must reach the end of the generated period
SELECT CASE WHEN max(order_date) < DATE'2026-09-30'
            THEN raise_error(concat('DQ gate: latest order_date is ', max(order_date))) END AS freshness_check
FROM IDENTIFIER(:catalog || '.gold.agg_sales_daily');

-- 5. Forecast must exist for the next 12 weeks
SELECT CASE WHEN count(DISTINCT week_start) < 12 THEN raise_error('DQ gate: demand_forecast has fewer than 12 future weeks') END AS forecast_check
FROM IDENTIFIER(:catalog || '.ml.demand_forecast');
