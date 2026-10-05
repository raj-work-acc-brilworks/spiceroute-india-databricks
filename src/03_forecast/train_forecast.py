"""Weekly demand forecast per SKU x zone — Prophet with Indian festival holidays, hierarchical reconciliation and
champion selection by rolling-origin backtest. Parallelised with applyInPandas.

Candidates (all evaluated on the same 3 x 12-week backtest folds, at SKU x zone x week):
  prophet_sku         Prophet fitted directly on each SKU x zone series
  topdown             Prophet fitted on base-spice x zone (smoother) split to SKUs by trailing 12-week mix share
  combined            average of prophet_sku and topdown
  <x>+snaive          0.7 * <x> + 0.3 * seasonal naive (same week last year)
The candidate with the lowest overall WAPE becomes the champion. 80% intervals are split-conformal: quantiles of
actual/forecast ratios per category x zone from the backtest (coverage checked on a held-out fold).

Outputs (schema <catalog>.ml):
  demand_forecast            12-week champion forecast + interval per SKU x zone (plus candidate columns)
  forecast_backtest          backtest predictions of the champion and candidates vs actuals
  forecast_accuracy          per-series WAPE / bias / interval coverage
  forecast_model_comparison  WAPE of every candidate at SKU x zone and base-spice x zone level
MLflow: one run per execution with all candidate metrics.
"""
import argparse
import json
import os
import tempfile
from datetime import datetime, timezone

import mlflow
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

p = argparse.ArgumentParser()
p.add_argument("--catalog", default="spiceroute")
p.add_argument("--horizon_weeks", type=int, default=12)
p.add_argument("--experiment", default="")
args, _ = p.parse_known_args()
CAT, H = args.catalog, args.horizon_weeks
spark = SparkSession.builder.getOrCreate()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.ml")

LAST_FULL_WEEK = "2026-09-21"   # actuals end Wed 2026-09-30; the week of 09-28 is partial
FOLDS = 3
SHARE_WEEKS = 12
SNAIVE_WEIGHT = 0.3
RUN_TS = datetime.now(timezone.utc).replace(tzinfo=None)

# ---------------------------------------------------------------- training data: complete weekly grid per series
products = spark.table(f"{CAT}.gold.dim_product").select("sku_id", "base_spice", "category")
weekly = spark.table(f"{CAT}.gold.agg_sales_weekly_sku_zone").where(f"week_start <= '{LAST_FULL_WEEK}'")
weeks = spark.sql(f"SELECT explode(sequence(DATE'2023-10-02', DATE'{LAST_FULL_WEEK}', INTERVAL 7 DAY)) AS week_start")
first_week = weekly.groupBy("sku_id", "zone").agg(F.min("week_start").alias("first_week"))
sku_grid = (first_week.crossJoin(weeks).where("week_start >= first_week")
            .join(weekly.select("sku_id", "zone", "week_start", "units", "promo_share"), ["sku_id", "zone", "week_start"], "left")
            .fillna({"units": 0, "promo_share": 0.0}).drop("first_week")
            .join(products, "sku_id"))
sku_grid.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{CAT}.ml._grid")
sku_grid = spark.table(f"{CAT}.ml._grid")

spice_grid = (sku_grid.groupBy("base_spice", "zone", "week_start")
              .agg(F.sum("units").alias("units"), F.avg("promo_share").alias("promo_share")))
both = (sku_grid.select(F.lit("sku").alias("level"), F.col("sku_id").alias("series_key"), "zone", "week_start", "units", "promo_share")
        .unionByName(spice_grid.select(F.lit("spice").alias("level"), F.col("base_spice").alias("series_key"), "zone", "week_start",
                                       "units", "promo_share")))

# festival holidays mapped to the (Monday) week they fall in; previous week also affected (pre-festival buying)
fest = (spark.table(f"{CAT}.silver.festivals")
        .select(F.col("festival_name").alias("holiday"), F.date_trunc("week", "festival_date").cast("date").alias("ds"))
        .toPandas())
fest["ds"] = pd.to_datetime(fest["ds"])
fest["lower_window"] = -7
fest["upper_window"] = 0
HOLIDAYS = fest[["holiday", "ds", "lower_window", "upper_window"]]

out_schema = ("level string, series_key string, zone string, kind string, cutoff date, week_start date, actual double, "
              "yhat double, snaive double, model string")


def forecast_series(pdf: pd.DataFrame) -> pd.DataFrame:
    import logging
    import numpy as np
    logging.getLogger("cmdstanpy").setLevel(logging.ERROR)
    logging.getLogger("prophet").setLevel(logging.ERROR)
    from prophet import Prophet

    level, key, zone = pdf.level.iloc[0], pdf.series_key.iloc[0], pdf.zone.iloc[0]
    df = pdf.sort_values("week_start").rename(columns={"week_start": "ds", "units": "y"})
    df["ds"] = pd.to_datetime(df["ds"])
    rows = []

    def future_promo(future_ds, train):
        last_year = train.set_index("ds").promo_share  # assume same promo intensity as the same week last year
        return [float(last_year.get(d - pd.Timedelta(weeks=52), 0.0)) for d in future_ds]

    def fit_predict(train, future_ds):
        if len(train) < 26 or train.y.sum() == 0:  # new / sparse product -> recent mean
            return np.full(len(future_ds), float(train.y.tail(8).mean()) if len(train) else 0.0), "recent_mean"
        mdl = Prophet(yearly_seasonality=len(train) >= 60, weekly_seasonality=False, daily_seasonality=False,
                      holidays=HOLIDAYS, seasonality_mode="multiplicative", uncertainty_samples=0,
                      changepoint_prior_scale=0.05, holidays_prior_scale=5.0)
        mdl.add_regressor("promo_share")
        mdl.fit(train[["ds", "y", "promo_share"]])
        fc = mdl.predict(pd.DataFrame({"ds": future_ds, "promo_share": future_promo(future_ds, train)}))
        return fc.yhat.clip(lower=0).values, "prophet"

    def snaive(train, future_ds):
        s = train.set_index("ds").y
        return np.array([float(s.get(d - pd.Timedelta(weeks=52), s.tail(4).mean())) for d in future_ds])

    n = len(df)
    for k in range(FOLDS, 0, -1):  # rolling-origin backtest
        cut = n - k * H
        if cut < 20:
            continue
        train, test = df.iloc[:cut], df.iloc[cut:cut + H]
        yhat, model = fit_predict(train, test.ds)
        sn = snaive(train, test.ds)
        cutoff = train.ds.iloc[-1].date()
        for i, r in enumerate(test.itertuples()):
            rows.append((level, key, zone, "backtest", cutoff, r.ds.date(), float(r.y), float(yhat[i]), float(sn[i]), model))
    future_ds = pd.date_range(df.ds.iloc[-1] + pd.Timedelta(weeks=1), periods=H + 1, freq="7D")  # +1 covers the partial week
    yhat, model = fit_predict(df, future_ds)
    sn = snaive(df, future_ds)
    cutoff = df.ds.iloc[-1].date()
    for i, d in enumerate(future_ds):
        rows.append((level, key, zone, "forecast", cutoff, d.date(), None, float(yhat[i]), float(sn[i]), model))
    return pd.DataFrame(rows, columns=[c.split()[0] for c in out_schema.split(", ")])


n_series = sku_grid.select("sku_id", "zone").distinct().count()
res = both.repartition(96, "level", "series_key", "zone").groupBy("level", "series_key", "zone").applyInPandas(forecast_series, out_schema)
res.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{CAT}.ml._forecast_raw")

# ---------------------------------------------------------------- reconcile: top-down shares + candidates
spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.ml._candidates AS
WITH cutoffs AS (SELECT DISTINCT cutoff FROM {CAT}.ml._forecast_raw),
share AS (   -- SKU share of its base spice within the zone over the 12 weeks up to each cutoff
  SELECT c.cutoff, g.sku_id, g.zone, g.base_spice,
         coalesce(sum(g.units) / nullif(sum(sum(g.units)) OVER (PARTITION BY c.cutoff, g.base_spice, g.zone), 0), 0) AS share
  FROM cutoffs c JOIN {CAT}.ml._grid g
    ON g.week_start > date_sub(c.cutoff, {7 * SHARE_WEEKS}) AND g.week_start <= c.cutoff
  GROUP BY c.cutoff, g.sku_id, g.zone, g.base_spice),
sku AS (SELECT * FROM {CAT}.ml._forecast_raw WHERE level = 'sku'),
spice AS (SELECT * FROM {CAT}.ml._forecast_raw WHERE level = 'spice')
SELECT s.series_key AS sku_id, s.zone, p.category, p.base_spice, s.kind, s.cutoff, s.week_start, s.actual,
       s.yhat AS prophet_sku, coalesce(sp.yhat * sh.share, s.yhat) AS topdown, s.snaive AS seasonal_naive, s.model AS sku_model
FROM sku s
JOIN {CAT}.gold.dim_product p ON s.series_key = p.sku_id
LEFT JOIN share sh ON sh.sku_id = s.series_key AND sh.zone = s.zone AND sh.cutoff = s.cutoff
LEFT JOIN spice sp ON sp.series_key = p.base_spice AND sp.zone = s.zone AND sp.cutoff = s.cutoff AND sp.week_start = s.week_start
""")
cand = spark.table(f"{CAT}.ml._candidates")
W = SNAIVE_WEIGHT
CANDIDATES = {
    "prophet_sku": "prophet_sku",
    "topdown": "topdown",
    "combined": "(prophet_sku + topdown) / 2",
    "prophet_sku+snaive": f"{1 - W} * prophet_sku + {W} * seasonal_naive",
    "topdown+snaive": f"{1 - W} * topdown + {W} * seasonal_naive",
    "combined+snaive": f"{1 - W} * (prophet_sku + topdown) / 2 + {W} * seasonal_naive",
    "seasonal_naive": "seasonal_naive",
}
cand = cand.select("*", *[F.expr(e).alias(f"c_{i}") for i, e in enumerate(CANDIDATES.values())])
bt = cand.where("kind = 'backtest'")
names = list(CANDIDATES)

sku_wape = bt.agg(*[(F.sum(F.abs(F.col("actual") - F.col(f"c_{i}"))) / F.sum("actual")).alias(n) for i, n in enumerate(names)]).first().asDict()
spice_lvl = bt.groupBy("base_spice", "zone", "cutoff", "week_start").agg(F.sum("actual").alias("actual"),
                                                                         *[F.sum(f"c_{i}").alias(f"c_{i}") for i in range(len(names))])
spice_wape = spice_lvl.agg(*[(F.sum(F.abs(F.col("actual") - F.col(f"c_{i}"))) / F.sum("actual")).alias(n) for i, n in enumerate(names)]).first().asDict()
champion = min((n for n in names if n != "seasonal_naive"), key=lambda n: sku_wape[n])
ci = names.index(champion)
print("SKU x zone WAPE:", json.dumps({k: round(v, 4) for k, v in sku_wape.items()}))
print("spice x zone WAPE:", json.dumps({k: round(v, 4) for k, v in spice_wape.items()}))
print("champion:", champion)

spark.createDataFrame([(lvl, n, float(w[n]), n == champion) for lvl, w in (("sku x zone x week", sku_wape), ("base spice x zone x week", spice_wape))
                       for n in names], "level string, candidate string, wape double, is_champion boolean") \
    .withColumn("evaluated_at", F.lit(RUN_TS)) \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{CAT}.ml.forecast_model_comparison")

# ---------------------------------------------------------------- split-conformal 80% intervals per category x zone
cand = cand.withColumn("fc", F.col(f"c_{ci}"))
bt = cand.where("kind = 'backtest' AND fc > 0.5").withColumn("ratio", F.col("actual") / F.col("fc"))
cutoffs = sorted(r.cutoff for r in bt.select("cutoff").distinct().collect())
q_all = bt.groupBy("category", "zone").agg(F.percentile_approx("ratio", 0.1).alias("q_lo"), F.percentile_approx("ratio", 0.9).alias("q_hi"))
q_cal = (bt.where(F.col("cutoff") < F.lit(cutoffs[-1])).groupBy("category", "zone")
         .agg(F.percentile_approx("ratio", 0.1).alias("q_lo"), F.percentile_approx("ratio", 0.9).alias("q_hi")))
holdout_cov = (bt.where(F.col("cutoff") == F.lit(cutoffs[-1])).join(q_cal, ["category", "zone"])
               .agg(F.avg(((F.col("actual") >= F.col("fc") * F.col("q_lo")) & (F.col("actual") <= F.col("fc") * F.col("q_hi"))).cast("int"))).first()[0])
final = (cand.join(q_all, ["category", "zone"], "left").fillna({"q_lo": 0.6, "q_hi": 1.4})
         .withColumn("lower", F.col("fc") * F.col("q_lo")).withColumn("upper", F.col("fc") * F.col("q_hi")))

# ---------------------------------------------------------------- outputs
(final.where("kind = 'forecast'")
 .select("sku_id", "zone", "week_start", F.round("fc", 1).alias("forecast_units"), F.round("lower", 1).alias("forecast_lower"),
         F.round("upper", 1).alias("forecast_upper"), F.round("seasonal_naive", 1).alias("seasonal_naive_units"),
         F.round("prophet_sku", 1).alias("prophet_sku_units"), F.round("topdown", 1).alias("topdown_units"),
         F.lit(champion).alias("model"), F.col("cutoff").alias("trained_through_week"), F.lit(RUN_TS).alias("forecast_created_at"))
 .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{CAT}.ml.demand_forecast"))

(final.where("kind = 'backtest'")
 .select("sku_id", "zone", "cutoff", "week_start", "actual", F.round("fc", 1).alias("forecast_units"),
         F.round("lower", 1).alias("forecast_lower"), F.round("upper", 1).alias("forecast_upper"),
         F.round("seasonal_naive", 1).alias("seasonal_naive_units"), F.round("prophet_sku", 1).alias("prophet_sku_units"),
         F.round("topdown", 1).alias("topdown_units"), F.lit(champion).alias("model"))
 .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{CAT}.ml.forecast_backtest"))

spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.ml.forecast_accuracy AS
SELECT b.sku_id, b.zone, p.category, p.base_spice, any_value(b.model) AS model, count(*) AS backtest_weeks, sum(actual) AS actual_units,
       round(sum(abs(actual - forecast_units)) / nullif(sum(actual), 0), 4) AS wape_model,
       round(sum(abs(actual - prophet_sku_units)) / nullif(sum(actual), 0), 4) AS wape_prophet_sku,
       round(sum(abs(actual - seasonal_naive_units)) / nullif(sum(actual), 0), 4) AS wape_seasonal_naive,
       round(sum(forecast_units - actual) / nullif(sum(actual), 0), 4) AS bias_model,
       round(avg(CASE WHEN actual BETWEEN forecast_lower AND forecast_upper THEN 1 ELSE 0 END), 3) AS interval_coverage
FROM {CAT}.ml.forecast_backtest b JOIN {CAT}.gold.dim_product p USING (sku_id)
GROUP BY b.sku_id, b.zone, p.category, p.base_spice""")
for t in ("_forecast_raw", "_candidates", "_grid"):
    spark.sql(f"DROP TABLE IF EXISTS {CAT}.ml.{t}")

beat = spark.sql(f"""SELECT avg(CASE WHEN wape_model < wape_seasonal_naive THEN 1 ELSE 0 END) b,
                            sum(forecast_units - actual) / sum(actual) bias
                     FROM {CAT}.ml.forecast_accuracy a JOIN (SELECT sku_id, zone, sum(forecast_units) forecast_units, sum(actual) actual
                       FROM {CAT}.ml.forecast_backtest GROUP BY ALL) USING (sku_id, zone) WHERE a.actual_units > 0""").first()
metrics = {"wape_champion_sku_zone": float(sku_wape[champion]), "wape_champion_spice_zone": float(spice_wape[champion]),
           "wape_prophet_sku": float(sku_wape["prophet_sku"]), "wape_seasonal_naive_sku_zone": float(sku_wape["seasonal_naive"]),
           "share_series_beating_snaive": float(beat.b), "bias_champion": float(beat.bias),
           "interval_coverage_80_holdout": float(holdout_cov), "n_series": float(n_series)}
metrics.update({f"wape_sku__{n.replace('+', '_plus_')}": float(v) for n, v in sku_wape.items()})
print(json.dumps(metrics, indent=2))

# ---------------------------------------------------------------- MLflow tracking
try:
    me = spark.sql("SELECT current_user()").first()[0]
    mlflow.set_experiment(args.experiment or f"/Users/{me}/spiceroute/demand_forecast")
    with mlflow.start_run(run_name=f"forecast_{champion}_{RUN_TS:%Y%m%d_%H%M}"):
        mlflow.log_params({"champion": champion, "grain": "week x sku x zone", "horizon_weeks": H, "folds": FOLDS,
                           "hierarchy": "base_spice x zone -> sku (trailing 12w share)", "snaive_weight": W,
                           "intervals": "split-conformal per category x zone", "holidays": "Indian festival calendar",
                           "regressors": "promo_share", "trained_through": LAST_FULL_WEEK})
        mlflow.log_metrics(metrics)
        with tempfile.TemporaryDirectory() as d:
            for t in ("forecast_accuracy", "forecast_model_comparison"):
                path = os.path.join(d, f"{t}.csv")
                spark.table(f"{CAT}.ml.{t}").toPandas().to_csv(path, index=False)
                mlflow.log_artifact(path)
except Exception as e:  # tracking must not fail the job
    print("MLflow logging skipped:", e)
