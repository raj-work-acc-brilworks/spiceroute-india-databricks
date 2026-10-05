"""Weekly demand forecast per SKU x zone with Prophet (Indian festival holidays), parallelised with applyInPandas.

Outputs (schema <catalog>.ml):
  demand_forecast     12-week-ahead forecast (yhat, 80% interval) per SKU x zone
  forecast_backtest   rolling-origin backtest predictions (3 folds x 12 weeks) vs actuals and seasonal-naive
  forecast_accuracy   WAPE / bias per series, Prophet vs seasonal-naive
MLflow: one run per execution with aggregate metrics and the accuracy table as an artifact.
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
RUN_TS = datetime.now(timezone.utc).replace(tzinfo=None)

# ---------------------------------------------------------------- training data: complete weekly grid per series
weekly = spark.table(f"{CAT}.gold.agg_sales_weekly_sku_zone").where(f"week_start <= '{LAST_FULL_WEEK}'")
weeks = spark.sql(f"SELECT explode(sequence(DATE'2023-10-02', DATE'{LAST_FULL_WEEK}', INTERVAL 7 DAY)) AS week_start")
first_week = weekly.groupBy("sku_id", "zone").agg(F.min("week_start").alias("first_week"))
grid = (first_week.crossJoin(weeks).where("week_start >= first_week")
        .join(weekly.select("sku_id", "zone", "week_start", "units", "promo_share"), ["sku_id", "zone", "week_start"], "left")
        .fillna({"units": 0, "promo_share": 0.0}).drop("first_week"))

# festival holidays mapped to the (Monday) week they fall in; previous week also affected (pre-festival buying)
fest = (spark.table(f"{CAT}.silver.festivals")
        .select(F.col("festival_name").alias("holiday"), F.date_trunc("week", "festival_date").cast("date").alias("ds"))
        .toPandas())
fest["ds"] = pd.to_datetime(fest["ds"])
fest["lower_window"] = -7
fest["upper_window"] = 0
HOLIDAYS = fest[["holiday", "ds", "lower_window", "upper_window"]]

out_schema = ("sku_id string, zone string, kind string, cutoff date, week_start date, actual double, "
              "yhat double, yhat_lower double, yhat_upper double, snaive double, model string")


def forecast_series(pdf: pd.DataFrame) -> pd.DataFrame:
    import logging
    import numpy as np
    logging.getLogger("cmdstanpy").setLevel(logging.ERROR)
    logging.getLogger("prophet").setLevel(logging.ERROR)
    from prophet import Prophet

    sku, zone = pdf.sku_id.iloc[0], pdf.zone.iloc[0]
    df = pdf.sort_values("week_start").rename(columns={"week_start": "ds", "units": "y"})
    df["ds"] = pd.to_datetime(df["ds"])
    rows = []

    def fit_predict(train, future_ds):
        if len(train) < 26 or train.y.sum() == 0:  # new / sparse product -> recent mean
            m = float(train.y.tail(8).mean()) if len(train) else 0.0
            sd = float(train.y.tail(8).std() or 0.0) if len(train) > 1 else 0.0
            return np.full(len(future_ds), m), np.full(len(future_ds), max(0, m - 1.28 * sd)), np.full(len(future_ds), m + 1.28 * sd), "recent_mean"
        mdl = Prophet(yearly_seasonality=len(train) >= 60, weekly_seasonality=False, daily_seasonality=False,
                      holidays=HOLIDAYS, seasonality_mode="multiplicative", interval_width=0.8, uncertainty_samples=200,
                      changepoint_prior_scale=0.05, holidays_prior_scale=5.0)
        mdl.add_regressor("promo_share")
        mdl.fit(train[["ds", "y", "promo_share"]])
        fut = pd.DataFrame({"ds": future_ds, "promo_share": future_promo(future_ds, train)})
        fc = mdl.predict(fut)
        return fc.yhat.clip(lower=0).values, fc.yhat_lower.clip(lower=0).values, fc.yhat_upper.clip(lower=0).values, "prophet"

    def future_promo(future_ds, train):
        # assume same promo intensity as the same week last year
        last_year = train.set_index("ds").promo_share
        return [float(last_year.get(d - pd.Timedelta(weeks=52), 0.0)) for d in future_ds]

    def snaive(train, future_ds):
        s = train.set_index("ds").y
        return np.array([float(s.get(d - pd.Timedelta(weeks=52), s.tail(4).mean())) for d in future_ds])

    n = len(df)
    # rolling-origin backtest
    for k in range(FOLDS, 0, -1):
        cut = n - k * H
        if cut < 20:
            continue
        train, test = df.iloc[:cut], df.iloc[cut:cut + H]
        yhat, lo, hi, model = fit_predict(train, test.ds)
        sn = snaive(train, test.ds)
        cutoff = train.ds.iloc[-1].date()
        for i, r in enumerate(test.itertuples()):
            rows.append((sku, zone, "backtest", cutoff, r.ds.date(), float(r.y), float(yhat[i]), float(lo[i]), float(hi[i]), float(sn[i]), model))
    # final fit + future
    future_ds = pd.date_range(df.ds.iloc[-1] + pd.Timedelta(weeks=1), periods=H + 1, freq="7D")  # +1 covers the partial week
    yhat, lo, hi, model = fit_predict(df, future_ds)
    sn = snaive(df, future_ds)
    cutoff = df.ds.iloc[-1].date()
    for i, d in enumerate(future_ds):
        rows.append((sku, zone, "forecast", cutoff, d.date(), None, float(yhat[i]), float(lo[i]), float(hi[i]), float(sn[i]), model))
    return pd.DataFrame(rows, columns=[c.split()[0] for c in out_schema.split(", ")])


n_series = grid.select("sku_id", "zone").distinct().count()
res = grid.repartition(64, "sku_id", "zone").groupBy("sku_id", "zone").applyInPandas(forecast_series, out_schema)
res.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{CAT}.ml._forecast_raw")
res = spark.table(f"{CAT}.ml._forecast_raw")

# ---------------------------------------------------------------- outputs
(res.where("kind = 'forecast'")
 .select("sku_id", "zone", "week_start", F.round("yhat", 1).alias("forecast_units"), F.round("yhat_lower", 1).alias("forecast_lower"),
         F.round("yhat_upper", 1).alias("forecast_upper"), F.round("snaive", 1).alias("seasonal_naive_units"), "model",
         F.col("cutoff").alias("trained_through_week"), F.lit(RUN_TS).alias("forecast_created_at"))
 .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{CAT}.ml.demand_forecast"))

(res.where("kind = 'backtest'")
 .select("sku_id", "zone", "cutoff", "week_start", "actual", F.round("yhat", 1).alias("forecast_units"),
         F.round("yhat_lower", 1).alias("forecast_lower"), F.round("yhat_upper", 1).alias("forecast_upper"),
         F.round("snaive", 1).alias("seasonal_naive_units"), "model")
 .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{CAT}.ml.forecast_backtest"))

acc = spark.sql(f"""
  SELECT b.sku_id, b.zone, p.category, p.base_spice, any_value(b.model) AS model, count(*) AS backtest_weeks,
         sum(actual) AS actual_units,
         round(sum(abs(actual - forecast_units)) / nullif(sum(actual), 0), 4) AS wape_prophet,
         round(sum(abs(actual - seasonal_naive_units)) / nullif(sum(actual), 0), 4) AS wape_seasonal_naive,
         round(sum(forecast_units - actual) / nullif(sum(actual), 0), 4) AS bias_prophet,
         round(avg(CASE WHEN actual BETWEEN forecast_lower AND forecast_upper THEN 1 ELSE 0 END), 3) AS interval_coverage
  FROM {CAT}.ml.forecast_backtest b JOIN {CAT}.gold.dim_product p USING (sku_id)
  GROUP BY b.sku_id, b.zone, p.category, p.base_spice""")
acc.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{CAT}.ml.forecast_accuracy")
spark.sql(f"DROP TABLE IF EXISTS {CAT}.ml._forecast_raw")

m = spark.sql(f"""
  SELECT sum(abs(actual - forecast_units)) / sum(actual) AS wape_prophet,
         sum(abs(actual - seasonal_naive_units)) / sum(actual) AS wape_snaive,
         sum(forecast_units - actual) / sum(actual) AS bias
  FROM {CAT}.ml.forecast_backtest""").first()
beat = spark.sql(f"SELECT avg(CASE WHEN wape_prophet < wape_seasonal_naive THEN 1 ELSE 0 END) b, avg(interval_coverage) c FROM {CAT}.ml.forecast_accuracy WHERE actual_units > 0").first()
metrics = {"wape_prophet": float(m.wape_prophet), "wape_seasonal_naive": float(m.wape_snaive), "bias_prophet": float(m.bias),
           "share_series_beating_snaive": float(beat.b), "interval_coverage_80": float(beat.c), "n_series": float(n_series)}
print(json.dumps(metrics, indent=2))

# ---------------------------------------------------------------- MLflow tracking
try:
    me = spark.sql("SELECT current_user()").first()[0]
    exp = args.experiment or f"/Users/{me}/spiceroute/demand_forecast"
    mlflow.set_experiment(exp)
    with mlflow.start_run(run_name=f"prophet_weekly_{RUN_TS:%Y%m%d_%H%M}"):
        mlflow.log_params({"model": "prophet", "grain": "week x sku x zone", "horizon_weeks": H, "folds": FOLDS,
                           "seasonality_mode": "multiplicative", "holidays": "Indian festival calendar",
                           "regressors": "promo_share", "trained_through": LAST_FULL_WEEK})
        mlflow.log_metrics(metrics)
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "forecast_accuracy.csv")
            spark.table(f"{CAT}.ml.forecast_accuracy").toPandas().to_csv(path, index=False)
            mlflow.log_artifact(path)
except Exception as e:  # tracking must not fail the job
    print("MLflow logging skipped:", e)
