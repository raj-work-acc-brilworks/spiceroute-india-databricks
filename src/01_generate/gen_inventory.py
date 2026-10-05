"""Step 3/3 — Simulate daily warehouse inventory and purchase orders from generated demand.

Weekly (s, S) replenishment per warehouse x SKU. During the 2024 chilli crop failure, chilli POs are partially filled,
delayed and half are diverted to a low-grade alternate supplier -> stockouts in South/West + quality returns.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(sys.argv[0] if sys.argv and sys.argv[0] else __file__)))
try:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass

import zlib

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import spice_common as C

args = C.parse_args()
CAT, SEED = args.catalog, args.seed
RAW = C.raw_root(CAT)
SYN = f"{CAT}.synthetic"
spark = SparkSession.builder.getOrCreate()

sup = spark.table(f"{SYN}.suppliers").toPandas()
primary = sup[sup.supplier_tier == "Primary"]
SUP_BY_SPICE = primary.groupby("primary_spice_code").supplier_id.apply(list).to_dict()
LT_BY_SUP = sup.set_index("supplier_id").lead_time_days.to_dict()
REL_BY_SUP = sup.set_index("supplier_id").reliability_score.to_dict()
BLEND_SUPS = SUP_BY_SPICE.get("BLEND", [])
ALT_SUP = sup[sup.supplier_tier == "Alternate"].supplier_id.iloc[0]
SPICE_OF = spark.table(f"{SYN}.products").select("sku_id", "base_spice_code", "pack_size_g").toPandas().set_index("sku_id")
SHORT_S, SHORT_E = pd.Timestamp(C.CHILLI_SHORTAGE[0]), pd.Timestamp(C.CHILLI_SHORTAGE[1])
HIT_WH = {"WH-CHE", "WH-BLR", "WH-HYD", "WH-MUM", "WH-AMD"}
ALL_DAYS = pd.date_range(C.START_DATE, C.END_DATE, freq="D")

inv_schema = ("warehouse_id string, sku_id string, snapshot_date date, opening_units long, receipts_units long, "
              "demand_units long, shipped_units long, closing_units long, reorder_point_units long, safety_stock_units long")
po_schema = ("po_id string, po_date date, supplier_id string, warehouse_id string, sku_id string, ordered_units long, "
             "received_units long, expected_date date, received_date date, quality_grade string")


def simulate(pdf: pd.DataFrame):
    wh, sku = pdf.warehouse_id.iloc[0], pdf.sku_id.iloc[0]
    seed = zlib.crc32(f"{wh}|{sku}|{SEED}".encode())
    rng = np.random.default_rng(seed)
    dem = pdf.set_index("date").units.reindex(ALL_DAYS.date, fill_value=0).astype(float).values
    code = SPICE_OF.loc[sku, "base_spice_code"]
    cands = SUP_BY_SPICE.get(code) or BLEND_SUPS
    sup_id = cands[seed % len(cands)]
    lt = int(LT_BY_SUP[sup_id])
    rel = float(REL_BY_SUP[sup_id])
    first = dem[:28].mean() if dem[:28].sum() > 0 else max(dem.mean(), 0.1)
    on_hand = first * 45
    pipeline = []  # (arrival_idx, units, po_index)
    inv, pos = [], []
    for i, d in enumerate(ALL_DAYS):
        hist = dem[max(0, i - 28):i] if i > 0 else dem[:28]
        avg, sd = max(hist.mean(), 0.05), hist.std()
        ss = 1.65 * sd * np.sqrt(lt + 7)
        rop = avg * (lt + 7) + ss
        receipts = sum(u for a, u, _ in pipeline if a == i)
        pipeline = [p for p in pipeline if p[0] != i]
        opening = on_hand
        avail = opening + receipts
        shipped = min(dem[i], avail)
        on_hand = avail - shipped
        inv.append((wh, sku, d.date(), int(round(opening)), int(round(receipts)), int(dem[i]), int(round(shipped)),
                    int(round(on_hand)), int(round(rop)), int(round(ss))))
        if d.weekday() == 0:  # weekly review
            position = on_hand + sum(u for _, u, _ in pipeline)
            in_short = code in C.CHILLI_CODES and SHORT_S <= d <= SHORT_E
            # crop failure: supplier allocation refused most weeks for the hardest-hit warehouses
            allocation_refused = in_short and rng.random() < (0.65 if wh in HIT_WH else 0.30)
            if position < rop and not allocation_refused:
                qty = rop + avg * 21 - position
                s_id, grade, fill, delay = sup_id, "A" if rng.random() < 0.8 else "B", 1.0, 0
                if in_short:
                    fill = 0.35 if wh in HIT_WH else 0.70
                    delay = int(rng.integers(5, 15))
                    if rng.random() < 0.5:
                        s_id, grade = ALT_SUP, "C"
                if rng.random() > rel:
                    delay += int(rng.integers(2, 8))
                received = qty * fill * rng.uniform(0.95, 1.0)
                arr = i + lt + delay
                po_id = f"PO-{wh[3:]}-{sku[3:]}-{d.strftime('%y%m%d')}"
                pos.append((po_id, d.date(), s_id, wh, sku, int(round(qty)), int(round(received)),
                            (d + pd.Timedelta(days=lt)).date(),
                            (d + pd.Timedelta(days=lt + delay)).date() if arr < len(ALL_DAYS) else None, grade))
                if arr < len(ALL_DAYS):
                    pipeline.append((arr, received, len(pos) - 1))
                else:
                    pos[-1] = pos[-1][:6] + (None,) + pos[-1][7:]
    return pd.DataFrame(inv, columns=[c.split()[0] for c in inv_schema.split(", ")]), pd.DataFrame(pos, columns=[c.split()[0] for c in po_schema.split(", ")])


def sim_inv(pdf):
    return simulate(pdf)[0]


def sim_po(pdf):
    return simulate(pdf)[1]


demand = spark.table(f"{SYN}.daily_demand").repartition(64, "warehouse_id", "sku_id")
inv = demand.groupBy("warehouse_id", "sku_id").applyInPandas(sim_inv, inv_schema)
inv.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{SYN}.inventory_truth")
po = demand.groupBy("warehouse_id", "sku_id").applyInPandas(sim_po, po_schema)
po.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{SYN}.po_truth")

inv_t = spark.table(f"{SYN}.inventory_truth")
inv_t.withColumn("batch", F.date_format("snapshot_date", "yyyy-MM")).write.mode("overwrite").partitionBy("batch").parquet(f"{RAW}/inventory_snapshots")

# purchase orders: value from monthly unit cost; quantities also in kg
pm = spark.table(f"{SYN}.product_month")
prod = spark.table(f"{SYN}.products").select("sku_id", "pack_size_g")
po_t = (spark.table(f"{SYN}.po_truth").withColumn("month", F.trunc("po_date", "month"))
        .join(pm, ["sku_id", "month"]).join(prod, "sku_id")
        .withColumn("unit_cost_inr", F.round(F.col("unit_cost_inr") *
                                             F.when(F.col("quality_grade") == "C", 1.12).otherwise(1.0), 2))
        .withColumn("ordered_kg", F.round(F.col("ordered_units") * F.col("pack_size_g") / 1000, 2))
        .withColumn("po_value_inr", F.round(F.col("ordered_units") * F.col("unit_cost_inr"), 2))
        .select("po_id", "po_date", "supplier_id", "warehouse_id", "sku_id", "ordered_units", "received_units", "ordered_kg",
                "unit_cost_inr", "po_value_inr", "expected_date", "received_date", "quality_grade"))
po_t.repartition(4).write.mode("overwrite").parquet(f"{RAW}/purchase_orders")
print("inventory rows:", inv_t.count(), "POs:", spark.table(f"{SYN}.po_truth").count())
