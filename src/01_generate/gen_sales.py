"""Step 2/3 — Generate orders, order lines (~25M) and returns.

Demand per store-day = base x channel trend x weekday x salary-week x festival x wedding x promo lift x noise,
scaled so total lines ~= target_lines. Product mix per line comes from the zone/week/mix-group bucket table.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(sys.argv[0] if sys.argv and sys.argv[0] else __file__)))
try:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

import spice_common as C

args = C.parse_args()
CAT, SEED = args.catalog, args.seed
RAW = C.raw_root(CAT)
SYN = f"{CAT}.synthetic"
NB = 4000
spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------- calendar
dates = (spark.sql(f"SELECT explode(sequence(DATE'{C.START_DATE}', DATE'{C.END_DATE}', INTERVAL 1 DAY)) AS date")
         .withColumn("dow", F.dayofweek("date"))  # 1=Sun..7=Sat
         .withColumn("dom", F.dayofmonth("date"))
         .withColumn("week_start", F.date_trunc("week", "date").cast("date"))
         .withColumn("month", F.trunc("date", "month"))
         .withColumn("yrs", F.datediff("date", F.lit(str(C.START_DATE)).cast("date")) / 365.0))

ch_rows = [(c[0], c[2], float(c[4] if c[0] != "QC" else 0.20), float(c[3])) for c in C.CHANNELS]
channels = spark.createDataFrame(ch_rows, ["channel_code", "channel_group", "growth", "partner_margin"])
stores = spark.table(f"{SYN}.stores").join(channels, "channel_code")

B2B = ["GT", "MT", "HRC"]  # sell-in pattern (weekday heavy, leads festivals by ~10 days)
is_b2b = F.col("channel_code").isin(B2B)

sd = (stores.crossJoin(dates)
      .where((F.col("date") >= F.col("opened_date").cast("date")) &
             (F.col("closed_date").isNull() | (F.col("date") < F.col("closed_date").cast("date"))))
      .withColumn("fest_join_date", F.when(is_b2b, F.date_add("date", 10)).otherwise(F.col("date"))))

dzm = spark.table(f"{SYN}.day_zone_mult")
sd = (sd.join(dzm.select(F.col("date").alias("fest_join_date"), "zone", "festival_mult"), ["fest_join_date", "zone"], "left")
      .join(dzm.select("date", "zone", "is_wedding_season"), ["date", "zone"], "left")
      .fillna({"festival_mult": 1.0, "is_wedding_season": False}))
promo_day = spark.table(f"{SYN}.promo_day")
sd = sd.join(promo_day, ["channel_code", "date"], "left")

weekday_consumer = F.element_at(F.array(*[F.lit(x) for x in [1.25, 0.92, 0.90, 0.93, 0.95, 1.02, 1.18]]), F.col("dow"))
weekday_b2b = F.element_at(F.array(*[F.lit(x) for x in [0.15, 1.15, 1.10, 1.10, 1.05, 1.05, 0.85]]), F.col("dow"))
sd = sd.withColumn(
    "lam",
    F.col("base_lambda")
    * F.pow(1 + F.col("growth"), F.col("yrs"))
    * F.when(is_b2b, weekday_b2b).otherwise(weekday_consumer)
    * F.when(~is_b2b & (F.col("dom") <= 7), 1.08).otherwise(1.0)
    * F.col("festival_mult")
    * F.when((F.col("channel_code") == "HRC") & F.col("is_wedding_season"), 1.45).otherwise(1.0)
    * F.when(F.col("promo_id").isNull(), 1.0)
       .when(F.col("category_scope") == "All", 1 + 1.3 * F.col("discount_pct"))
       .otherwise(1 + 0.4 * F.col("discount_pct"))
    * F.exp(F.randn(SEED) * 0.15))

MEAN_LINES = {"Distributor": 6.0, "Supermarket": 5.0, "Own Store": 2.4, "HoReCa Account": 4.0, "Dark Store": 2.0,
              "Marketplace FC": 2.2, "D2C Web": 3.4}
mean_lines = F.create_map(*[x for k, v in MEAN_LINES.items() for x in (F.lit(k), F.lit(v))])
expected = sd.agg(F.sum(F.col("lam") * mean_lines[F.col("store_type")])).first()[0]
scale = args.target_lines * 1.03 / expected  # +3% offsets duplicate-SKU removal
print(f"expected lines (unscaled) {expected:,.0f}; scale factor {scale:.3f}")

sd = (sd.withColumn("lam", F.col("lam") * scale)
      .withColumn("n_orders", F.when(F.col("lam") < 5, F.floor(F.col("lam") + F.rand(SEED + 1)))
                  .otherwise(F.greatest(F.lit(0), F.round(F.col("lam") + F.sqrt("lam") * F.randn(SEED + 2)))).cast("int"))
      .where("n_orders > 0"))

# ---------------------------------------------------------------- orders
ci = spark.table(f"{SYN}.customer_idx")
base_cnt = ci.where(F.col("signup_date") < F.lit(str(C.START_DATE)).cast("date")).groupBy("zone").agg(F.count("*").alias("base"))
new_cnt = ci.groupBy("zone", F.col("signup_date").alias("date")).agg(F.count("*").alias("new"))
zone_day_cnt = (dates.select("date").crossJoin(spark.createDataFrame([(z,) for z in C.ZONES], ["zone"]))
                .join(new_cnt, ["zone", "date"], "left").join(base_cnt, "zone", "left").fillna({"new": 0, "base": 0})
                .withColumn("cust_avail", F.col("base") + F.sum("new").over(
                    Window.partitionBy("zone").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)))
                .select("date", "zone", "cust_avail"))

consumer_hours = [7, 8, 9, 10, 10, 11, 11, 11, 12, 12, 12, 13, 13, 14, 15, 16, 17, 18, 18, 19, 19, 19, 20, 20, 20, 21, 21, 22, 23]
b2b_hours = [9, 9, 10, 10, 10, 11, 11, 11, 12, 12, 13, 14, 14, 15, 15, 16, 16, 17, 18]


def pick(arr, seed):
    return F.element_at(F.array(*[F.lit(x) for x in arr]), (F.floor(F.rand(seed) * len(arr)) + 1).cast("int"))


orders = (sd.select("date", "week_start", "month", "store_id", "store_type", "channel_code", "channel_group", "zone", "city",
                    "servicing_warehouse_id", "partner_margin", "promo_id", "promo_code", "discount_pct", "category_scope",
                    F.explode(F.sequence(F.lit(1), F.col("n_orders"))).alias("seq"))
          .withColumn("order_id", F.concat(F.lit("SO"), F.date_format("date", "yyMMdd"), F.substring("store_id", 4, 5),
                                           F.lpad(F.col("seq").cast("string"), 4, "0")))
          .withColumn("hour", F.when(is_b2b, pick(b2b_hours, SEED + 3)).otherwise(pick(consumer_hours, SEED + 4)))
          .withColumn("order_ts", F.to_timestamp(F.concat_ws(" ", F.col("date").cast("string"),
                                                             F.format_string("%02d:%02d:%02d", F.col("hour"),
                                                                             (F.rand(SEED + 5) * 60).cast("int"),
                                                                             (F.rand(SEED + 6) * 60).cast("int")))))
          .withColumn("needs_customer", F.col("channel_code").isin("QC", "MKT", "D2C") |
                      ((F.col("channel_code") == "OWN") & (F.rand(SEED + 7) < 0.70))))

orders = (orders.join(zone_day_cnt, ["date", "zone"], "left")
          .withColumn("zidx", F.when(F.col("needs_customer"),
                                     F.floor(F.col("cust_avail") * F.pow(F.rand(SEED + 8), F.lit(2.2))).cast("int")))
          .join(spark.table(f"{SYN}.customer_idx").select("zone", "zidx", "customer_id", F.col("city").alias("customer_city")),
                ["zone", "zidx"], "left"))

r = F.rand(SEED + 9)
pay = (F.when(F.col("channel_code").isin("GT", "MT", "HRC"), F.when(r < 0.85, "Credit").otherwise("NEFT/RTGS"))
       .when(F.col("channel_code") == "OWN", F.when(r < 0.55, "UPI").when(r < 0.80, "Card").otherwise("Cash"))
       .when(F.col("channel_code") == "QC", F.when(r < 0.75, "UPI").when(r < 0.95, "Card").otherwise("COD"))
       .when(F.col("channel_code") == "MKT", F.when(r < 0.45, "UPI").when(r < 0.70, "Card").otherwise("COD"))
       .otherwise(F.when(r < 0.50, "UPI").when(r < 0.80, "Card").otherwise("COD")))
orders = (orders.withColumn("payment_mode", pay)
          .withColumn("order_status", F.when(F.rand(SEED + 10) < F.when(F.col("channel_group") == "Online", 0.015).otherwise(0.003),
                                             "Cancelled").otherwise("Delivered"))
          .withColumn("delivery_city", F.when(F.col("channel_group") == "Online", F.col("customer_city")).otherwise(F.col("city")))
          .withColumn("batch", F.date_format("date", "yyyy-MM")))
orders.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{SYN}.orders_truth")
orders = spark.table(f"{SYN}.orders_truth")

raw_orders = (orders.select("order_id", "order_ts",
                            F.when(F.rand(SEED + 11) < 0.003, F.lit(None)).otherwise(F.col("store_id")).alias("store_id"),
                            "channel_code", "customer_id", "payment_mode", "order_status", "delivery_city", "batch"))
raw_orders.write.mode("overwrite").partitionBy("batch").parquet(f"{RAW}/orders")

# ---------------------------------------------------------------- lines
mean_minus_1 = mean_lines[F.col("store_type")] - 0.5  # E[floor(Exp(mu))] ~= mu - 0.5
cap = F.when(F.col("channel_code").isin("GT", "MT"), 14).otherwise(8)
lines = (orders.where("order_status IS NOT NULL")
         .withColumn("k", (1 + F.least(cap, F.floor(-F.log(F.rand(SEED + 12)) * mean_minus_1))).cast("int"))
         .select("order_id", "date", "week_start", "month", "zone", "store_type", "channel_code", "partner_margin", "promo_code",
                 "discount_pct", "category_scope", "servicing_warehouse_id", "order_status", "batch",
                 F.explode(F.sequence(F.lit(1), F.col("k"))).alias("line_no"))
         .withColumn("mix_group", F.when(F.col("channel_code").isin("GT", "HRC"), "bulk").otherwise("consumer"))
         .withColumn("bucket", F.floor(F.rand(SEED + 13) * NB).cast("int"))
         .join(spark.table(f"{SYN}.product_mix_bucket"), ["zone", "week_start", "mix_group", "bucket"])
         .dropDuplicates(["order_id", "sku_id"]))

prod = spark.table(f"{SYN}.products").select("sku_id", "category", "pack_size_g", "base_spice_code")
pm = spark.table(f"{SYN}.product_month").select("sku_id", "month", "mrp_inr", "unit_cost_inr")
lines = lines.join(F.broadcast(prod), "sku_id").join(F.broadcast(pm), ["sku_id", "month"])

case_units = (F.when(F.col("pack_size_g") <= 25, 96).when(F.col("pack_size_g") <= 100, 48)
              .when(F.col("pack_size_g") <= 200, 24).when(F.col("pack_size_g") <= 500, 12).otherwise(6))
rq = F.rand(SEED + 14)
qty = (F.when(F.col("store_type") == "Distributor", case_units * F.greatest(F.lit(1), F.round(F.exp(F.randn(SEED + 15) * 0.6 + 0.8))))
       .when(F.col("store_type") == "Supermarket", F.greatest(F.lit(6), F.round(case_units / 2 * F.exp(F.randn(SEED + 16) * 0.5))))
       .when(F.col("store_type") == "HoReCa Account", 1 + F.floor(F.exp(F.randn(SEED + 17) * 0.7 + 1.9)))
       .when(F.col("store_type").isin("D2C Web", "Marketplace FC"), F.when(rq < 0.55, 1).when(rq < 0.85, 2).when(rq < 0.95, 3).otherwise(5))
       .otherwise(F.when(rq < 0.72, 1).when(rq < 0.93, 2).when(rq < 0.98, 3).otherwise(5)))
promo_applies = (F.col("promo_code").isNotNull() &
                 ((F.col("category_scope") == "All") | (F.col("category_scope") == F.col("category"))) &
                 (F.col("channel_code").isin("GT", "HRC") | (F.rand(SEED + 18) < 0.85)))
lines = (lines.withColumn("quantity", qty.cast("int"))
         .withColumn("unit_price_inr", F.round(F.col("mrp_inr") * (1 - F.col("partner_margin")), 2))
         .withColumn("line_promo_code", F.when(promo_applies, F.col("promo_code")))
         .withColumn("line_discount_pct", F.when(promo_applies, F.col("discount_pct")).otherwise(F.lit(0.0)))
         .withColumn("line_amount_inr", F.round(F.col("quantity") * F.col("unit_price_inr") * (1 - F.col("line_discount_pct")), 2))
         .withColumn("line_id", F.concat_ws("-", "order_id", F.lpad(F.col("line_no").cast("string"), 2, "0")))
         # ~1% of lines arrive late (land in next month's batch folder)
         .withColumn("batch", F.when(F.rand(SEED + 19) < 0.01, F.date_format(F.add_months("date", 1), "yyyy-MM")).otherwise(F.col("batch"))))
lines.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{SYN}.lines_truth")
lines = spark.table(f"{SYN}.lines_truth")

raw_lines = lines.select("line_id", "order_id", "line_no", F.col("date").alias("order_date"), "sku_id", "quantity",
                         F.col("mrp_inr").alias("unit_mrp_inr"), "unit_price_inr", F.col("line_promo_code").alias("promo_code"),
                         F.col("line_discount_pct").alias("discount_pct"), "line_amount_inr", "batch")
# data quality issues for the silver layer to catch
rd = F.rand(SEED + 20)
raw_lines = (raw_lines
             .withColumn("quantity", F.when(rd < 0.0008, -F.col("quantity")).otherwise(F.col("quantity")))
             .withColumn("sku_id", F.when((rd >= 0.0008) & (rd < 0.0011), F.lit("SR-UNK-0000")).otherwise(F.col("sku_id")))
             .withColumn("unit_price_inr", F.when((rd >= 0.0011) & (rd < 0.0013), F.lit(None).cast("double")).otherwise(F.col("unit_price_inr"))))
dups = raw_lines.where(F.rand(SEED + 21) < 0.005)
raw_lines.unionByName(dups).repartition(F.col("batch")).write.mode("overwrite").partitionBy("batch").parquet(f"{RAW}/sales_lines")

# ---------------------------------------------------------------- returns
sh_s, sh_e = C.CHILLI_SHORTAGE
is_chilli_short = (F.col("base_spice_code").isin(C.CHILLI_CODES) & (F.col("date") >= F.lit(sh_s).cast("date")) &
                   (F.col("date") <= F.lit("2024-12-31").cast("date")))
p_ret = (F.when(F.col("channel_code").isin("GT", "MT", "HRC"), 0.006).otherwise(0.018)
         + F.when(is_chilli_short, 0.05).otherwise(0.0))
rr = F.rand(SEED + 23)
reason = (F.when(is_chilli_short & (rr < 0.75), "Quality issue")
          .when(F.col("channel_code").isin("QC", "MKT", "D2C"),
                F.when(rr < 0.45, "Damaged in transit").when(rr < 0.65, "Wrong item").when(rr < 0.85, "Quality issue").otherwise("Changed mind"))
          .otherwise(F.when(rr < 0.40, "Near expiry").when(rr < 0.70, "Damaged packaging").when(rr < 0.90, "Quality issue").otherwise("Wrong item")))
returns = (lines.where("order_status = 'Delivered'").where(F.rand(SEED + 22) < p_ret)
           .withColumn("return_qty", F.when(F.col("quantity") <= 3, F.col("quantity"))
                       .otherwise(F.greatest(F.lit(1), F.ceil(F.col("quantity") * F.rand(SEED + 24) * 0.3))).cast("int"))
           .select(F.concat(F.lit("RT"), F.substring("line_id", 3, 40)).alias("return_id"), "line_id", "order_id", "sku_id",
                   F.date_add("date", (2 + F.rand(SEED + 25) * 23).cast("int")).alias("return_date"), "return_qty",
                   reason.alias("return_reason"),
                   F.round(F.col("line_amount_inr") * F.col("return_qty") / F.col("quantity"), 2).alias("refund_amount_inr")))
returns.repartition(8).write.mode("overwrite").parquet(f"{RAW}/returns")

# ---------------------------------------------------------------- daily demand per warehouse x sku (for inventory sim)
(lines.where("order_status = 'Delivered'")
 .groupBy(F.col("servicing_warehouse_id").alias("warehouse_id"), "sku_id", "date").agg(F.sum("quantity").alias("units"))
 .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{SYN}.daily_demand"))

print("orders:", spark.table(f"{SYN}.orders_truth").count(), "lines:", spark.table(f"{SYN}.lines_truth").count())
