"""Step 1/3 — Generate SpiceRoute master/reference data + generator helper tables.

Writes raw files to /Volumes/<catalog>/bronze/raw_landing/... and helper Delta tables to <catalog>.synthetic.*
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(sys.argv[0] if sys.argv and sys.argv[0] else __file__)))
try:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass

import math
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import spice_common as C

args = C.parse_args()
CAT = args.catalog
RAW = C.raw_root(CAT)
SYN = f"{CAT}.synthetic"
rng = np.random.default_rng(args.seed)
spark = SparkSession.builder.getOrCreate()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SYN}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CAT}.bronze.raw_landing")


def write_csv(pdf, name):
    spark.createDataFrame(pdf).coalesce(1).write.mode("overwrite").option("header", True).csv(f"{RAW}/{name}")


def save_syn(pdf_or_df, name):
    df = spark.createDataFrame(pdf_or_df) if isinstance(pdf_or_df, pd.DataFrame) else pdf_or_df
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{SYN}.{name}")


def haversine(lat1, lon1, lat2, lon2):
    p = math.pi / 180
    a = 0.5 - math.cos((lat2 - lat1) * p) / 2 + math.cos(lat1 * p) * math.cos(lat2 * p) * (1 - math.cos((lon2 - lon1) * p)) / 2
    return 12742 * math.asin(math.sqrt(a))


days = pd.date_range(C.START_DATE, C.END_DATE, freq="D")

# ======================================================================================
# 1. Geography, warehouses, channels
# ======================================================================================
geo = pd.DataFrame(C.CITIES, columns=["city", "state", "zone", "tier", "lat", "lon", "population_m"])
geo["city_id"] = [f"CTY-{i:03d}" for i in range(1, len(geo) + 1)]
geo["state_code"] = geo["state"].map(C.STATE_CODES)
geo["is_metro"] = geo["city"].isin(["Delhi", "Mumbai", "Bengaluru", "Chennai", "Kolkata", "Hyderabad", "Pune", "Ahmedabad"])


def nearest_wh(r):
    if r.zone == "North-East":
        return "WH-GAU"
    return min(C.WAREHOUSES, key=lambda w: haversine(r.lat, r.lon, w[5], w[6]))[0]


geo["warehouse_id"] = geo.apply(nearest_wh, axis=1)
write_csv(geo[["city_id", "city", "state", "state_code", "zone", "tier", "is_metro", "lat", "lon", "population_m"]], "reference/geography")

wh = pd.DataFrame(C.WAREHOUSES, columns=["warehouse_id", "warehouse_name", "city", "state", "zone", "lat", "lon"])
wh["capacity_tonnes"] = [1800, 1500, 900, 800, 1100, 900, 1000, 300]
write_csv(wh, "reference/warehouses")

ch = pd.DataFrame(C.CHANNELS, columns=["channel_code", "channel_name", "channel_group", "partner_margin_pct", "growth_pa", "store_type"])
write_csv(ch[["channel_code", "channel_name", "channel_group", "partner_margin_pct"]], "reference/channels")

# ======================================================================================
# 2. Festival calendar
# ======================================================================================
fest_rows = []
for name, (dts, zones, lead, uplift, boosts, ) in C.FESTIVALS.items():
    for d in dts:
        fest_rows.append(dict(festival_name=name, festival_date=d, year=int(d[:4]),
                              region_relevance="All-India" if zones is None else ",".join(zones),
                              lead_days=lead, expected_uplift_pct=round((uplift - 1) * 100, 1),
                              key_products=",".join(boosts.keys())))
fest = pd.DataFrame(fest_rows)
write_csv(fest, "reference/festivals")

# ======================================================================================
# 3. Products, price history, raw-material index, monthly unit costs
# ======================================================================================
prods = pd.DataFrame(C.build_products())
months = pd.period_range(C.START_DATE, C.CALENDAR_END, freq="M")

# MRP history (SCD2 source): base MRP is the price at START_DATE, then revisions
price_rows = []
for p in prods.itertuples():
    mrp = p.mrp_inr
    start = "2023-04-01"
    for eff, codes, pct in C.PRICE_REVISIONS:
        if codes is None or p.base_spice_code in codes:
            price_rows.append(dict(sku_id=p.sku_id, effective_from=start, mrp_inr=round(mrp, 0)))
            mrp = mrp * (1 + pct)
            start = eff
    price_rows.append(dict(sku_id=p.sku_id, effective_from=start, mrp_inr=round(mrp, 0)))
price_hist = pd.DataFrame(price_rows)
price_hist["effective_to"] = price_hist.groupby("sku_id")["effective_from"].shift(-1)
prods = prods.merge(price_hist.groupby("sku_id").tail(1)[["sku_id", "mrp_inr"]].rename(columns={"mrp_inr": "current_mrp_inr"}), on="sku_id")
write_csv(price_hist[["sku_id", "effective_from", "effective_to", "mrp_inr"]], "product_prices")

# raw material price index per base spice per month (harvest seasonality + random walk + events)
harvest_dip = {"TUR": 2, "RCH": 2, "KCH": 3, "COR": 3, "CUM": 3, "CUP": 3, "BPW": 1, "BPP": 1, "CAR": 9, "MUS": 3, "FEN": 3}
base_price_kg = {s[0]: s[5] * 10 * C.COST_RATIO[s[2]] * 0.7 for s in C.SPICES}
rm_rows = []
for code in base_price_kg:
    walk = 1.0
    for m in months:
        walk = float(np.clip(walk * math.exp(rng.normal(0.004, 0.025)), 0.85, 1.25))
        hm = harvest_dip.get(code)
        seas = 1.0 if hm is None else 1 - 0.06 * math.cos(2 * math.pi * (m.month - hm) / 12)
        ev = 1.0
        for s, e, peak in C.COST_EVENTS.get(code, []):
            s_, e_ = pd.Period(s, "M"), pd.Period(e, "M")
            if s_ <= m <= e_:
                span = (e_ - s_).n + 1
                pos = (m - s_).n + 0.5
                ev = 1 + (peak - 1) * math.sin(math.pi * pos / span)
        idx = walk * seas * ev
        rm_rows.append(dict(base_spice_code=code, month=m.to_timestamp().date(), price_index=round(idx, 4),
                            price_per_kg_inr=round(base_price_kg[code] * idx, 2)))
rm = pd.DataFrame(rm_rows)
write_csv(rm.assign(month=rm.month.astype(str)), "raw_material_prices")

# product x month: MRP in force on the 15th + unit cost (standard cost moved by raw material index)
pm_rows = []
ph = price_hist.copy()
for p in prods.itertuples():
    hist = ph[ph.sku_id == p.sku_id]
    rmi = rm[rm.base_spice_code == p.base_spice_code].set_index("month")["price_index"]
    base_mrp = hist.iloc[0].mrp_inr
    for m in months:
        mid = str(m.to_timestamp().date().replace(day=15))
        mrp = hist[hist.effective_from <= mid].iloc[-1].mrp_inr
        idx = rmi[m.to_timestamp().date()]
        # material ~70% of cost, packaging/conversion ~30% (inflates 4% p.a.)
        yrs = (m.to_timestamp() - pd.Timestamp(C.START_DATE)).days / 365.0
        unit_cost = base_mrp * p.std_cost_ratio * (0.7 * idx + 0.3 * (1.04 ** yrs))
        pm_rows.append(dict(sku_id=p.sku_id, month=m.to_timestamp().date(), mrp_inr=float(mrp), unit_cost_inr=round(unit_cost, 2)))
pm = pd.DataFrame(pm_rows)
save_syn(pm, "product_month")
write_csv(pm.assign(month=pm.month.astype(str)).rename(columns={"mrp_inr": "_mrp"})[["sku_id", "month", "unit_cost_inr"]], "product_costs")

prod_out = prods[["sku_id", "product_name", "base_spice_code", "base_spice", "category", "sub_category", "pack_size_g",
                  "is_organic", "current_mrp_inr", "gst_rate", "shelf_life_days", "launch_date"]].rename(columns={"current_mrp_inr": "mrp_inr"})
write_csv(prod_out, "products")
save_syn(prods[["sku_id", "base_spice_code", "category", "pack_size_g", "is_organic", "gst_rate", "launch_date", "popularity"]], "products")

# ======================================================================================
# 4. Suppliers
# ======================================================================================
SUPPLIER_ORIGINS = {
    "TUR": [("Erode", "Tamil Nadu"), ("Sangli", "Maharashtra"), ("Nizamabad", "Telangana")],
    "RCH": [("Guntur", "Andhra Pradesh"), ("Byadgi", "Karnataka"), ("Khammam", "Telangana")],
    "KCH": [("Srinagar", "Jammu and Kashmir"), ("Byadgi", "Karnataka")],
    "COR": [("Kota", "Rajasthan"), ("Guna", "Madhya Pradesh")], "CUM": [("Unjha", "Gujarat"), ("Jodhpur", "Rajasthan")],
    "CUP": [("Unjha", "Gujarat")], "BPW": [("Wayanad", "Kerala"), ("Idukki", "Kerala")], "BPP": [("Kochi", "Kerala")],
    "CAR": [("Idukki", "Kerala"), ("Bodinayakanur", "Tamil Nadu")], "BCA": [("Gangtok", "Sikkim")],
    "SAF": [("Pampore", "Jammu and Kashmir")], "MUS": [("Bharatpur", "Rajasthan"), ("Alwar", "Rajasthan")],
    "FEN": [("Nagaur", "Rajasthan")], "FNL": [("Unjha", "Gujarat")], "AJW": [("Chittorgarh", "Rajasthan")],
    "HNG": [("Hathras", "Uttar Pradesh")], "CLO": [("Nagercoil", "Tamil Nadu")], "CIN": [("Kozhikode", "Kerala")],
    "GIN": [("Kochi", "Kerala")], "MAC": [("Kottayam", "Kerala")], "AMC": [("Lucknow", "Uttar Pradesh")],
    "KSM": [("Nagaur", "Rajasthan")], "BAY": [("Dehradun", "Uttarakhand")], "BLS": [("Bikaner", "Rajasthan")],
}
SUFFIX = ["Agro Exports", "Spice Traders", "Farmer Producer Co.", "Masala Mills", "Agri Ventures", "Commodities"]
sup_rows = []
i = 1
for code, origins in SUPPLIER_ORIGINS.items():
    for town, state in origins:
        sup_rows.append(dict(supplier_id=f"SUP-{i:03d}", supplier_name=f"{town} {SUFFIX[i % len(SUFFIX)]}",
                             origin_town=town, origin_state=state, primary_spice_code=code,
                             reliability_score=round(float(rng.uniform(0.82, 0.98)), 2),
                             lead_time_days=int(rng.integers(6, 18)), supplier_tier="Primary"))
        i += 1
# blend ingredients + generic suppliers, and the low-grade alternate chilli supplier of the 2024 shortage story
for town, state, code in [("Delhi", "Delhi", "BLEND"), ("Indore", "Madhya Pradesh", "BLEND"), ("Ahmedabad", "Gujarat", "BLEND"),
                          ("Chennai", "Tamil Nadu", "BLEND"), ("Kolkata", "West Bengal", "BLEND")]:
    sup_rows.append(dict(supplier_id=f"SUP-{i:03d}", supplier_name=f"{town} {SUFFIX[i % len(SUFFIX)]}", origin_town=town,
                         origin_state=state, primary_spice_code=code, reliability_score=round(float(rng.uniform(0.85, 0.95)), 2),
                         lead_time_days=int(rng.integers(5, 12)), supplier_tier="Primary"))
    i += 1
sup_rows.append(dict(supplier_id=f"SUP-{i:03d}", supplier_name="Rapid Commodity Imports", origin_town="Mundra", origin_state="Gujarat",
                     primary_spice_code="RCH", reliability_score=0.61, lead_time_days=9, supplier_tier="Alternate"))
sup = pd.DataFrame(sup_rows)
write_csv(sup, "suppliers")
save_syn(sup, "suppliers")

# ======================================================================================
# 5. Stores / accounts
# ======================================================================================
from faker import Faker  # noqa: E402

fake = Faker("en_IN")
Faker.seed(args.seed)
CHAINS = ["FreshMart", "DailyBasket", "SuperValue", "CityMart", "MegaSaver", "HappyCart", "GreenGrocer", "BigBazaarwala"]
QC_PARTNERS = ["ZipCart", "Dash10", "InstaBasket"]
MKT_PARTNERS = ["ShopKart", "BharatBazaar"]
tier_factor = {1: 1.3, 2: 1.0, 3: 0.75}
geo["w"] = geo.population_m ** 0.85
store_rows = []


def pick_cities(n, weights):
    w = np.asarray(weights, dtype=float)
    return rng.choice(len(geo), size=n, p=w / w.sum())


def add_store(stype, ch_code, gi, name, opened, base_lambda, size_band, closed=None):
    g = geo.iloc[gi]
    sid = f"ST-{len(store_rows) + 1:05d}"
    store_rows.append(dict(store_id=sid, store_name=name, store_type=stype, channel_code=ch_code, city_id=g.city_id,
                           city=g.city, state=g.state, zone=g.zone, tier=int(g.tier), servicing_warehouse_id=g.warehouse_id,
                           opened_date=opened, closed_date=closed, size_band=size_band,
                           sales_rep_id=f"REP-{C.ZONE_CODE[g.zone]}-{int(rng.integers(1, 15)):02d}",
                           base_lambda=float(base_lambda * tier_factor[int(g.tier)] * rng.lognormal(0, 0.45))))


def old_date():
    return str(date(2012, 1, 1) + timedelta(days=int(rng.integers(0, 4000))))


def date_between(a, b):
    a, b = pd.Timestamp(a), pd.Timestamp(b)
    return str((a + pd.Timedelta(days=int(rng.integers(0, (b - a).days)))).date())


# every city gets >=1 distributor
for gi in range(len(geo)):
    add_store("Distributor", "GT", gi, f"{fake.last_name()} {rng.choice(['Distributors', 'Agencies', 'Enterprises', 'Trading Co.'])}",
              old_date(), 2.0, rng.choice(["Large", "Medium"]))
for gi in pick_cities(350 - len(geo), geo.w):
    closed = date_between("2025-01-01", "2026-06-30") if rng.random() < 0.04 else None
    add_store("Distributor", "GT", gi, f"{fake.last_name()} {rng.choice(['Distributors', 'Agencies', 'Enterprises', 'Trading Co.'])}",
              old_date(), 2.0, rng.choice(["Large", "Medium", "Small"], p=[0.2, 0.5, 0.3]), closed)
for gi in pick_cities(600, geo.w * geo.tier.map({1: 2.0, 2: 1.0, 3: 0.4})):
    opened = old_date() if rng.random() < 0.85 else date_between(C.START_DATE, "2026-03-31")
    add_store("Supermarket", "MT", gi, f"{rng.choice(CHAINS)} {geo.iloc[gi].city} #{int(rng.integers(1, 60))}", opened, 1.2,
              rng.choice(["Large", "Medium", "Small"], p=[0.25, 0.5, 0.25]))
for k, gi in enumerate(pick_cities(120, geo.w * geo.tier.map({1: 2.5, 2: 1.0, 3: 0.0}))):
    opened = old_date() if k < 100 else date_between(C.START_DATE, "2026-05-31")
    add_store("Own Store", "OWN", gi, f"SpiceRoute Store - {geo.iloc[gi].city} {k + 1:03d}", opened, 60, rng.choice(["Flagship", "Standard"], p=[0.15, 0.85]))
for gi in pick_cities(900, geo.w * geo.tier.map({1: 1.6, 2: 1.0, 3: 0.5})):
    kind = rng.choice(["Hotel", "Restaurant", "Caterers", "Cloud Kitchen", "Dhaba"], p=[0.15, 0.4, 0.2, 0.15, 0.1])
    nm = {"Hotel": f"Hotel {fake.last_name()} {rng.choice(['Residency', 'Grand', 'Inn', 'Palace'])}",
          "Restaurant": f"{fake.first_name()}'s {rng.choice(['Kitchen', 'Bhojanalaya', 'Rasoi', 'Biryani House', 'Family Restaurant'])}",
          "Caterers": f"{fake.last_name()} Caterers", "Cloud Kitchen": f"{rng.choice(['Tadka', 'Masala', 'Curry', 'Zaika'])} Cloud Kitchen {int(rng.integers(1, 99))}",
          "Dhaba": f"{fake.first_name()} Da Dhaba"}[kind]
    add_store("HoReCa Account", "HRC", gi, nm, old_date() if rng.random() < 0.8 else date_between(C.START_DATE, "2026-06-30"), 0.7, kind)
tier1 = geo.index[geo.tier == 1].tolist()
tier2_big = geo.index[(geo.tier == 2) & (geo.population_m >= 2.0)].tolist()
for k in range(150):
    gi = int(rng.choice(tier1, p=(geo.loc[tier1, "w"] / geo.loc[tier1, "w"].sum()).values)) if k < 110 else int(rng.choice(tier2_big))
    opened = date_between("2023-06-01", "2025-03-31") if k < 110 else date_between("2025-01-01", "2026-06-30")
    add_store("Dark Store", "QC", gi, f"{rng.choice(QC_PARTNERS)} Dark Store {geo.iloc[gi].city} {k + 1:03d}", opened, 75, "Micro")
for w in C.WAREHOUSES:
    gi = int(geo.index[geo.city == w[2]][0])
    add_store("Marketplace FC", "MKT", gi, f"{MKT_PARTNERS[len(store_rows) % 2]} FC - {w[2]}", "2019-01-01", 1100 * (0.35 if w[0] == "WH-GAU" else 1.0), "Virtual")
for zone, hub in [("North", "Delhi"), ("West", "Mumbai"), ("South", "Bengaluru"), ("East", "Kolkata"), ("Central", "Indore"), ("North-East", "Guwahati")]:
    gi = int(geo.index[geo.city == hub][0])
    zw = geo[geo.zone == zone].population_m.sum() / geo.population_m.sum()
    add_store("D2C Web", "D2C", gi, f"SpiceRoute.in - {zone}", "2020-08-15", 3600 * zw / tier_factor[int(geo.iloc[gi].tier)], "Virtual")

stores = pd.DataFrame(store_rows)
stores.loc[stores.store_type.isin(["Marketplace FC", "D2C Web"]), "base_lambda"] = stores.base_lambda / np.exp(0.1)
save_syn(stores, "stores")

# raw store master: messy state spellings (5%), JSON lines
raw_st = stores.drop(columns=["base_lambda", "zone", "tier", "city_id"]).copy()
mask = rng.random(len(raw_st)) < 0.06
raw_st.loc[mask, "state"] = [rng.choice(C.STATE_ALIASES[s]) if s in C.STATE_ALIASES else s for s in raw_st.loc[mask, "state"]]
spark.createDataFrame(raw_st).coalesce(1).write.mode("overwrite").json(f"{RAW}/stores")

# ======================================================================================
# 6. Customers (1M) — Spark + Faker pandas UDF
# ======================================================================================
N_CUST = 1_000_000
geo["cust_w"] = geo.population_m * geo.tier.map({1: 1.8, 2: 1.0, 3: 0.55})
B = 10000
cw = (geo.cust_w / geo.cust_w.sum()).cumsum().values
bucket_city = np.searchsorted(cw, (np.arange(B) + 0.5) / B)
city_bucket = pd.DataFrame({"bucket": np.arange(B), "city_idx": bucket_city})
geo_idx = geo.reset_index().rename(columns={"index": "city_idx"})[["city_idx", "city", "state", "zone"]]
geo_idx["preferred_language_local"] = geo_idx.state.map(C.LANGUAGE_BY_STATE).fillna("Hindi")
cb = spark.createDataFrame(city_bucket).join(spark.createDataFrame(geo_idx), "city_idx")


@F.pandas_udf(StringType())
def fake_name(gender: pd.Series) -> pd.Series:
    from faker import Faker
    f = Faker("en_IN")
    return pd.Series([f.name_female() if g == "F" else f.name_male() for g in gender])


cust = (spark.range(0, N_CUST, numPartitions=64)
        .withColumn("bucket", (F.rand(args.seed) * B).cast("int"))
        .join(F.broadcast(cb), "bucket")
        .withColumn("customer_id", F.concat(F.lit("CU"), F.lpad((F.col("id") + 1).cast("string"), 7, "0")))
        .withColumn("gender", F.when(F.rand(args.seed + 1) < 0.56, "F").otherwise("M"))
        .withColumn("r_age", F.rand(args.seed + 2))
        .withColumn("age_band", F.when(F.col("r_age") < 0.18, "18-24").when(F.col("r_age") < 0.48, "25-34")
                    .when(F.col("r_age") < 0.73, "35-44").when(F.col("r_age") < 0.90, "45-54").otherwise("55+"))
        # signups accelerate over time (online growth); 30% pre-2023
        .withColumn("signup_date", F.when(F.rand(args.seed + 3) < 0.30,
                                          F.date_add(F.lit("2019-01-01").cast("date"), (F.rand(args.seed + 4) * 1730).cast("int")))
                    .otherwise(F.date_add(F.lit("2023-10-01").cast("date"), (F.pow(F.rand(args.seed + 5), 0.8) * 1090).cast("int"))))
        .withColumn("r_loy", F.rand(args.seed + 6))
        .withColumn("loyalty_tier", F.when(F.col("r_loy") < 0.55, "None").when(F.col("r_loy") < 0.80, "Silver")
                    .when(F.col("r_loy") < 0.95, "Gold").otherwise("Platinum"))
        .withColumn("preferred_language", F.when(F.rand(args.seed + 7) < 0.30, "English").otherwise(F.col("preferred_language_local")))
        .withColumn("full_name", fake_name(F.col("gender")))
        .withColumn("customer_type", F.lit("Retail Consumer")))
cust_cols = ["customer_id", "full_name", "gender", "age_band", "city", "state", "signup_date", "loyalty_tier", "preferred_language", "customer_type"]
cust.select(*cust_cols, "zone").write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{SYN}.customers_full")
cust_t = spark.table(f"{SYN}.customers_full")
cust_t.select(*cust_cols).repartition(8).write.mode("overwrite").parquet(f"{RAW}/customers")
# zone index ordered by signup date: customers with signup<=d are exactly zidx < count(zone, d)
(cust_t.withColumn("zidx", F.row_number().over(Window.partitionBy("zone").orderBy("signup_date", F.xxhash64("customer_id"))) - 1)
 .select("customer_id", "zone", "zidx", "signup_date", "city")
 .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{SYN}.customer_idx"))

# ======================================================================================
# 7. Promotions
# ======================================================================================
CONSUMER_CH = ["MT", "OWN", "QC", "MKT", "D2C"]
promo_rows = []
pid = 1
for name, (dts, zones, lead, uplift, boosts) in C.FESTIVALS.items():
    if uplift < 1.10:
        continue
    for d in dts:
        fd = pd.Timestamp(d)
        if fd.date() > C.END_DATE + timedelta(days=60):
            continue
        for chc in CONSUMER_CH:
            if rng.random() < 0.25:
                continue
            disc = float(np.clip(rng.lognormal(math.log(0.14), 0.35), 0.05, 0.30))
            scope = "All" if rng.random() < 0.6 else str(rng.choice(["Masala Blends", "Premium", "Ground Spices", "Whole Spices"]))
            short = name.split(" ")[0].split("/")[0]
            promo_rows.append(dict(promo_id=f"PR-{pid:04d}", promo_code=f"{short.upper()[:6]}{fd.year % 100}{chc}",
                                   promo_name=f"{short} {['Dhamaka', 'Utsav', 'Bonanza', 'Special'][pid % 4]} {fd.year} - {chc}",
                                   promo_type=str(rng.choice(["Percent Off", "Combo", "BOGO", "Free Gift"], p=[0.6, 0.2, 0.1, 0.1])),
                                   discount_pct=round(disc, 3), start_date=str((fd - pd.Timedelta(days=max(lead, 7))).date()),
                                   end_date=str((fd + pd.Timedelta(days=1)).date()), channel_code=chc, category_scope=scope,
                                   funded_by="SpiceRoute" if chc in ("OWN", "D2C") else "Co-funded", festival_name=name))
            pid += 1
# monthly online flash sales + marketplace mega sales + quarterly trade schemes for B2B
for m in pd.period_range(C.START_DATE, C.END_DATE, freq="M"):
    for chc in ["MKT", "D2C", "QC"]:
        if rng.random() < 0.7:
            st = m.to_timestamp() + pd.Timedelta(days=int(rng.integers(3, 20)))
            disc = float(np.clip(rng.lognormal(math.log(0.10), 0.3), 0.05, 0.25))
            promo_rows.append(dict(promo_id=f"PR-{pid:04d}", promo_code=f"FLASH{m.year % 100}{m.month:02d}{chc}",
                                   promo_name=f"Flash Sale {m.strftime('%b %Y')} - {chc}", promo_type="Percent Off",
                                   discount_pct=round(disc, 3), start_date=str(st.date()), end_date=str((st + pd.Timedelta(days=2)).date()),
                                   channel_code=chc, category_scope="All", funded_by="Co-funded", festival_name=None))
            pid += 1
for q in pd.period_range(C.START_DATE, C.END_DATE, freq="Q"):
    for chc in ["GT", "HRC"]:
        st = q.start_time + pd.Timedelta(days=int(rng.integers(0, 30)))
        promo_rows.append(dict(promo_id=f"PR-{pid:04d}", promo_code=f"TRADE{q.year % 100}Q{q.quarter}{chc}",
                               promo_name=f"Trade Scheme {q.year}-Q{q.quarter} - {chc}", promo_type="Volume Rebate",
                               discount_pct=round(float(rng.uniform(0.03, 0.08)), 3), start_date=str(st.date()),
                               end_date=str((st + pd.Timedelta(days=21)).date()), channel_code=chc, category_scope="All",
                               funded_by="SpiceRoute", festival_name=None))
        pid += 1
promos = pd.DataFrame(promo_rows)
spark.createDataFrame(promos).coalesce(1).write.mode("overwrite").json(f"{RAW}/promotions")

# promo_day: one winning (max discount) promo per channel x day
pd_rows = []
for p in promos.itertuples():
    for d in pd.date_range(p.start_date, p.end_date):
        if C.START_DATE <= d.date() <= C.END_DATE:
            pd_rows.append((p.channel_code, d.date(), p.promo_id, p.promo_code, p.discount_pct, p.category_scope))
promo_day = (pd.DataFrame(pd_rows, columns=["channel_code", "date", "promo_id", "promo_code", "discount_pct", "category_scope"])
             .sort_values("discount_pct", ascending=False).drop_duplicates(["channel_code", "date"]))
save_syn(promo_day, "promo_day")

# ======================================================================================
# 8. Demand multipliers: festival (zone x day) overall + product mix weights (zone x week x mix_group)
# ======================================================================================
n_days = len(days)
zones = C.ZONES
zc = [C.ZONE_CODE[z] for z in zones]
overall = np.ones((n_days, len(zones)))
tag_boost = {}  # tag -> array(days, zones)
fest_name_day = {}


def wfun(off, lead):
    if -7 <= off <= 0:
        return 1.0
    if -lead <= off < -7:
        return 0.5
    if 1 <= off <= 2:
        return 0.3
    return 0.0


day_index = {d.date(): i for i, d in enumerate(days)}
for name, (dts, fzones, lead, uplift, boosts) in C.FESTIVALS.items():
    for d in dts:
        fd = pd.Timestamp(d).date()
        for off in range(-lead, 11):
            dd = fd + timedelta(days=off)
            if dd not in day_index:
                continue
            i = day_index[dd]
            if off == 0:
                fest_name_day.setdefault(dd, set()).add(name)
            for j, z in enumerate(zc):
                rel = fzones is None or z in fzones
                strength = 1.0 if rel else 0.25
                w = wfun(off, lead) * strength
                if off >= 3 and rel:
                    overall[i, j] *= 1 - 0.12 * (1 - (off - 3) / 8)  # post-festival dip
                if w == 0:
                    continue
                overall[i, j] *= 1 + (uplift - 1) * w
                for tag, b in boosts.items():
                    arr = tag_boost.setdefault(tag, np.ones((n_days, len(zones))))
                    arr[i, j] *= 1 + (b - 1) * w

dz = pd.DataFrame([(d.date(), z, overall[i, j]) for i, d in enumerate(days) for j, z in enumerate(zones)],
                  columns=["date", "zone", "festival_mult"])
md = pd.Series([d.strftime("%m-%d") for d in days])
wed = np.zeros(n_days, dtype=bool)
for a, b in C.WEDDING_WINDOWS:
    wed |= ((md >= a) & (md <= b)).values if a < b else ((md >= a) | (md <= b)).values
dz["is_wedding_season"] = np.repeat(wed, len(zones))
save_syn(dz, "day_zone_mult")

# product weights per day x zone, averaged to ISO week, per mix group
prod_list = prods.to_dict("records")
start_ts = pd.Timestamp(C.START_DATE)
sh_s, sh_e = pd.Timestamp(C.CHILLI_SHORTAGE[0]), pd.Timestamp(C.CHILLI_SHORTAGE[1])
month_idx = np.array([d.month - 1 for d in days])
yrs = np.array([(d - start_ts).days / 365.0 for d in days])
in_short = np.array([(sh_s <= d <= sh_e) for d in days])
CAT_TREND = {"Premium": 0.15, "Masala Blends": 0.08, "Ground Spices": 0.03, "Whole Spices": 0.02, "Seasonings": 0.05}
week_start = np.array([(d - pd.Timedelta(days=d.weekday())).date() for d in days])
weeks = sorted(set(week_start))
week_pos = {w: k for k, w in enumerate(weeks)}
wk_of_day = np.array([week_pos[w] for w in week_start])
counts = np.bincount(wk_of_day)
mix_rows = []
for p in prod_list:
    season = np.array(C.SEASON[p["season"]])[month_idx]
    launch = pd.Timestamp(p["launch_date"])
    age = np.array([(d - launch).days for d in days])
    launch_f = np.where(age < 0, 0.0, np.minimum(1.0, 0.2 + 0.8 * age / 120.0))
    trend = (1 + CAT_TREND[p["category"]]) ** yrs
    if p["is_organic"]:
        trend = trend * np.where(age > 0, 1.6 ** np.clip(age / 365.0, 0, None), 1.0)
    base = p["popularity"] * season * launch_f * trend
    for j, z in enumerate(zones):
        reg = p["region"].get(C.ZONE_CODE[z], 1.0)
        boost = np.ones(n_days)
        for tag in p["tags"]:
            if tag in tag_boost:
                boost = boost * tag_boost[tag][:, j]
        wed_b = np.where(wed & bool(set(p["tags"]) & {"wedding"}), 1.2, 1.0)
        short = np.ones(n_days)
        if p["base_spice_code"] in C.CHILLI_CODES:
            short = np.where(in_short, 0.55 if C.ZONE_CODE[z] in ("S", "W") else 0.85, 1.0)
        daily = base * reg * boost * wed_b * short
        wk = np.bincount(wk_of_day, weights=daily) / counts
        for grp in ("consumer", "bulk"):
            share = C.PACK_SHARE[grp][p["pack_size_g"]]
            for k, w in enumerate(weeks):
                mix_rows.append((z, w, grp, p["sku_id"], float(wk[k] * share)))

mix = pd.DataFrame(mix_rows, columns=["zone", "week_start", "mix_group", "sku_id", "weight"])
# normalise pack shares within a base spice so a spice's total weight = its popularity, regardless of pack count
pack_norm = {}
for grp in ("consumer", "bulk"):
    std = prods[~prods.is_organic]
    tot = std.assign(s=std.pack_size_g.map(C.PACK_SHARE[grp])).groupby("base_spice_code").s.sum()
    for p in prods.itertuples():
        pack_norm[(grp, p.sku_id)] = 1.0 if p.is_organic else 1.0 / tot[p.base_spice_code]
mix["weight"] = mix.weight * [pack_norm[(g, s)] for g, s in zip(mix.mix_group, mix.sku_id)]
mix = mix.sort_values(["zone", "week_start", "mix_group", "sku_id"])
grp_sum = mix.groupby(["zone", "week_start", "mix_group"]).weight.transform("sum")
mix["p"] = mix.weight / grp_sum
mix["cum_hi"] = mix.groupby(["zone", "week_start", "mix_group"]).p.cumsum()
mix["cum_lo"] = mix.cum_hi - mix.p
save_syn(mix[["zone", "week_start", "mix_group", "sku_id", "p", "cum_lo", "cum_hi"]], "product_mix_week")

# expand to bucket lookup (B buckets per zone-week-group) in Spark
NB = 4000
mix_sdf = spark.table(f"{SYN}.product_mix_week")
buckets = (mix_sdf
           .withColumn("b_lo", F.floor(F.col("cum_lo") * NB).cast("int"))
           .withColumn("b_hi", F.least(F.lit(NB - 1), F.ceil(F.col("cum_hi") * NB).cast("int") - 1))
           .where("b_hi >= b_lo")
           .withColumn("bucket", F.explode(F.sequence("b_lo", "b_hi")))
           # bucket may straddle two products -> keep the one with larger overlap (first wins deterministically)
           .withColumn("rn", F.row_number().over(Window.partitionBy("zone", "week_start", "mix_group", "bucket").orderBy(F.desc("p"))))
           .where("rn = 1")
           .select("zone", "week_start", "mix_group", "bucket", "sku_id"))
buckets.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{SYN}.product_mix_bucket")

print("dims done:", len(prods), "SKUs;", len(stores), "stores;", N_CUST, "customers;", len(promos), "promotions")
