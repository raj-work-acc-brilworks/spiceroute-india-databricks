"""Shared reference data, configuration and helpers for SpiceRoute India synthetic data generation.

Everything here is deterministic. Spark-side randomness is seeded in the generator scripts.
"""
import argparse
from datetime import date

# --------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------
START_DATE = date(2023, 10, 1)
END_DATE = date(2026, 9, 30)          # last day of actuals
CALENDAR_END = date(2026, 12, 31)     # dim_date extends past actuals for the forecast horizon
ZONES = ["North", "South", "East", "West", "Central", "North-East"]
ZONE_CODE = {"North": "N", "South": "S", "East": "E", "West": "W", "Central": "C", "North-East": "NE"}


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", default="spiceroute")
    p.add_argument("--target_lines", type=int, default=25_000_000)
    p.add_argument("--seed", type=int, default=42)
    args, _ = p.parse_known_args()
    if args.catalog in ("dev_retail_catalog", "workspace", "samples", "system"):
        raise ValueError(f"Refusing to write SpiceRoute data into catalog '{args.catalog}'")
    return args


def raw_root(catalog):
    return f"/Volumes/{catalog}/bronze/raw_landing"


# --------------------------------------------------------------------------------------
# Geography: (city, state, zone, tier, lat, lon, population_millions)
# --------------------------------------------------------------------------------------
CITIES = [
    # North
    ("Delhi", "Delhi", "North", 1, 28.61, 77.21, 32.0), ("Gurugram", "Haryana", "North", 1, 28.46, 77.03, 2.5),
    ("Noida", "Uttar Pradesh", "North", 1, 28.54, 77.39, 2.5), ("Faridabad", "Haryana", "North", 2, 28.41, 77.32, 2.0),
    ("Ghaziabad", "Uttar Pradesh", "North", 2, 28.67, 77.45, 2.4), ("Lucknow", "Uttar Pradesh", "North", 2, 26.85, 80.95, 3.8),
    ("Kanpur", "Uttar Pradesh", "North", 2, 26.45, 80.33, 3.2), ("Agra", "Uttar Pradesh", "North", 2, 27.18, 78.01, 2.0),
    ("Varanasi", "Uttar Pradesh", "North", 2, 25.32, 82.97, 1.6), ("Prayagraj", "Uttar Pradesh", "North", 2, 25.44, 81.85, 1.5),
    ("Meerut", "Uttar Pradesh", "North", 2, 28.98, 77.71, 1.6), ("Bareilly", "Uttar Pradesh", "North", 3, 28.37, 79.43, 1.1),
    ("Gorakhpur", "Uttar Pradesh", "North", 3, 26.76, 83.37, 0.9), ("Aligarh", "Uttar Pradesh", "North", 3, 27.88, 78.08, 1.0),
    ("Jaipur", "Rajasthan", "North", 2, 26.91, 75.79, 4.0), ("Jodhpur", "Rajasthan", "North", 2, 26.24, 73.02, 1.4),
    ("Udaipur", "Rajasthan", "North", 3, 24.58, 73.71, 0.6), ("Kota", "Rajasthan", "North", 3, 25.21, 75.86, 1.2),
    ("Ajmer", "Rajasthan", "North", 3, 26.45, 74.64, 0.6), ("Bikaner", "Rajasthan", "North", 3, 28.02, 73.31, 0.7),
    ("Chandigarh", "Chandigarh", "North", 2, 30.73, 76.78, 1.2), ("Ludhiana", "Punjab", "North", 2, 30.90, 75.86, 1.8),
    ("Amritsar", "Punjab", "North", 2, 31.63, 74.87, 1.3), ("Jalandhar", "Punjab", "North", 3, 31.33, 75.58, 0.9),
    ("Patiala", "Punjab", "North", 3, 30.34, 76.39, 0.5), ("Dehradun", "Uttarakhand", "North", 2, 30.32, 78.03, 0.9),
    ("Haridwar", "Uttarakhand", "North", 3, 29.95, 78.16, 0.3), ("Shimla", "Himachal Pradesh", "North", 3, 31.10, 77.17, 0.2),
    ("Srinagar", "Jammu and Kashmir", "North", 2, 34.08, 74.80, 1.5), ("Jammu", "Jammu and Kashmir", "North", 3, 32.73, 74.86, 0.7),
    ("Panipat", "Haryana", "North", 3, 29.39, 76.97, 0.5), ("Hisar", "Haryana", "North", 3, 29.15, 75.72, 0.4),
    # West
    ("Mumbai", "Maharashtra", "West", 1, 19.08, 72.88, 21.0), ("Pune", "Maharashtra", "West", 1, 18.52, 73.86, 7.0),
    ("Thane", "Maharashtra", "West", 1, 19.22, 72.98, 2.5), ("Navi Mumbai", "Maharashtra", "West", 1, 19.03, 73.03, 1.5),
    ("Nagpur", "Maharashtra", "West", 2, 21.15, 79.09, 2.9), ("Nashik", "Maharashtra", "West", 2, 20.00, 73.79, 2.0),
    ("Aurangabad", "Maharashtra", "West", 2, 19.88, 75.34, 1.5), ("Solapur", "Maharashtra", "West", 3, 17.66, 75.91, 1.0),
    ("Kolhapur", "Maharashtra", "West", 3, 16.70, 74.24, 0.6), ("Ahmedabad", "Gujarat", "West", 1, 23.02, 72.57, 8.5),
    ("Surat", "Gujarat", "West", 2, 21.17, 72.83, 7.0), ("Vadodara", "Gujarat", "West", 2, 22.31, 73.18, 2.2),
    ("Rajkot", "Gujarat", "West", 2, 22.30, 70.80, 2.0), ("Bhavnagar", "Gujarat", "West", 3, 21.76, 72.15, 0.7),
    ("Jamnagar", "Gujarat", "West", 3, 22.47, 70.06, 0.7), ("Panaji", "Goa", "West", 3, 15.49, 73.83, 0.5),
    ("Gandhinagar", "Gujarat", "West", 3, 23.22, 72.65, 0.3),
    # South
    ("Bengaluru", "Karnataka", "South", 1, 12.97, 77.59, 13.0), ("Chennai", "Tamil Nadu", "South", 1, 13.08, 80.27, 11.0),
    ("Hyderabad", "Telangana", "South", 1, 17.39, 78.49, 10.5), ("Coimbatore", "Tamil Nadu", "South", 2, 11.02, 76.96, 2.5),
    ("Madurai", "Tamil Nadu", "South", 2, 9.93, 78.12, 1.6), ("Tiruchirappalli", "Tamil Nadu", "South", 3, 10.79, 78.70, 1.1),
    ("Salem", "Tamil Nadu", "South", 3, 11.66, 78.15, 0.9), ("Erode", "Tamil Nadu", "South", 3, 11.34, 77.72, 0.6),
    ("Tirunelveli", "Tamil Nadu", "South", 3, 8.71, 77.76, 0.5), ("Vellore", "Tamil Nadu", "South", 3, 12.92, 79.13, 0.5),
    ("Mysuru", "Karnataka", "South", 2, 12.30, 76.64, 1.1), ("Mangaluru", "Karnataka", "South", 2, 12.91, 74.86, 0.7),
    ("Hubballi", "Karnataka", "South", 3, 15.36, 75.12, 1.0), ("Belagavi", "Karnataka", "South", 3, 15.85, 74.50, 0.6),
    ("Kochi", "Kerala", "South", 2, 9.93, 76.27, 2.2), ("Thiruvananthapuram", "Kerala", "South", 2, 8.52, 76.94, 1.7),
    ("Kozhikode", "Kerala", "South", 2, 11.26, 75.78, 2.0), ("Thrissur", "Kerala", "South", 3, 10.53, 76.21, 1.9),
    ("Visakhapatnam", "Andhra Pradesh", "South", 2, 17.69, 83.22, 2.2), ("Vijayawada", "Andhra Pradesh", "South", 2, 16.51, 80.65, 1.8),
    ("Guntur", "Andhra Pradesh", "South", 3, 16.31, 80.44, 0.8), ("Tirupati", "Andhra Pradesh", "South", 3, 13.63, 79.42, 0.5),
    ("Nellore", "Andhra Pradesh", "South", 3, 14.44, 79.99, 0.6), ("Warangal", "Telangana", "South", 3, 17.97, 79.59, 0.9),
    ("Karimnagar", "Telangana", "South", 3, 18.44, 79.13, 0.3), ("Puducherry", "Puducherry", "South", 3, 11.94, 79.81, 0.7),
    # East
    ("Kolkata", "West Bengal", "East", 1, 22.57, 88.36, 15.0), ("Howrah", "West Bengal", "East", 2, 22.59, 88.31, 1.2),
    ("Durgapur", "West Bengal", "East", 3, 23.52, 87.31, 0.6), ("Asansol", "West Bengal", "East", 3, 23.68, 86.98, 1.2),
    ("Siliguri", "West Bengal", "East", 3, 26.73, 88.40, 0.8), ("Bhubaneswar", "Odisha", "East", 2, 20.30, 85.82, 1.2),
    ("Cuttack", "Odisha", "East", 3, 20.46, 85.88, 0.7), ("Rourkela", "Odisha", "East", 3, 22.26, 84.85, 0.6),
    ("Patna", "Bihar", "East", 2, 25.59, 85.14, 2.5), ("Gaya", "Bihar", "East", 3, 24.79, 85.00, 0.5),
    ("Muzaffarpur", "Bihar", "East", 3, 26.12, 85.39, 0.4), ("Bhagalpur", "Bihar", "East", 3, 25.25, 86.97, 0.4),
    ("Ranchi", "Jharkhand", "East", 2, 23.34, 85.31, 1.5), ("Jamshedpur", "Jharkhand", "East", 2, 22.80, 86.20, 1.4),
    ("Dhanbad", "Jharkhand", "East", 3, 23.80, 86.43, 1.2),
    # Central
    ("Indore", "Madhya Pradesh", "Central", 2, 22.72, 75.86, 3.3), ("Bhopal", "Madhya Pradesh", "Central", 2, 23.26, 77.41, 2.4),
    ("Jabalpur", "Madhya Pradesh", "Central", 2, 23.18, 79.99, 1.4), ("Gwalior", "Madhya Pradesh", "Central", 2, 26.22, 78.18, 1.2),
    ("Ujjain", "Madhya Pradesh", "Central", 3, 23.18, 75.78, 0.6), ("Sagar", "Madhya Pradesh", "Central", 3, 23.84, 78.74, 0.4),
    ("Raipur", "Chhattisgarh", "Central", 2, 21.25, 81.63, 1.4), ("Bhilai", "Chhattisgarh", "Central", 3, 21.21, 81.38, 1.1),
    ("Bilaspur", "Chhattisgarh", "Central", 3, 22.08, 82.15, 0.5),
    # North-East
    ("Guwahati", "Assam", "North-East", 2, 26.14, 91.74, 1.1), ("Dibrugarh", "Assam", "North-East", 3, 27.47, 94.91, 0.2),
    ("Silchar", "Assam", "North-East", 3, 24.83, 92.78, 0.2), ("Jorhat", "Assam", "North-East", 3, 26.75, 94.20, 0.15),
    ("Shillong", "Meghalaya", "North-East", 3, 25.58, 91.89, 0.4), ("Agartala", "Tripura", "North-East", 3, 23.83, 91.29, 0.5),
    ("Imphal", "Manipur", "North-East", 3, 24.82, 93.94, 0.3), ("Aizawl", "Mizoram", "North-East", 3, 23.73, 92.72, 0.3),
    ("Kohima", "Nagaland", "North-East", 3, 25.67, 94.11, 0.1), ("Dimapur", "Nagaland", "North-East", 3, 25.91, 93.73, 0.2),
    ("Gangtok", "Sikkim", "North-East", 3, 27.33, 88.61, 0.1), ("Itanagar", "Arunachal Pradesh", "North-East", 3, 27.08, 93.61, 0.1),
]

STATE_CODES = {
    "Delhi": "DL", "Haryana": "HR", "Uttar Pradesh": "UP", "Rajasthan": "RJ", "Chandigarh": "CH", "Punjab": "PB",
    "Uttarakhand": "UK", "Himachal Pradesh": "HP", "Jammu and Kashmir": "JK", "Maharashtra": "MH", "Gujarat": "GJ",
    "Goa": "GA", "Karnataka": "KA", "Tamil Nadu": "TN", "Telangana": "TS", "Kerala": "KL", "Andhra Pradesh": "AP",
    "Puducherry": "PY", "West Bengal": "WB", "Odisha": "OD", "Bihar": "BR", "Jharkhand": "JH", "Madhya Pradesh": "MP",
    "Chhattisgarh": "CG", "Assam": "AS", "Meghalaya": "ML", "Tripura": "TR", "Manipur": "MN", "Mizoram": "MZ",
    "Nagaland": "NL", "Sikkim": "SK", "Arunachal Pradesh": "AR",
}

# Messy spellings injected into raw store files (silver standardises them)
STATE_ALIASES = {
    "Tamil Nadu": ["Tamilnadu", "TN", "Tamil nadu"], "Maharashtra": ["Maharastra", "MH"], "Delhi": ["NCT of Delhi", "Delhi NCR", "New Delhi"],
    "West Bengal": ["West Bangal", "WB"], "Karnataka": ["Karnatka", "KA"], "Odisha": ["Orissa"], "Uttarakhand": ["Uttaranchal"],
    "Uttar Pradesh": ["UP", "Uttar pradesh"], "Telangana": ["Telengana"], "Gujarat": ["Gujrat"],
}

LANGUAGE_BY_STATE = {
    "Tamil Nadu": "Tamil", "Puducherry": "Tamil", "Karnataka": "Kannada", "Kerala": "Malayalam", "Telangana": "Telugu",
    "Andhra Pradesh": "Telugu", "Maharashtra": "Marathi", "Goa": "Konkani", "Gujarat": "Gujarati", "West Bengal": "Bengali",
    "Tripura": "Bengali", "Odisha": "Odia", "Assam": "Assamese", "Punjab": "Punjabi",
}

# Warehouses (id, name, city, state, zone, lat, lon)
WAREHOUSES = [
    ("WH-DEL", "Delhi NCR DC (Kundli)", "Delhi", "Delhi", "North", 28.85, 77.13),
    ("WH-MUM", "Mumbai DC (Bhiwandi)", "Mumbai", "Maharashtra", "West", 19.30, 73.06),
    ("WH-AMD", "Ahmedabad DC (Changodar)", "Ahmedabad", "Gujarat", "West", 22.93, 72.44),
    ("WH-KOL", "Kolkata DC (Dankuni)", "Kolkata", "West Bengal", "East", 22.68, 88.29),
    ("WH-CHE", "Chennai DC (Sriperumbudur)", "Chennai", "Tamil Nadu", "South", 12.97, 79.95),
    ("WH-BLR", "Bengaluru DC (Hoskote)", "Bengaluru", "Karnataka", "South", 13.07, 77.80),
    ("WH-HYD", "Hyderabad DC (Shamshabad)", "Hyderabad", "Telangana", "South", 17.25, 78.43),
    ("WH-GAU", "Guwahati DC (Changsari)", "Guwahati", "Assam", "North-East", 26.25, 91.70),
]

# Channels (code, name, group, margin given to channel partner, growth p.a., store_type)
CHANNELS = [
    ("GT", "General Trade", "Offline", 0.22, 0.04, "Distributor"),
    ("MT", "Modern Trade", "Offline", 0.18, 0.09, "Supermarket"),
    ("OWN", "Own Retail", "Offline", 0.00, 0.06, "Own Store"),
    ("HRC", "HoReCa", "B2B", 0.20, 0.10, "HoReCa Account"),
    ("QC", "Quick Commerce", "Online", 0.12, 0.55, "Dark Store"),
    ("MKT", "Marketplace", "Online", 0.15, 0.30, "Marketplace FC"),
    ("D2C", "D2C Website", "Online", 0.05, 0.22, "D2C Web"),
]

# --------------------------------------------------------------------------------------
# Products: base spices
# (code, name, category, sub_category, popularity, mrp_per_100g, packs_g, season, region_mult, tags, organic_launch)
# --------------------------------------------------------------------------------------
G, W, B, S, P = "Ground Spices", "Whole Spices", "Masala Blends", "Seasonings", "Premium"
SPICES = [
    ("TUR", "Turmeric Powder", G, "Everyday Essentials", 10.0, 32, [50, 100, 200, 500, 1000], "flat", {}, [], "2024-08-01"),
    ("RCH", "Red Chilli Powder", G, "Everyday Essentials", 9.0, 45, [50, 100, 200, 500, 1000], "pickle", {"S": 1.5, "E": 1.1, "N": 0.95}, ["chilli"], "2024-08-01"),
    ("KCH", "Kashmiri Chilli Powder", G, "Everyday Essentials", 4.0, 70, [50, 100, 200, 500], "pickle", {"N": 1.4, "W": 1.1, "S": 0.8}, ["chilli", "festive_rich"], "2025-02-01"),
    ("COR", "Coriander Powder", G, "Everyday Essentials", 8.0, 30, [50, 100, 200, 500, 1000], "flat", {}, [], "2024-08-01"),
    ("CUP", "Cumin Powder", G, "Everyday Essentials", 4.0, 80, [50, 100, 200], "summer_mild", {"N": 1.3, "W": 1.2}, ["vrat"], "2025-02-01"),
    ("BPP", "Black Pepper Powder", G, "Everyday Essentials", 3.0, 120, [50, 100, 200], "winter", {"S": 1.3}, ["vrat", "winter"], None),
    ("GIN", "Dry Ginger Powder", G, "Everyday Essentials", 1.5, 90, [50, 100], "winter_monsoon", {"N": 1.2}, ["winter"], None),
    ("AMC", "Amchur Powder", G, "Everyday Essentials", 1.5, 70, [50, 100], "summer", {"N": 1.4, "C": 1.3, "S": 0.5}, [], None),
    ("CUM", "Cumin Seeds", W, "Whole Spices", 6.0, 65, [50, 100, 200, 500, 1000], "flat", {"N": 1.2, "W": 1.3}, ["vrat"], "2024-08-01"),
    ("MUS", "Mustard Seeds", W, "Whole Spices", 4.0, 25, [50, 100, 200, 500, 1000], "pickle", {"E": 1.8, "S": 1.4, "NE": 1.6, "N": 0.8}, [], None),
    ("FEN", "Fenugreek Seeds", W, "Whole Spices", 2.0, 22, [50, 100, 200, 500], "pickle", {}, [], None),
    ("BPW", "Black Pepper Whole", W, "Whole Spices", 3.0, 110, [50, 100, 200, 500], "winter", {"S": 1.5}, ["winter"], None),
    ("CLO", "Cloves", W, "Whole Spices", 1.5, 180, [50, 100], "winter", {}, ["winter", "xmas"], None),
    ("CIN", "Cinnamon Sticks", W, "Whole Spices", 1.5, 90, [50, 100], "winter", {}, ["winter", "xmas"], None),
    ("BAY", "Bay Leaf", W, "Whole Spices", 1.0, 40, [25, 50], "flat", {"N": 1.3, "E": 1.4}, [], None),
    ("FNL", "Fennel Seeds", W, "Whole Spices", 2.0, 45, [50, 100, 200], "summer_mild", {"N": 1.3, "W": 1.2}, ["thandai"], None),
    ("AJW", "Ajwain", W, "Whole Spices", 1.5, 50, [50, 100], "winter", {"N": 1.3, "W": 1.2}, [], None),
    ("KSM", "Kasuri Methi", W, "Dried Herbs", 2.0, 60, [25, 50, 100], "flat", {"N": 1.8, "C": 1.3, "S": 0.6}, [], None),
    ("GAR", "Garam Masala", B, "Curry Masala", 8.0, 85, [50, 100, 200, 500, 1000], "winter_mild", {"N": 1.5, "C": 1.2, "S": 0.8}, ["festive_rich", "wedding"], "2024-08-01"),
    ("CHM", "Chicken Masala", B, "Non-Veg Masala", 4.0, 75, [50, 100, 200, 500], "flat", {"S": 1.2, "E": 1.3, "N": 1.1}, ["nonveg", "eid"], None),
    ("MTM", "Meat Masala", B, "Non-Veg Masala", 3.5, 80, [50, 100, 200, 500], "winter_mild", {"N": 1.2, "E": 1.2, "NE": 1.4}, ["nonveg", "eid", "wedding"], None),
    ("BIR", "Biryani Masala", B, "Rice Masala", 4.0, 90, [50, 100, 200, 500], "flat", {"S": 1.5, "N": 1.2}, ["eid", "wedding", "festive_rich"], None),
    ("SAM", "Sambar Powder", B, "South Indian", 5.0, 60, [100, 200, 500, 1000], "flat", {"S": 3.2, "W": 0.6, "N": 0.35, "E": 0.3, "C": 0.4, "NE": 0.3}, ["onam", "pongal"], None),
    ("RAS", "Rasam Powder", B, "South Indian", 3.0, 60, [100, 200, 500], "winter_monsoon", {"S": 3.2, "W": 0.4, "N": 0.25, "E": 0.2, "C": 0.3, "NE": 0.2}, ["onam"], None),
    ("CPD", "Chutney Podi", B, "South Indian", 1.2, 70, [100, 200], "flat", {"S": 3.0, "W": 0.5, "N": 0.2, "E": 0.2, "C": 0.3, "NE": 0.2}, ["onam"], None),
    ("PAV", "Pav Bhaji Masala", B, "Street Food", 2.5, 70, [50, 100, 200], "flat", {"W": 2.0, "N": 1.2, "S": 0.6}, [], None),
    ("MSL", "Kolhapuri Misal Masala", B, "Street Food", 1.0, 75, [50, 100], "flat", {"W": 3.0, "C": 0.8, "N": 0.3, "S": 0.4, "E": 0.2, "NE": 0.2}, ["ganesh"], None),
    ("CHL", "Chole Masala", B, "Curry Masala", 2.5, 70, [50, 100, 200], "winter_mild", {"N": 1.8, "C": 1.2, "S": 0.5}, ["festive_rich"], None),
    ("RJM", "Rajma Masala", B, "Curry Masala", 1.5, 70, [50, 100, 200], "winter_mild", {"N": 2.0, "C": 1.2, "S": 0.4, "E": 0.6}, [], None),
    ("PNR", "Paneer Butter Masala", B, "Curry Masala", 2.0, 80, [50, 100, 200], "flat", {"N": 1.6, "W": 1.2, "C": 1.2, "S": 0.7}, ["festive_rich", "wedding"], None),
    ("KKG", "Kitchen King Masala", B, "Curry Masala", 3.0, 75, [50, 100, 200, 500], "flat", {"N": 1.4, "W": 1.2, "C": 1.3, "S": 0.5}, ["festive_rich", "wedding"], None),
    ("EGG", "Egg Curry Masala", B, "Non-Veg Masala", 1.0, 65, [50, 100], "winter_mild", {"E": 1.8, "NE": 1.6, "S": 1.2, "N": 0.8}, ["nonveg"], None),
    ("TEA", "Tea Masala", B, "Beverage Masala", 2.0, 120, [50, 100], "winter_monsoon", {"N": 1.3, "W": 1.3}, ["winter"], None),
    ("PPH", "Panch Phoron", B, "Regional Masala", 1.5, 50, [50, 100, 200], "flat", {"E": 3.2, "NE": 2.0, "N": 0.3, "S": 0.2, "W": 0.2, "C": 0.4}, ["puja"], None),
    ("GOD", "Goda Masala", B, "Regional Masala", 1.5, 80, [50, 100, 200], "flat", {"W": 3.0, "C": 0.6, "N": 0.2, "S": 0.4, "E": 0.2, "NE": 0.2}, ["ganesh"], None),
    ("FCM", "Fish Curry Masala", B, "Non-Veg Masala", 2.0, 70, [50, 100, 200], "monsoon", {"E": 2.2, "S": 1.7, "W": 1.3, "NE": 1.6, "N": 0.4, "C": 0.3}, ["puja", "nonveg"], None),
    ("PKM", "Achar (Pickle) Masala", B, "Regional Masala", 1.5, 55, [100, 200, 500], "pickle_strong", {"N": 1.3, "W": 1.2}, [], None),
    ("CHT", "Chaat Masala", S, "Seasoning", 3.5, 65, [50, 100, 200], "summer", {"N": 1.6, "C": 1.3, "S": 0.6}, ["holi"], None),
    ("JLJ", "Jaljeera Powder", S, "Seasoning", 1.2, 60, [50, 100], "summer_strong", {"N": 1.6}, ["holi"], None),
    ("BLS", "Black Salt", S, "Seasoning", 1.5, 20, [100, 200], "summer", {"N": 1.5}, ["vrat"], None),
    ("HNG", "Hing (Asafoetida)", S, "Seasoning", 2.5, 600, [10, 25, 50], "flat", {"W": 1.5, "C": 1.3, "S": 1.2}, [], None),
    ("CAR", "Green Cardamom", P, "Premium Whole", 2.5, 350, [25, 50, 100], "winter", {"S": 1.3}, ["sweets", "festive_rich", "thandai"], None),
    ("BCA", "Black Cardamom", P, "Premium Whole", 1.0, 250, [25, 50], "winter", {"N": 1.4, "E": 1.2}, ["festive_rich"], None),
    ("MAC", "Mace & Nutmeg", P, "Premium Whole", 0.6, 400, [25, 50], "winter", {}, ["festive_rich", "sweets", "xmas"], None),
    ("SAF", "Kashmiri Saffron", P, "Premium Saffron", 0.6, 35000, [1, 2], "winter", {"N": 1.3}, ["sweets", "eid", "thandai"], None),
]
NEW_SPICE_LAUNCH = {"CPD": "2025-01-15", "MSL": "2024-04-01", "EGG": "2024-11-01"}  # products launched mid-history

GST_BY_CATEGORY = {G: 0.05, W: 0.05, B: 0.05, S: 0.12, P: 0.05}
COST_RATIO = {G: 0.44, W: 0.46, B: 0.40, S: 0.38, P: 0.58}  # standard cost as share of MRP
SHELF_LIFE = {G: 365, W: 540, B: 300, S: 365, P: 730}

# Month multipliers (Jan..Dec)
SEASON = {
    "flat":           [1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00],
    "winter":         [1.35, 1.25, 1.00, 0.85, 0.80, 0.80, 0.85, 0.85, 0.90, 1.05, 1.25, 1.40],
    "winter_mild":    [1.15, 1.10, 1.00, 0.95, 0.90, 0.90, 0.92, 0.92, 0.95, 1.05, 1.12, 1.18],
    "winter_monsoon": [1.30, 1.20, 1.00, 0.80, 0.75, 0.95, 1.20, 1.20, 1.05, 1.00, 1.15, 1.35],
    "monsoon":        [0.95, 0.95, 0.95, 0.95, 1.00, 1.15, 1.25, 1.25, 1.10, 1.00, 0.95, 0.95],
    "summer":         [0.80, 0.90, 1.15, 1.45, 1.55, 1.40, 1.05, 0.95, 0.95, 0.90, 0.85, 0.80],
    "summer_mild":    [0.92, 0.95, 1.05, 1.15, 1.18, 1.12, 1.00, 0.97, 0.96, 0.95, 0.93, 0.92],
    "summer_strong":  [0.55, 0.70, 1.20, 1.90, 2.20, 1.80, 1.00, 0.85, 0.80, 0.70, 0.60, 0.55],
    "pickle":         [0.95, 1.00, 1.25, 1.35, 1.30, 1.05, 0.95, 0.90, 0.90, 0.95, 0.95, 0.95],
    "pickle_strong":  [0.80, 0.95, 1.60, 1.90, 1.80, 1.10, 0.85, 0.75, 0.75, 0.80, 0.80, 0.80],
}

# Pack-size preference by mix group
PACK_SHARE = {
    "consumer": {1: 1.0, 2: 0.6, 10: 1.0, 25: 1.0, 50: 1.0, 100: 1.3, 200: 1.0, 500: 0.40, 1000: 0.18},
    "bulk":     {1: 0.5, 2: 1.0, 10: 0.6, 25: 0.8, 50: 0.35, 100: 0.55, 200: 0.85, 500: 1.40, 1000: 1.70},
}

# --------------------------------------------------------------------------------------
# Festivals: name -> (dates, zones (None=all), lead_days, overall_uplift, {tag: product_boost}, gift/sweets flag)
# --------------------------------------------------------------------------------------
FESTIVALS = {
    "Diwali": (["2023-11-12", "2024-11-01", "2025-10-20", "2026-11-08"], None, 14, 1.60,
               {"festive_rich": 1.8, "sweets": 2.5, "wedding": 1.2}),
    "Navratri": (["2023-10-15", "2024-10-03", "2025-09-22", "2026-10-11"], ["N", "W", "C"], 3, 1.12,
                 {"vrat": 1.7, "nonveg": 0.55}),
    "Durga Puja": (["2023-10-21", "2024-10-10", "2025-09-29", "2026-10-18"], ["E", "NE"], 10, 1.40,
                   {"puja": 1.9, "nonveg": 1.6, "festive_rich": 1.4}),
    "Dussehra": (["2023-10-24", "2024-10-12", "2025-10-02", "2026-10-20"], None, 3, 1.08, {"festive_rich": 1.2}),
    "Ganesh Chaturthi": (["2024-09-07", "2025-08-27", "2026-09-14"], ["W", "S"], 7, 1.22, {"ganesh": 2.0, "sweets": 1.8}),
    "Onam": (["2024-09-15", "2025-09-05", "2026-08-26"], ["S"], 7, 1.18, {"onam": 1.6, "sweets": 1.4}),
    "Pongal / Makar Sankranti": (["2024-01-15", "2025-01-14", "2026-01-14"], None, 5, 1.12, {"pongal": 1.5, "sweets": 1.3}),
    "Holi": (["2024-03-25", "2025-03-14", "2026-03-04"], ["N", "C", "E", "W"], 7, 1.18, {"holi": 1.8, "thandai": 1.8}),
    "Eid al-Fitr": (["2024-04-10", "2025-03-31", "2026-03-20"], None, 10, 1.28, {"eid": 2.4, "sweets": 1.6, "nonveg": 1.4}),
    "Eid al-Adha": (["2024-06-17", "2025-06-07", "2026-05-27"], None, 7, 1.22, {"eid": 2.0, "nonveg": 2.0}),
    "Raksha Bandhan": (["2024-08-19", "2025-08-09", "2026-08-28"], ["N", "W", "C"], 5, 1.06, {"sweets": 1.4}),
    "Christmas": (["2023-12-25", "2024-12-25", "2025-12-25", "2026-12-25"], None, 7, 1.08, {"xmas": 1.8}),
}
FESTIVAL_REGION_TEXT = {None: "All-India"}

# Wedding seasons (month-day windows) boost HoReCa + wedding-tagged products
WEDDING_WINDOWS = [("11-15", "02-28"), ("04-15", "05-31")]

# Chilli crop failure story (Andhra / Telangana) -> cost spike, constrained supply, low-grade alt supplier
CHILLI_SHORTAGE = ("2024-06-01", "2024-10-15")
CHILLI_CODES = ["RCH", "KCH"]

# MRP revisions: (effective date, codes or None for all, pct)
PRICE_REVISIONS = [("2024-04-01", None, 0.05), ("2024-07-01", CHILLI_CODES, 0.12), ("2025-04-01", None, 0.04),
                   ("2026-04-01", None, 0.03)]

# Raw-material cost index events per base spice: (start, end, peak multiplier)
COST_EVENTS = {
    "RCH": [("2024-05-01", "2025-01-31", 1.38)], "KCH": [("2024-05-01", "2025-01-31", 1.33)],
    "CUM": [("2023-10-01", "2024-05-31", 1.30)], "CUP": [("2023-10-01", "2024-05-31", 1.25)],
    "CAR": [("2025-06-01", "2025-12-31", 1.22)], "BPW": [("2025-09-01", "2026-06-30", 1.15)],
}


def spice_attrs():
    """Index of base spice code -> attributes dict."""
    out = {}
    for (code, name, cat, sub, pop, price100, packs, season, region, tags, org_launch) in SPICES:
        out[code] = dict(code=code, name=name, category=cat, sub_category=sub, pop=pop, price100=price100,
                         packs=packs, season=season, region=region, tags=tags, organic_launch=org_launch)
    return out


def pack_label(g):
    return f"{g}g" if g < 1000 else "1kg"


def build_products():
    """Returns list of product dicts (one per SKU)."""
    rows = []
    for (code, name, cat, sub, pop, price100, packs, season, region, tags, org_launch) in SPICES:
        launch = NEW_SPICE_LAUNCH.get(code, "2018-04-01")
        variants = [(g, False, launch) for g in packs]
        if org_launch:
            variants.append((200, True, org_launch))
        for g, organic, launch_dt in variants:
            mrp = price100 * (g / 100.0) ** 0.92 * (1.4 if organic else 1.0)
            mrp = max(5.0, round(mrp / 5.0) * 5.0 if mrp > 20 else round(mrp))
            sku = f"SR-{code}-{g:04d}" + ("-ORG" if organic else "")
            rows.append(dict(
                sku_id=sku, base_spice_code=code, base_spice=name,
                product_name=f"SpiceRoute {'Organic ' if organic else ''}{name} {pack_label(g)}",
                category=cat, sub_category=sub, pack_size_g=g, is_organic=organic, mrp_inr=float(mrp),
                gst_rate=GST_BY_CATEGORY[cat], std_cost_ratio=COST_RATIO[cat] + (0.06 if organic else 0.0),
                shelf_life_days=SHELF_LIFE[cat], launch_date=launch_dt,
                popularity=pop * (0.18 if organic else 1.0), season=season, region=region, tags=tags,
            ))
    return rows
