# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "pandas>=3.0.6",
# ]
# ///
import json
import sqlite3
from pathlib import Path
import pandas as pd 

INPUT_DIR = Path(__file__).resolve().parent.parent / "input"

# PREVIEW FOR DELIVERABLE 2 SCREENSHOTS

# 1. Preview Raw CSV
print("--- SOURCE 1: RAW ORDERS CSV ---")
print(pd.read_csv(INPUT_DIR / "orders.csv").head(4))

# 2. Preview Raw SQL
print("\n--- SOURCE 2: RAW CRM SQL DB ---")
conn = sqlite3.connect(INPUT_DIR / "crm_database.db")
print(pd.read_sql_query("SELECT * FROM customers", conn).head(4))
conn.close()

# 3. Preview Raw JSON
print("\n--- SOURCE 3: RAW NOSQL JSON (UNNESTED PREVIEW) ---")
with open(INPUT_DIR / "delivery_logs.json") as f:
    raw_json = json.load(f)
print(pd.json_normalize(raw_json).head(4))