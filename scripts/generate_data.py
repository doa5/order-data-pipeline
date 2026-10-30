# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas>=3.0.6",
# ]
# ///
"""Generate the three sample input sources for the pipeline.

Creates deliberately messy data in ../input: orders.csv (flat file),
crm_database.db (SQLite) and delivery_logs.json (document-style JSON).

Run:  uv run scripts/generate_data.py
"""

import json
import sqlite3
from pathlib import Path

import pandas as pd

INPUT_DIR = Path(__file__).resolve().parent.parent / "input"


# =====================================================================
# SOURCE 1: Flat File (orders.csv) - 21 Records
# =====================================================================
def create_orders():
    orders_data = {
        "order_id": [f"ORD_{1000+i}" for i in range(1, 22)],
        "customer_id": [
            "CUST_501", "CUST_502", "CUST_503", "CUST_501", "CUST_504",
            "CUST_505", "CUST_502", "CUST_999", "CUST_503", "CUST_504", # CUST_999 = Unmatched Key
            "CUST_506", "CUST_507", "CUST_501", "CUST_508", "CUST_505",
            "CUST_502", "CUST_506", "CUST_507", "CUST_508", "CUST_501",
            "CUST_502"
        ],
        "order_date": [
            "2026-10-01", "01/10/2026", "2026-10-02", "2026-10-03", "04/10/2026",
            "2026-10-05", "2026-10-05", "06/10/2026", "2026-10-07", "2026-10-08",
            "08/10/2026", "2026-10-09", "2026-10-10", "10/10/2026", "2026-10-11",
            "2026-10-11", "12/10/2026", "2026-10-12", "2026-10-13", "13/10/2026",
            "2026-10-14"
        ],
        "order_value": [
            "£145.50", "£45.00", "£210.00", "£85.20", "£12.50",
            "£320.00", "£99.99", "£150.00", "£18.00", "£450.00",
            "£65.00", "£180.00", "£520.00", "£30.00", "£115.00",
            "£250.00", "£89.50", "£410.00", "£15.00", "£300.00",
            "£35.00" # ORD_1021 = low-value failed order, stays below the P2 threshold
        ],
        "shipping_postcode": [
            " M1 2WD ", "LS1 4AP", " B1 1AA ", "M1 2WD", " G1 3SL ",
            "SW1A 1AA", " LS1 4AP ", "BD1 1HA", "B1 1AA", " M1 2WD ",
            " M1 2WD ", "LS1 4AP", " B1 1AA ", "M1 2WD", " G1 3SL ",
            "SW1A 1AA", " LS1 4AP ", "BD1 1HA", "B1 1AA", " M1 2WD ",
            "LS1 4AP"
        ]
    }

    pd.DataFrame(orders_data).to_csv(INPUT_DIR / "orders.csv", index=False)
    print("Source 1 created: 'orders.csv' (21 records)")


# =====================================================================
# SOURCE 2: Tabular SQL (crm_database.db / customers) - 8 Master Customers
# =====================================================================
def create_crm():
    conn = sqlite3.connect(INPUT_DIR / "crm_database.db")
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS customers")
    cursor.execute("""
        CREATE TABLE customers (
            customer_id TEXT PRIMARY KEY,
            full_name TEXT,
            email TEXT,
            account_tier TEXT NOT NULL,
            phone TEXT
        )
    """)

    customers_tuples = [
        ("CUST_501", "Alice Smith", " ALICE.SMITH@Email.COM ", "VIP", "07700900001"),
        ("CUST_502", "Bob Jones", "bob.jones@provider.co.uk", "Standard", "07700900002"),
        ("CUST_503", "Charlie Brown", "charlie.b@domain.org", "gold", "07700900003"), # Lowercase gold
        ("CUST_504", "Diana Prince", "diana@wonder.com", " standard ", "07700900004"), # Padded lowercase tier
        ("CUST_505", "Evan Wright", "evan.wright@tech.com", "VIP", "07700900005"),
        ("CUST_506", "Fiona Gallagher", "fiona@chat.co.uk", "Standard", "07700900006"),
        ("CUST_507", "George Clark", " GEORGE.C@WEB.COM ", "VIP", "07700900007"),
        ("CUST_508", "Hannah Abbott", "hannah@herbs.org", "Gold", "07700900008")
    ]

    cursor.executemany("INSERT INTO customers VALUES (?, ?, ?, ?, ?)", customers_tuples)
    conn.commit()
    conn.close()
    print("Source 2 created: 'crm_database.db' (8 customer profiles)")


# =====================================================================
# SOURCE 3: NoSQL JSON (delivery_logs.json) - 21 Records + 1 Orphan
# =====================================================================
def create_logs():
    delivery_logs_data = [
        {"order_id": "ORD_1001", "carrier": "DPD", "status": "Failed Delivery", "tracking_details": {"last_checkpoint": "Manchester Hub", "delivery_notes": "Address incomplete"}},
        {"order_id": "ORD_1002", "carrier": "Royal Mail", "status": "Delivered", "tracking_details": {"last_checkpoint": "Leeds Depot", "delivery_notes": "Left in safe place"}},
        {"order_id": "ORD_1003", "carrier": "DHL", "status": "Failed Delivery", "tracking_details": {"last_checkpoint": "Birmingham South", "delivery_notes": "Refused package - damaged"}},
        {"order_id": "ORD_1004", "carrier": "DPD", "status": None, "tracking_details": {"last_checkpoint": "Manchester Hub", "delivery_notes": "Awaiting scan"}},
        {"order_id": "ORD_1005", "carrier": "Evri", "status": "In Transit", "tracking_details": {"last_checkpoint": "Glasgow Central", "delivery_notes": "Out for delivery"}},
        {"order_id": "ORD_1006", "carrier": "DPD", "status": "Failed Delivery", "tracking_details": {"last_checkpoint": "London North Hub", "delivery_notes": "Gate locked"}},
        {"order_id": "ORD_1007", "carrier": "Royal Mail", "status": "Delivered", "tracking_details": {"last_checkpoint": "Leeds Depot", "delivery_notes": "Handed to resident"}},
        {"order_id": "ORD_1008", "carrier": "DHL", "status": "Delayed", "tracking_details": {"last_checkpoint": "Bradford Hub", "delivery_notes": "Vehicle breakdown"}},
        {"order_id": "ORD_1009", "carrier": "Evri", "status": "In Transit", "tracking_details": {"last_checkpoint": "Birmingham South", "delivery_notes": "Arrived at facility"}},
        {"order_id": "ORD_1010", "carrier": "DPD", "status": "Failed Delivery", "tracking_details": {"last_checkpoint": "Manchester Hub", "delivery_notes": "Security access required"}},
        {"order_id": "ORD_1011", "carrier": "Royal Mail", "status": "Delivered", "tracking_details": {"last_checkpoint": "Bristol Hub", "delivery_notes": "Signed for"}},
        {"order_id": "ORD_1012", "carrier": "DHL", "status": "In Transit", "tracking_details": {"last_checkpoint": "London Hub", "delivery_notes": "On route"}},
        {"order_id": "ORD_1013", "carrier": "DPD", "status": "Failed Delivery", "tracking_details": {"last_checkpoint": "Manchester Hub", "delivery_notes": "No answer at door"}},
        {"order_id": "ORD_1014", "carrier": "Evri", "status": "Delivered", "tracking_details": {"last_checkpoint": "Cardiff Depot", "delivery_notes": "Safe location"}},
        {"order_id": "ORD_1015", "carrier": "Royal Mail", "status": "Delayed", "tracking_details": {"last_checkpoint": "Edinburgh Central", "delivery_notes": "Weather delay"}},
        {"order_id": "ORD_1016", "carrier": "DHL", "status": "Delivered", "tracking_details": {"last_checkpoint": "London Hub", "delivery_notes": "Delivered"}},
        {"order_id": "ORD_1017", "carrier": "DPD", "status": "In Transit", "tracking_details": {"last_checkpoint": "Sheffield Depot", "delivery_notes": "In vehicle"}},
        {"order_id": "ORD_1018", "carrier": "Evri", "status": "Failed Delivery", "tracking_details": {"last_checkpoint": "Newcastle Hub", "delivery_notes": "Incorrect postal code"}},
        {"order_id": "ORD_1019", "carrier": "Royal Mail", "status": "Delivered", "tracking_details": {"last_checkpoint": "Belfast Hub", "delivery_notes": "Handed to neighbor"}},
        {"order_id": "ORD_1020", "carrier": "DPD", "status": "Failed Delivery", "tracking_details": {"last_checkpoint": "Manchester Hub", "delivery_notes": "Refused delivery"}},
        {"order_id": "ORD_1021", "carrier": "Royal Mail", "status": "Failed Delivery", "tracking_details": {"last_checkpoint": "Leeds Depot", "delivery_notes": "Card left, redeliver"}},
        {"order_id": "ORD_9999", "carrier": "DHL", "status": "In Transit", "tracking_details": {"last_checkpoint": "Unknown", "delivery_notes": "Orphaned API log"}} # Orphan key
    ]

    with open(INPUT_DIR / "delivery_logs.json", "w", encoding="utf-8") as f:
        json.dump(delivery_logs_data, f, indent=2)

    print("Source 3 created: 'delivery_logs.json' (22 logs including 1 orphan)")


def main():
    INPUT_DIR.mkdir(exist_ok=True)
    create_orders()
    create_crm()
    create_logs()


if __name__ == "__main__":
    main()
