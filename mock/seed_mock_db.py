"""Seed a mock plant database with Dynamics-like tables.

Creates a SQLite database that mimics a biodiesel/ethanol plant's ERP system
with both relevant business tables and noise/system tables.

Usage:
    python mock/seed_mock_db.py [--output mock_plant.db]
"""

import sqlite3
import random
import argparse
from datetime import datetime, timedelta


def create_mock_schema(db_path: str = "mock_plant.db"):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # --- Relevant tables (what the agent should find) ---

    c.execute("""
        CREATE TABLE IF NOT EXISTS INVENTTRANS (
            RECID INTEGER PRIMARY KEY,
            ITEMID TEXT,
            QTY REAL,
            COSTAMOUNT REAL,
            TRANSDATE TEXT,
            INVENTDIMID TEXT,
            STATUSISSUE INTEGER,
            DATAAREAID TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS PRODTABLE (
            RECID INTEGER PRIMARY KEY,
            PRODID TEXT,
            ITEMID TEXT,
            QTYSCHED REAL,
            QTYREPORT REAL,
            PRODSTATUS INTEGER,
            STARTDATE TEXT,
            ENDDATE TEXT,
            DATAAREAID TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS CUSTINVOICEJOUR (
            RECID INTEGER PRIMARY KEY,
            INVOICEID TEXT,
            INVOICEDATE TEXT,
            INVOICEAMOUNT REAL,
            CUSTACCOUNT TEXT,
            DATAAREAID TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS ZRIN_GENERATION (
            RECID INTEGER PRIMARY KEY,
            BATCHID TEXT,
            RIN_CATEGORY TEXT,
            GALLONS_PRODUCED REAL,
            RINS_GENERATED REAL,
            GEN_DATE TEXT,
            D_CODE TEXT,
            DATAAREAID TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS INVENTTABLE (
            ITEMID TEXT PRIMARY KEY,
            ITEMNAME TEXT,
            ITEMGROUPID TEXT,
            UNITID TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS VENDTABLE (
            ACCOUNTNUM TEXT PRIMARY KEY,
            NAME TEXT,
            VENDGROUP TEXT,
            CURRENCY TEXT,
            DATAAREAID TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS PURCHLINE (
            RECID INTEGER PRIMARY KEY,
            PURCHID TEXT,
            ITEMID TEXT,
            QTY REAL,
            PURCHPRICE REAL,
            LINEAMOUNT REAL,
            DELIVERYDATE TEXT,
            VENDACCOUNT TEXT,
            DATAAREAID TEXT
        )
    """)

    # --- Noise tables (what the agent should ignore) ---

    c.execute("""
        CREATE TABLE IF NOT EXISTS SYSUSERLOG (
            RECID INTEGER PRIMARY KEY,
            USERID TEXT,
            LOGINDATETIME TEXT,
            SESSIONID TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS BATCHJOB (
            RECID INTEGER PRIMARY KEY,
            JOBDESCRIPTION TEXT,
            STATUS INTEGER,
            STARTDATETIME TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS DOCUREF (
            RECID INTEGER PRIMARY KEY,
            REFCOMPANYID TEXT,
            REFTABLEID INTEGER,
            NOTES TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS NUMBERSEQUENCETABLE (
            RECID INTEGER PRIMARY KEY,
            NUMBERSEQUENCE TEXT,
            NEXTREC INTEGER
        )
    """)

    # Empty table (should be filtered by min_rows)
    c.execute("""
        CREATE TABLE IF NOT EXISTS EMPTY_STAGING (
            RECID INTEGER PRIMARY KEY,
            DATA TEXT
        )
    """)

    # --- Seed with sample data ---
    _seed_data(c)
    conn.commit()
    conn.close()
    print(f"Mock DB created at {db_path}")
    print(f"  7 business tables + 4 noise tables + 1 empty table = 12 total")


def _seed_data(c):
    # Item master
    items = [
        ("SOYOIL", "Soybean Oil", "FEEDSTK", "GAL"),
        ("CORNOIL", "Corn Oil", "FEEDSTK", "GAL"),
        ("METHANOL", "Methanol", "CHEM", "GAL"),
        ("BD100", "Biodiesel B100", "PRODUCT", "GAL"),
        ("BD20", "Biodiesel B20 Blend", "PRODUCT", "GAL"),
        ("GLYCERIN", "Glycerin Byproduct", "BYPROD", "LB"),
    ]
    c.executemany("INSERT OR IGNORE INTO INVENTTABLE VALUES (?,?,?,?)", items)

    # Vendors
    vendors = [
        ("V001", "Midwest Soy Suppliers", "FEEDSTK", "USD", "BIODSL"),
        ("V002", "ChemCorp International", "CHEMICAL", "USD", "BIODSL"),
        ("V003", "Transport Logistics LLC", "FREIGHT", "USD", "BIODSL"),
    ]
    c.executemany("INSERT OR IGNORE INTO VENDTABLE VALUES (?,?,?,?,?)", vendors)

    item_ids = [i[0] for i in items]

    # Inventory transactions — last 90 days
    for i in range(200):
        days_ago = random.randint(0, 90)
        date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        item = random.choice(item_ids)
        c.execute(
            "INSERT INTO INVENTTRANS (ITEMID, QTY, COSTAMOUNT, TRANSDATE, INVENTDIMID, STATUSISSUE, DATAAREAID) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                item,
                round(random.uniform(100, 50000), 2),
                round(random.uniform(500, 200000), 2),
                date,
                f"DIM_{random.randint(1,5)}",
                random.choice([0, 1, 2]),
                "BIODSL",
            ),
        )

    # Production orders
    for i in range(30):
        days_ago = random.randint(0, 90)
        start = datetime.now() - timedelta(days=days_ago)
        end = start + timedelta(days=random.randint(1, 5))
        c.execute(
            "INSERT INTO PRODTABLE (PRODID, ITEMID, QTYSCHED, QTYREPORT, PRODSTATUS, STARTDATE, ENDDATE, DATAAREAID) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (
                f"PROD-{i:04d}",
                random.choice(["BD100", "BD20"]),
                round(random.uniform(5000, 50000), 2),
                round(random.uniform(4000, 50000), 2),
                random.choice([1, 2, 3, 4]),
                start.strftime("%Y-%m-%d"),
                end.strftime("%Y-%m-%d"),
                "BIODSL",
            ),
        )

    # Customer invoices
    for i in range(40):
        days_ago = random.randint(0, 90)
        date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        c.execute(
            "INSERT INTO CUSTINVOICEJOUR (INVOICEID, INVOICEDATE, INVOICEAMOUNT, CUSTACCOUNT, DATAAREAID) "
            "VALUES (?,?,?,?,?)",
            (
                f"INV-{i:05d}",
                date,
                round(random.uniform(5000, 500000), 2),
                f"CUST-{random.randint(100,110)}",
                "BIODSL",
            ),
        )

    # RIN generation records
    for i in range(50):
        days_ago = random.randint(0, 90)
        date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        gallons = round(random.uniform(10000, 100000), 2)
        c.execute(
            "INSERT INTO ZRIN_GENERATION (BATCHID, RIN_CATEGORY, GALLONS_PRODUCED, RINS_GENERATED, GEN_DATE, D_CODE, DATAAREAID) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                f"BATCH_{i:04d}",
                random.choice(["D4", "D5"]),
                gallons,
                round(gallons * 1.5, 2),
                date,
                random.choice(["D4", "D5"]),
                "BIODSL",
            ),
        )

    # Purchase lines
    for i in range(60):
        days_ago = random.randint(0, 90)
        date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        qty = round(random.uniform(1000, 30000), 2)
        price = round(random.uniform(1.5, 8.0), 4)
        c.execute(
            "INSERT INTO PURCHLINE (PURCHID, ITEMID, QTY, PURCHPRICE, LINEAMOUNT, DELIVERYDATE, VENDACCOUNT, DATAAREAID) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (
                f"PO-{i:04d}",
                random.choice(["SOYOIL", "CORNOIL", "METHANOL"]),
                qty,
                price,
                round(qty * price, 2),
                date,
                random.choice(["V001", "V002", "V003"]),
                "BIODSL",
            ),
        )

    # Noise table data (minimal)
    for i in range(5):
        c.execute(
            "INSERT INTO SYSUSERLOG (USERID, LOGINDATETIME, SESSIONID) VALUES (?,?,?)",
            (f"user{i}", datetime.now().isoformat(), f"sess_{i}"),
        )

    for i in range(3):
        c.execute(
            "INSERT INTO NUMBERSEQUENCETABLE (NUMBERSEQUENCE, NEXTREC) VALUES (?,?)",
            (f"SEQ_{i}", random.randint(1, 999)),
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed mock ERP database")
    parser.add_argument("--output", default="mock_plant.db", help="Output SQLite file path")
    args = parser.parse_args()
    create_mock_schema(args.output)
