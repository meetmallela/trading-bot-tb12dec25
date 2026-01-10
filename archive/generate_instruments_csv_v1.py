"""
generate_instruments_csv.py

Generates valid_instruments.csv from Zerodha Kite instruments API

SUPPORTED:
- NFO index options (NIFTY, BANKNIFTY, FINNIFTY, etc.)
- MCX futures
- MCX options (CE / PE) where available (e.g., NATURALGAS, CRUDEOIL)
- CDS futures / options (basic support)

AUTHORITATIVE, EXCHANGE-AWARE, FUTURE-PROOF
"""

import csv
import json
from datetime import datetime, timedelta
from kiteconnect import KiteConnect

# ================= CONFIG =================

KITE_CONFIG_FILE = "kite_config.json"
OUTPUT_FILE = "valid_instruments.csv"

EXCHANGE_CONFIG = {
    "NFO": {
        "types": ["CE", "PE"],
        "expiry_days": 14,
        "category": "INDEX_OPTION"
    },
    "BFO": {
        "types": ["CE", "PE"],
        "expiry_days": 14,
        "category": "INDEX_OPTION"
    },
    "MCX": {
        "types": ["FUT", "CE", "PE"],   # FUT + OPTIONS
        "expiry_days": 90,
        "category": "COMMODITY"
    },
    "CDS": {
        "types": ["FUT", "OPT"],
        "expiry_days": 60,
        "category": "CURRENCY"
    }
}

# ================= KITE INIT =================

with open(KITE_CONFIG_FILE, "r") as f:
    cfg = json.load(f)

kite = KiteConnect(api_key=cfg["api_key"])
kite.set_access_token(cfg["access_token"])

today = datetime.now().date()

print("\n" + "=" * 80)
print("GENERATING valid_instruments.csv (FINAL, MCX OPTIONS ENABLED)")
print("=" * 80)

rows = []

# ================= MAIN LOOP =================

for exchange, ex_cfg in EXCHANGE_CONFIG.items():
    print(f"\n[{exchange}] Fetching instruments...")
    try:
        instruments = kite.instruments(exchange)
    except Exception as e:
        print(f"[{exchange}] ERROR fetching instruments: {e}")
        continue

    expiry_limit = today + timedelta(days=ex_cfg["expiry_days"])
    added = 0

    for inst in instruments:
        inst_type = inst.get("instrument_type")

        # 1️⃣ Instrument type filter
        if inst_type not in ex_cfg["types"]:
            continue

        expiry = inst.get("expiry")
        if not expiry:
            continue

        # 2️⃣ Normalize expiry (datetime OR date)
        if hasattr(expiry, "date"):
            expiry_date = expiry.date()
        else:
            expiry_date = expiry

        if not (today <= expiry_date <= expiry_limit):
            continue

        # 3️⃣ MCX option sanity
        if exchange == "MCX" and inst_type in ["CE", "PE"]:
            strike = inst.get("strike")
            if not strike or strike <= 0:
                continue

        # 4️⃣ Normalize fields
        rows.append({
            "symbol": inst.get("name"),
            "strike": inst.get("strike", 0),
            "option_type": inst_type,
            "expiry_date": expiry_date.strftime("%Y-%m-%d"),
            "tick_size": inst.get("tick_size"),
            "lot_size": inst.get("lot_size"),
            "exchange": exchange,
            "category": ex_cfg["category"],
            "tradingsymbol": inst.get("tradingsymbol")
        })

        added += 1

    print(f"[{exchange}] Added {added} instruments")

# ================= WRITE CSV =================

fieldnames = [
    "symbol",
    "strike",
    "option_type",
    "expiry_date",
    "tick_size",
    "lot_size",
    "exchange",
    "category",
    "tradingsymbol"
]

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

# ================= SUMMARY =================

print("\n✅ CSV GENERATED SUCCESSFULLY")
print(f"Output file: {OUTPUT_FILE}")
print(f"Total instruments: {len(rows)}")

summary = {}
for r in rows:
    key = f"{r['exchange']}:{r['symbol']}:{r['option_type']}"
    summary[key] = summary.get(key, 0) + 1

print("\nInstrument summary (top 20):")
for k in list(sorted(summary.items()))[:20]:
    print(f"{k[0]} → {k[1]}")

print("\n" + "=" * 80)
print("MCX OPTIONS (INCLUDING NATURALGAS) ARE NOW INCLUDED")
print("=" * 80)
