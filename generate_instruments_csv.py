"""
generate_instruments_csv.py

Generates valid_instruments.csv from Zerodha Kite instruments API

CSV STRUCTURE (UPDATED):
symbol, tradingsymbol, strike, option_type, expiry_date,
tick_size, lot_size, exchange, instrument_type
"""

import csv
import json
import logging
import sys
import io
from datetime import datetime, timedelta
from kiteconnect import KiteConnect

# Fix Windows encoding
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Set up logging with timestamped filename
log_filename = f"generate_instruments_csv_{datetime.now().strftime('%d%b%y_%I%M%S_%p').upper()}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - INSTRUMENTS - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(
            io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
            if sys.platform == 'win32' else sys.stdout
        )
    ]
)
logging.info(f"[LOG] Writing to: {log_filename}")

# ================= CONFIG =================

KITE_CONFIG_FILE = "kite_config.json"
OUTPUT_FILE = "valid_instruments.csv"

EXCHANGE_CONFIG = {
    "NFO": {
        "types": ["CE", "PE"],
        "expiry_days": 45  # Increased to capture all Feb 2026 expiries (BANKNIFTY/FINNIFTY/MIDCPNIFTY monthly = Feb 24)
    },
    "BFO": {
        "types": ["CE", "PE"],
        "expiry_days": 45  # Increased to capture all Feb 2026 expiries
    },
    "MCX": {
        "types": ["FUT", "CE", "PE"],   # FUT + OPTIONS
        "expiry_days": 90
    },
    "CDS": {
        "types": ["FUT", "OPT"],
        "expiry_days": 60
    }
}

# ================= KITE INIT =================

with open(KITE_CONFIG_FILE, "r") as f:
    cfg = json.load(f)

kite = KiteConnect(api_key=cfg["api_key"])
kite.set_access_token(cfg["access_token"])

today = datetime.now().date()

print("\n" + "=" * 80)
print("GENERATING valid_instruments.csv (STRUCTURE PRESERVED)")
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

        # Normalize expiry (datetime OR date)
        if hasattr(expiry, "date"):
            expiry_date = expiry.date()
        else:
            expiry_date = expiry

        if not (today <= expiry_date <= expiry_limit):
            continue
        # Add after line 88 (after adding to rows):
        if added < 5:  # Print first few for each exchange
            print(f"  Sample: {inst.get('name')} | {inst.get('tradingsymbol')}")
        # 2️⃣ MCX option sanity (strike must exist)
        if exchange == "MCX" and inst_type in ["CE", "PE"]:
            strike = inst.get("strike")
            if not strike or strike <= 0:
                continue

        # 3️⃣ WRITE ROW — INCLUDES TRADINGSYMBOL
        rows.append({
            "symbol": inst.get("name"),
            "tradingsymbol": inst.get("tradingsymbol"),  # CRITICAL: Added for order placement
            "strike": inst.get("strike", 0),
            "option_type": inst_type,
            "expiry_date": expiry_date.strftime("%Y-%m-%d"),
            "tick_size": inst.get("tick_size"),
            "lot_size": inst.get("lot_size"),
            "exchange": exchange,
            "instrument_type": inst_type
        })

        added += 1

    print(f"[{exchange}] Added {added} instruments")
    
    # Show unique index names found
    if exchange in ["NFO", "BFO"]:
        unique_names = set(row["symbol"] for row in rows if row["exchange"] == exchange)
        print(f"[{exchange}] Unique indices: {', '.join(sorted(unique_names))}")

# ================= WRITE CSV =================

fieldnames = [
    "symbol",
    "tradingsymbol",  # CRITICAL: Added for order placement
    "strike",
    "option_type",
    "expiry_date",
    "tick_size",
    "lot_size",
    "exchange",
    "instrument_type"
]

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

# ================= SUMMARY =================

print("\n✅ CSV GENERATED SUCCESSFULLY")
print(f"Output file: {OUTPUT_FILE}")
print(f"Total instruments: {len(rows)}")

print("\n" + "=" * 80)
print("CSV STRUCTURE UNCHANGED — ONLY INSTRUMENT COVERAGE IMPROVED")
print("MCX FUT + OPTIONS (INCLUDING NATURALGAS) INCLUDED")
print("=" * 80)
