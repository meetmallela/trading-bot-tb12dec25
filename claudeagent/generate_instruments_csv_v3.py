"""
generate_instruments_csv.py - CORRECTED VERSION

CRITICAL FIX: Stock options are in NFO exchange (not NSE)!
NFO contains BOTH index options AND stock options

This script separates them properly:
- Index options: NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, etc.
- Stock options: TATASTEEL, RELIANCE, HDFC, etc.
"""

import csv
import json
from datetime import datetime, timedelta
from kiteconnect import KiteConnect

# ================= CONFIG =================

KITE_CONFIG_FILE = "kite_config.json"
OUTPUT_FILE = "valid_instruments.csv"

# List of index symbols (NOT stock options)
INDEX_SYMBOLS = [
    'NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY',  # NSE indices
    'SENSEX', 'BANKEX'  # BSE indices
]

EXCHANGE_CONFIG = {
    "NFO": {
        "types": ["CE", "PE", "FUT"],  # NFO has BOTH index + stock options + futures
        "expiry_days": 45,              # Get 45 days (covers weekly + monthly)
        "separate_stocks": True         # Flag to separate stock options from indices
    },
    "BFO": {
        "types": ["CE", "PE"],  # BSE Index options (SENSEX, BANKEX)
        "expiry_days": 14,
        "separate_stocks": False
    },
    "MCX": {
        "types": ["FUT", "CE", "PE"],   # Commodity futures + options
        "expiry_days": 90,
        "separate_stocks": False
    },
    "CDS": {
        "types": ["FUT", "OPT"],
        "expiry_days": 60,
        "separate_stocks": False
    }
}

# ================= KITE INIT =================

print("\n" + "=" * 80)
print("LOADING KITE CONFIG...")
print("=" * 80)

with open(KITE_CONFIG_FILE, "r") as f:
    cfg = json.load(f)

kite = KiteConnect(api_key=cfg["api_key"])
kite.set_access_token(cfg["access_token"])

today = datetime.now().date()

print(f"✓ Connected to Kite")
print(f"✓ Date: {today}")

print("\n" + "=" * 80)
print("GENERATING valid_instruments.csv")
print("STOCK OPTIONS ARE IN NFO (not NSE)!")
print("=" * 80)

rows = []
nfo_index_count = 0
nfo_stock_count = 0

# ================= MAIN LOOP =================

for exchange, ex_cfg in EXCHANGE_CONFIG.items():
    print(f"\n[{exchange}] Fetching instruments...")
    try:
        instruments = kite.instruments(exchange)
        print(f"[{exchange}] Found {len(instruments)} total instruments")
    except Exception as e:
        print(f"[{exchange}] ERROR fetching instruments: {e}")
        continue

    expiry_limit = today + timedelta(days=ex_cfg["expiry_days"])
    added = 0

    for inst in instruments:
        inst_type = inst.get("instrument_type")
        symbol = inst.get("name", "")

        # Filter by instrument type
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

        # Filter by expiry date range
        if not (today <= expiry_date <= expiry_limit):
            continue

        # Strike sanity check for options
        if inst_type in ["CE", "PE"]:
            strike = inst.get("strike")
            if not strike or strike <= 0:
                continue

        # CRITICAL: Determine if this is a stock option or index option
        is_index = symbol in INDEX_SYMBOLS
        
        # For NFO, track separately
        if exchange == "NFO":
            if is_index:
                nfo_index_count += 1
            else:
                nfo_stock_count += 1

        # Get tradingsymbol
        tradingsymbol = inst.get("tradingsymbol", "")
        
        # Write row with ALL necessary fields
        rows.append({
            "symbol": symbol,
            "tradingsymbol": tradingsymbol,
            "strike": inst.get("strike", 0),
            "option_type": inst_type,
            "expiry_date": expiry_date.strftime("%Y-%m-%d"),
            "tick_size": inst.get("tick_size"),
            "lot_size": inst.get("lot_size"),
            "exchange": exchange,
            "instrument_type": inst_type,
            "is_index": "YES" if is_index else "NO"  # Helper field
        })

        added += 1

    print(f"[{exchange}] ✓ Added {added} instruments")
    
    # Special breakdown for NFO
    if exchange == "NFO":
        print(f"  └─ Index options: {nfo_index_count}")
        print(f"  └─ Stock options: {nfo_stock_count} ⭐")

# ================= WRITE CSV =================

fieldnames = [
    "symbol",
    "tradingsymbol",
    "strike",
    "option_type",
    "expiry_date",
    "tick_size",
    "lot_size",
    "exchange",
    "instrument_type",
    "is_index"
]

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

# ================= SUMMARY =================

print("\n" + "=" * 80)
print("✅ CSV GENERATED SUCCESSFULLY")
print("=" * 80)
print(f"Output file: {OUTPUT_FILE}")
print(f"Total instruments: {len(rows):,}")
print("")

# Count by exchange
from collections import Counter
exchange_counts = Counter(row['exchange'] for row in rows)

print("Breakdown by exchange:")
for exchange, count in sorted(exchange_counts.items()):
    print(f"  {exchange}: {count:,} instruments")

print("")

# Count stock options specifically (NFO non-index)
stock_options = sum(1 for row in rows if row['is_index'] == 'NO' and row['option_type'] in ['CE', 'PE'])
index_options = sum(1 for row in rows if row['is_index'] == 'YES' and row['option_type'] in ['CE', 'PE'])

print(f"Index Options (NIFTY, BANKNIFTY, etc.): {index_options:,}")
print(f"Stock Options (TATASTEEL, RELIANCE, etc.): {stock_options:,} ⭐")

# Show some stock option examples
print("\nSample Stock Options in CSV:")
stock_samples = [row for row in rows if row['is_index'] == 'NO' and row['option_type'] in ['CE', 'PE']][:10]
for s in stock_samples:
    print(f"  {s['tradingsymbol']:25s} {s['symbol']:15s} Strike: {s['strike']}")

print("\n" + "=" * 80)
print("READY FOR TRADING!")
print("CSV now includes:")
print("  ✓ NFO Index options (NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY)")
print(f"  ✓ NFO Stock options ({stock_options:,} instruments) ⭐ FIXED!")
print("  ✓ BFO BSE options (SENSEX, BANKEX)")
print("  ✓ MCX Commodity options")
print("  ✓ Tradingsymbol field for accurate lookups")
print("  ✓ is_index field to distinguish stock vs index options")
print("=" * 80)
print("")
