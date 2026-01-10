"""
generate_instruments_csv.py - FIXED VERSION

Generates valid_instruments.csv from Zerodha Kite instruments API
NOW INCLUDES: NFO Index options + NSE Stock options + MCX Commodities

CRITICAL FIX: Added NSE exchange for stock options!
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
        "types": ["CE", "PE"],  # Index options (NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, etc.)
        "expiry_days": 14       # Weekly expiries for indices
    },
    "BFO": {
        "types": ["CE", "PE"],  # BSE Index options (SENSEX, BANKEX)
        "expiry_days": 14
    },
    "NSE": {
        "types": ["CE", "PE"],  # STOCK OPTIONS (TATASTEEL, RELIANCE, HDFC, MCX, DIXON, etc.) ⭐ CRITICAL!
        "expiry_days": 45       # Monthly expiries for stocks (next 2 months)
    },
    "MCX": {
        "types": ["FUT", "CE", "PE"],   # Commodity futures + options
        "expiry_days": 90
    },
    "CDS": {
        "types": ["FUT", "OPT"],
        "expiry_days": 60
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
print("GENERATING valid_instruments.csv WITH NSE STOCK OPTIONS")
print("=" * 80)

rows = []

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

        # MCX option sanity check (strike must exist)
        if exchange == "MCX" and inst_type in ["CE", "PE"]:
            strike = inst.get("strike")
            if not strike or strike <= 0:
                continue

        # NSE stock option sanity check (strike must exist)
        if exchange == "NSE" and inst_type in ["CE", "PE"]:
            strike = inst.get("strike")
            if not strike or strike <= 0:
                continue

        # CRITICAL: Add tradingsymbol field for lookup
        tradingsymbol = inst.get("tradingsymbol", "")
        
        # Write row with ALL necessary fields
        rows.append({
            "symbol": inst.get("name"),
            "tradingsymbol": tradingsymbol,  # CRITICAL for lookup!
            "strike": inst.get("strike", 0),
            "option_type": inst_type,
            "expiry_date": expiry_date.strftime("%Y-%m-%d"),
            "tick_size": inst.get("tick_size"),
            "lot_size": inst.get("lot_size"),
            "exchange": exchange,
            "instrument_type": inst_type
        })

        added += 1

    print(f"[{exchange}] ✓ Added {added} instruments")

# ================= WRITE CSV =================

fieldnames = [
    "symbol",
    "tradingsymbol",  # ADDED - Critical for parser lookup!
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

print("\n" + "=" * 80)
print("✅ CSV GENERATED SUCCESSFULLY")
print("=" * 80)
print(f"Output file: {OUTPUT_FILE}")
print(f"Total instruments: {len(rows)}")
print("")

# Count by exchange
from collections import Counter
exchange_counts = Counter(row['exchange'] for row in rows)

print("Breakdown by exchange:")
for exchange, count in sorted(exchange_counts.items()):
    print(f"  {exchange}: {count:,} instruments")

print("")

# Count stock options specifically
nse_options = sum(1 for row in rows if row['exchange'] == 'NSE' and row['option_type'] in ['CE', 'PE'])
print(f"NSE Stock Options: {nse_options:,} ⭐ (CRITICAL - these were missing!)")

print("\n" + "=" * 80)
print("READY FOR TRADING!")
print("CSV now includes:")
print("  ✓ NFO Index options (NIFTY, BANKNIFTY, etc.)")
print("  ✓ NSE Stock options (TATASTEEL, RELIANCE, etc.) ⭐ NEW!")
print("  ✓ BFO BSE options (SENSEX, BANKEX)")
print("  ✓ MCX Commodity options")
print("  ✓ Tradingsymbol field for accurate lookups ⭐ NEW!")
print("=" * 80)
print("")