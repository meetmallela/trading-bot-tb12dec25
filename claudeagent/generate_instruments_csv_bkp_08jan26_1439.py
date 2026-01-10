"""
generate_instruments_csv_complete.py
Gets ALL NFO options (no date restrictions)
This ensures we get all tradable stock options
"""

import csv
import json
from datetime import datetime
from kiteconnect import KiteConnect

KITE_CONFIG_FILE = "kite_config.json"
OUTPUT_FILE = "valid_instruments.csv"

INDEX_SYMBOLS = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX']

with open(KITE_CONFIG_FILE, "r") as f:
    cfg = json.load(f)

kite = KiteConnect(api_key=cfg["api_key"])
kite.set_access_token(cfg["access_token"])

today = datetime.now().date()

print("="*80)
print("FETCHING ALL NFO OPTIONS (No Date Filter)")
print("="*80)

# Get ALL NFO instruments
instruments = kite.instruments("NFO")
print(f"Total NFO instruments: {len(instruments)}")

rows = []
index_count = 0
stock_count = 0

for inst in instruments:
    inst_type = inst.get("instrument_type")
    symbol = inst.get("name", "")
    
    # Only options (CE/PE)
    if inst_type not in ["CE", "PE"]:
        continue
    
    # Check expiry exists
    expiry = inst.get("expiry")
    if not expiry:
        continue
        
    if hasattr(expiry, "date"):
        expiry_date = expiry.date()
    else:
        expiry_date = expiry
    
    # CRITICAL: Include ALL options (past and future)
    # No date filter - get everything!
    
    # Check strike
    strike = inst.get("strike")
    if not strike or strike <= 0:
        continue
    
    # Categorize
    is_index = symbol in INDEX_SYMBOLS
    if is_index:
        index_count += 1
    else:
        stock_count += 1
    
    rows.append({
        "symbol": symbol,
        "tradingsymbol": inst.get("tradingsymbol", ""),
        "strike": strike,
        "option_type": inst_type,
        "expiry_date": expiry_date.strftime("%Y-%m-%d"),
        "tick_size": inst.get("tick_size"),
        "lot_size": inst.get("lot_size"),
        "exchange": "NFO",
        "instrument_type": inst_type
    })

# Write CSV
fieldnames = ["symbol", "tradingsymbol", "strike", "option_type", "expiry_date", 
              "tick_size", "lot_size", "exchange", "instrument_type"]

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f"\n✅ CSV GENERATED: {OUTPUT_FILE}")
print(f"Total: {len(rows):,} instruments")
print(f"  Index options: {index_count:,}")
print(f"  Stock options: {stock_count:,}")

# Check for today's signals
print("\n" + "="*80)
print("CHECKING TODAY'S SIGNALS:")
print("="*80)
test_stocks = ['BEL', 'OBEROIRLTY', 'BPCL', 'SOLARIND', 'NATIONALISM']
for stock in test_stocks:
    count = sum(1 for r in rows if r['symbol'] == stock)
    status = "✓" if count > 0 else "✗"
    print(f"{status} {stock}: {count} options")

print("="*80)