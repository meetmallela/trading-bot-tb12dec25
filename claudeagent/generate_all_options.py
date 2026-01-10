"""
generate_all_options.py
Gets ALL options from NFO (both index + stock options)
"""

import csv
import json
from datetime import datetime, timedelta
from kiteconnect import KiteConnect

KITE_CONFIG_FILE = "kite_config.json"
OUTPUT_FILE = "valid_instruments.csv"

# Index symbols (to separate from stock options)
INDEX_SYMBOLS = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX']

with open(KITE_CONFIG_FILE, "r") as f:
    cfg = json.load(f)

kite = KiteConnect(api_key=cfg["api_key"])
kite.set_access_token(cfg["access_token"])

today = datetime.now().date()
expiry_limit = today + timedelta(days=45)

print("="*80)
print("FETCHING ALL OPTIONS FROM NFO (Index + Stock)")
print("="*80)

# Get NFO instruments
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
    
    # Check expiry
    expiry = inst.get("expiry")
    if not expiry:
        continue
        
    if hasattr(expiry, "date"):
        expiry_date = expiry.date()
    else:
        expiry_date = expiry
    
    if not (today <= expiry_date <= expiry_limit):
        continue
    
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
print(f"  Stock options: {stock_count:,} ⭐")
print("\nSample stock options:")
samples = [r for r in rows if r['symbol'] not in INDEX_SYMBOLS][:5]
for s in samples:
    print(f"  {s['tradingsymbol']:25s} {s['symbol']}")
print("="*80)