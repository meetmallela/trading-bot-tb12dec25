"""
debug_nse_instruments.py
Check what instrument types Zerodha returns for NSE
"""

import json
from kiteconnect import KiteConnect
from collections import Counter

# Load config
with open("kite_config.json", "r") as f:
    cfg = json.load(f)

kite = KiteConnect(api_key=cfg["api_key"])
kite.set_access_token(cfg["access_token"])

print("\n" + "="*80)
print("DEBUGGING NSE INSTRUMENTS")
print("="*80)

# Fetch NSE instruments
instruments = kite.instruments("NSE")
print(f"\nTotal NSE instruments: {len(instruments)}")

# Count by instrument_type
types = Counter(inst.get("instrument_type") for inst in instruments)

print("\n" + "="*80)
print("INSTRUMENT TYPES IN NSE:")
print("="*80)
for inst_type, count in sorted(types.items(), key=lambda x: -x[1]):
    print(f"{inst_type:20s}: {count:,}")

# Find some option examples
print("\n" + "="*80)
print("SAMPLE STOCK OPTIONS FROM NSE:")
print("="*80)

samples = []
for inst in instruments:
    # Look for options (have strike price)
    if inst.get("strike") and inst.get("strike") > 0:
        samples.append({
            'name': inst.get('name'),
            'tradingsymbol': inst.get('tradingsymbol'),
            'type': inst.get('instrument_type'),
            'strike': inst.get('strike'),
            'expiry': inst.get('expiry')
        })
        if len(samples) >= 10:
            break

if samples:
    print("\nFound stock options! Examples:")
    for s in samples:
        print(f"  {s['name']:15s} Strike: {s['strike']:8.0f} Type: {s['type']:15s} Symbol: {s['tradingsymbol']}")
else:
    print("\n❌ NO STOCK OPTIONS FOUND IN NSE!")
    print("This means stock options are NOT in NSE exchange")
    print("They might be in NFO exchange instead!")

print("\n" + "="*80)
print("CHECKING NFO FOR STOCK OPTIONS...")
print("="*80)

nfo_instruments = kite.instruments("NFO")
print(f"Total NFO instruments: {len(nfo_instruments)}")

# Count by type
nfo_types = Counter(inst.get("instrument_type") for inst in nfo_instruments)
print("\nInstrument types in NFO:")
for inst_type, count in sorted(nfo_types.items(), key=lambda x: -x[1]):
    print(f"{inst_type:20s}: {count:,}")

# Look for stock options in NFO
print("\n" + "="*80)
print("SAMPLE INSTRUMENTS FROM NFO:")
print("="*80)

stock_options = []
index_options = []

for inst in nfo_instruments:
    name = inst.get('name', '')
    inst_type = inst.get('instrument_type')
    
    # Index options
    if name in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY'] and inst_type in ['CE', 'PE']:
        if len(index_options) < 3:
            index_options.append(inst)
    
    # Stock options (not indices)
    elif name not in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY'] and inst_type in ['CE', 'PE']:
        if len(stock_options) < 10:
            stock_options.append(inst)

print("\nIndex Options (NIFTY, BANKNIFTY, etc.):")
for inst in index_options:
    print(f"  {inst.get('tradingsymbol'):25s} {inst.get('name'):15s} Strike: {inst.get('strike')}")

print("\nStock Options (TATASTEEL, RELIANCE, etc.):")
for inst in stock_options:
    print(f"  {inst.get('tradingsymbol'):25s} {inst.get('name'):15s} Strike: {inst.get('strike')}")

print("\n" + "="*80)
print("DIAGNOSIS:")
print("="*80)

if stock_options:
    print("✓ Stock options ARE in NFO exchange!")
    print("✓ They use instrument_type: 'CE' or 'PE'")
    print("✓ Solution: Filter NFO for stock options (name NOT in index list)")
else:
    print("❌ Stock options not found in NFO either!")
    print("Need to investigate further...")

print("="*80)