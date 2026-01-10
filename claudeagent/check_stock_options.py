"""
check_stock_options.py
Check if today's JP signals actually exist on Zerodha
"""

import json
from kiteconnect import KiteConnect

# Stocks from today's signals
TEST_STOCKS = ['SOLARIND', 'NATIONALISM', 'BEL', 'OBEROIRLTY', 'BPCL']

with open("kite_config.json") as f:
    cfg = json.load(f)

kite = KiteConnect(api_key=cfg["api_key"])
kite.set_access_token(cfg["access_token"])

print("="*80)
print("CHECKING IF TODAY'S STOCK OPTIONS EXIST ON ZERODHA")
print("="*80)

# Get NFO instruments
nfo_instruments = kite.instruments("NFO")
print(f"\nTotal NFO instruments: {len(nfo_instruments)}")

# Check each stock
for stock in TEST_STOCKS:
    print(f"\n{stock}:")
    found = []
    for inst in nfo_instruments:
        if inst.get('name', '').upper() == stock.upper():
            if inst.get('instrument_type') in ['CE', 'PE']:
                found.append({
                    'tradingsymbol': inst.get('tradingsymbol'),
                    'strike': inst.get('strike'),
                    'type': inst.get('instrument_type'),
                    'expiry': inst.get('expiry')
                })
    
    if found:
        print(f"  ✓ Found {len(found)} options")
        for opt in found[:5]:  # Show first 5
            print(f"    {opt['tradingsymbol']:30s} Strike: {opt['strike']:8.0f}")
    else:
        print(f"  ✗ NOT FOUND - This stock has no options on Zerodha NFO!")

print("\n" + "="*80)
print("CONCLUSION:")
print("="*80)
print("If stocks show 'NOT FOUND', they genuinely don't have options on Zerodha.")
print("JP channel is signaling stocks that aren't actually tradable!")
print("="*80)