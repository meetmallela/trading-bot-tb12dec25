"""
generate_instruments_csv.py - Generate valid_instruments.csv from Kite API
Run this weekly to keep your reference data updated
"""

import csv
import json
from datetime import datetime, timedelta
from kiteconnect import KiteConnect

# Load Kite credentials
try:
    with open('kite_config.json', 'r') as f:
        config = json.load(f)
        API_KEY = config.get('api_key')
        ACCESS_TOKEN = config.get('access_token')
except FileNotFoundError:
    print("❌ kite_config.json not found!")
    exit(1)

# Initialize Kite
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

print("\n" + "="*80)
print("GENERATING VALID INSTRUMENTS CSV FROM KITE")
print("="*80)

# Get instruments for different exchanges
exchanges = ['NFO', 'BFO', 'MCX', 'CDS']
all_instruments = []

for exchange in exchanges:
    try:
        print(f"\n[{exchange}] Fetching instruments...")
        instruments = kite.instruments(exchange)
        
        # Filter for current week + next week expiries only
        today = datetime.now().date()
        next_week = today + timedelta(days=14)
        
        filtered = []
        for inst in instruments:
            # Only options
            if inst['instrument_type'] not in ['CE', 'PE']:
                continue
            
            # Only upcoming expiries
            if inst['expiry']:
                expiry_date = inst['expiry'].date() if hasattr(inst['expiry'], 'date') else inst['expiry']
                if today <= expiry_date <= next_week:
                    filtered.append({
                        'symbol': inst['name'],
                        'strike': inst['strike'],
                        'option_type': inst['instrument_type'],
                        'expiry_date': expiry_date.strftime('%Y-%m-%d'),
                        'tick_size': inst['tick_size'],
                        'lot_size': inst['lot_size'],
                        'exchange': inst['exchange'],
                        'instrument_type': 'INDEX_OPTION' if exchange in ['NFO', 'BFO'] else 'COMMODITY'
                    })
        
        all_instruments.extend(filtered)
        print(f"[{exchange}] Found {len(filtered)} valid instruments")
        
    except Exception as e:
        print(f"[{exchange}] Error: {e}")

# Write to CSV
output_file = 'valid_instruments.csv'

print(f"\n{'='*80}")
print(f"WRITING TO {output_file}")
print(f"{'='*80}")

with open(output_file, 'w', newline='') as f:
    if all_instruments:
        writer = csv.DictWriter(f, fieldnames=all_instruments[0].keys())
        writer.writeheader()
        writer.writerows(all_instruments)

print(f"\n✅ SUCCESS!")
print(f"Total instruments: {len(all_instruments)}")
print(f"File: {output_file}")

# Show breakdown
by_symbol = {}
for inst in all_instruments:
    symbol = inst['symbol']
    if symbol not in by_symbol:
        by_symbol[symbol] = 0
    by_symbol[symbol] += 1

print(f"\nBreakdown:")
for symbol, count in sorted(by_symbol.items()):
    print(f"  {symbol}: {count} instruments")

print(f"\n{'='*80}")
print("NEXT STEPS:")
print("="*80)
print("""
1. Verify the CSV has your expected strikes
2. Use grounded_llm_parser.py with this CSV
3. Re-run this script weekly to update expiries
4. Optionally: Set up a cron job to auto-update

Usage:
    python grounded_llm_parser.py
""")

print("="*80 + "\n")
