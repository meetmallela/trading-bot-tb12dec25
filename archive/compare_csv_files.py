"""
Find which CSV file has the most complete instrument data
"""

import pandas as pd

files = [
    'instruments_cache.csv',
    'instruments_master.csv', 
    'valid_instruments.csv'
]

print("="*80)
print("CSV FILE COMPARISON")
print("="*80)

best_file = None
best_count = 0

for filename in files:
    try:
        df = pd.read_csv(filename)
        
        # Count instruments
        total = len(df)
        
        # Count by symbol
        nifty = len(df[df['symbol'] == 'NIFTY'])
        banknifty = len(df[df['symbol'] == 'BANKNIFTY'])
        sensex = len(df[df['symbol'] == 'SENSEX'])
        gold = len(df[df['symbol'] == 'GOLDM'])
        
        print(f"\n{filename}:")
        print(f"  Total: {total:,}")
        print(f"  NIFTY: {nifty}")
        print(f"  BANKNIFTY: {banknifty}")
        print(f"  SENSEX: {sensex}")
        print(f"  GOLDM: {gold}")
        
        if total > best_count:
            best_count = total
            best_file = filename
            
    except Exception as e:
        print(f"\n{filename}: ❌ Error - {e}")

print("\n" + "="*80)
print("RECOMMENDATION")
print("="*80)
print(f"\n✅ Use: {best_file}")
print(f"   (Most complete with {best_count:,} instruments)")

print("\n" + "="*80)
print("FIX COMMAND")
print("="*80)
print(f"\ncopy {best_file} instruments_master.csv")
print("\nThen restart telegram_reader_production.py")
