"""
Quick check: What columns are in valid_instruments.csv?
"""

import pandas as pd

csv_file = 'valid_instruments.csv'

try:
    df = pd.read_csv(csv_file)
    
    print("="*80)
    print(f"COLUMNS IN {csv_file}")
    print("="*80)
    print(f"\nTotal rows: {len(df)}")
    print(f"\nColumns ({len(df.columns)}):")
    
    for i, col in enumerate(df.columns, 1):
        print(f"  {i}. {col}")
    
    print("\n" + "="*80)
    print("SAMPLE DATA")
    print("="*80)
    print(df.head(3))
    
    print("\n" + "="*80)
    print("COLUMN NAME MAPPING NEEDED")
    print("="*80)
    
    # Check what the trading symbol column is called
    possible_names = ['tradingsymbol', 'symbol', 'name', 'trading_symbol', 'instrument_name', 'token']
    
    for name in possible_names:
        if name in df.columns:
            print(f"✅ Found column: '{name}'")
            if 'NIFTY' in str(df[name].iloc[0]):
                print(f"   → This looks like the tradingsymbol column!")
        else:
            print(f"❌ Not found: '{name}'")

except Exception as e:
    print(f"Error: {e}")