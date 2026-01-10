"""
Diagnose what's in instruments_master.csv
"""

import pandas as pd

csv_file = 'instruments_master.csv'

print("="*80)
print("CSV DIAGNOSTIC")
print("="*80)

try:
    df = pd.read_csv(csv_file)
    
    print(f"\n✅ Loaded {len(df)} instruments")
    
    # Check columns
    print(f"\nColumns: {list(df.columns)}")
    
    # Check for NIFTY
    print("\n" + "="*80)
    print("NIFTY OPTIONS")
    print("="*80)
    
    nifty = df[df['symbol'] == 'NIFTY']
    print(f"Total NIFTY options: {len(nifty)}")
    
    if len(nifty) > 0:
        print("\nSample NIFTY records:")
        print(nifty.head(3))
        
        print("\nAvailable NIFTY strikes (first 20):")
        strikes = sorted(nifty['strike'].unique())
        print([int(s) for s in strikes[:20]])
        
        print("\nStrike range:")
        print(f"  Min: {int(min(strikes))}")
        print(f"  Max: {int(max(strikes))}")
        
        # Check if 25900 exists
        if 25900 in nifty['strike'].values:
            print(f"\n✅ Strike 25900 EXISTS")
            print(nifty[nifty['strike'] == 25900][['tradingsymbol', 'expiry_date', 'option_type']].head())
        else:
            print(f"\n❌ Strike 25900 does NOT exist")
            # Find nearest
            nearest = min(strikes, key=lambda x: abs(x - 25900))
            print(f"   Nearest strike: {int(nearest)}")
    
    # Check for SENSEX
    print("\n" + "="*80)
    print("SENSEX OPTIONS")
    print("="*80)
    
    sensex = df[df['symbol'] == 'SENSEX']
    print(f"Total SENSEX options: {len(sensex)}")
    
    if len(sensex) > 0:
        print("\nSample SENSEX records:")
        print(sensex.head(3))
        
        print("\nAvailable SENSEX strikes (first 20):")
        strikes = sorted(sensex['strike'].unique())
        print([int(s) for s in strikes[:20]])
        
        print("\nStrike range:")
        print(f"  Min: {int(min(strikes))}")
        print(f"  Max: {int(max(strikes))}")
        
        # Check if 84500 exists
        if 84500 in sensex['strike'].values:
            print(f"\n✅ Strike 84500 EXISTS")
            print(sensex[sensex['strike'] == 84500][['tradingsymbol', 'expiry_date', 'option_type']].head())
        else:
            print(f"\n❌ Strike 84500 does NOT exist")
            # Find nearest
            nearest = min(strikes, key=lambda x: abs(x - 84500))
            print(f"   Nearest strike: {int(nearest)}")
    else:
        print("❌ No SENSEX options found!")
    
    # Check all unique symbols
    print("\n" + "="*80)
    print("ALL SYMBOLS IN CSV")
    print("="*80)
    
    symbols = df['symbol'].value_counts()
    print(f"\nTotal unique symbols: {len(symbols)}")
    print("\nTop 20 symbols by count:")
    print(symbols.head(20))
    
    # Check data types
    print("\n" + "="*80)
    print("DATA TYPES")
    print("="*80)
    print(df.dtypes)
    
    # Sample records
    print("\n" + "="*80)
    print("SAMPLE RECORDS")
    print("="*80)
    print(df.head(5))

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*80)
print("RECOMMENDATION")
print("="*80)
print("""
Based on the results above:

1. If NIFTY/SENSEX exist but strikes don't match:
   → CSV might be outdated
   → Download fresh instruments from Zerodha

2. If NIFTY/SENSEX don't exist at all:
   → Wrong CSV file
   → Try: python instrument_finder_df.py valid_instruments.csv

3. If column names are different:
   → CSV format might be different
   → Check column names match: symbol, strike, option_type, etc.
""")