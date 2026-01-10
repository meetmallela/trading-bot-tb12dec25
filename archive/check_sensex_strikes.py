"""
Check what SENSEX strikes are available in instruments_master.csv
"""

import pandas as pd

try:
    df = pd.read_csv('instruments_master.csv')
    
    print("="*80)
    print("SENSEX STRIKES AVAILABLE")
    print("="*80)
    
    # Filter SENSEX options
    sensex = df[df['symbol'] == 'SENSEX'].copy()
    
    if len(sensex) == 0:
        print("❌ NO SENSEX options found in CSV!")
        print("\nCheck if:")
        print("1. File is instruments_master.csv or valid_instruments.csv?")
        print("2. Symbol might be 'SENSEX' or 'BSX' or something else?")
    else:
        print(f"✅ Found {len(sensex)} SENSEX options\n")
        
        # Group by expiry
        print("AVAILABLE STRIKES BY EXPIRY:")
        print("-"*80)
        
        for expiry in sorted(sensex['expiry_date'].unique()):
            expiry_options = sensex[sensex['expiry_date'] == expiry]
            strikes = sorted(expiry_options['strike'].unique())
            
            print(f"\nExpiry: {expiry}")
            print(f"  CE strikes: {[int(s) for s in strikes if int(s) in expiry_options[expiry_options['option_type'] == 'CE']['strike'].values][:10]}")
            print(f"  PE strikes: {[int(s) for s in strikes if int(s) in expiry_options[expiry_options['option_type'] == 'PE']['strike'].values][:10]}")
            print(f"  Range: {int(min(strikes))} to {int(max(strikes))}")
            print(f"  Total: {len(strikes)} strikes")
        
        # Check if 84500 exists
        print("\n" + "="*80)
        print("CHECKING STRIKE 84500:")
        print("="*80)
        
        strike_84500 = sensex[sensex['strike'] == 84500]
        
        if len(strike_84500) > 0:
            print(f"✅ SENSEX 84500 EXISTS!")
            print("\nDetails:")
            for _, row in strike_84500.iterrows():
                print(f"  {row['tradingsymbol']} | Expiry: {row['expiry_date']} | {row['option_type']}")
        else:
            print(f"❌ SENSEX 84500 does NOT exist!")
            
            # Find nearest strikes
            all_strikes = sorted(sensex['strike'].unique())
            print(f"\nNearest available strikes:")
            for s in all_strikes:
                if 84000 <= s <= 85000:
                    print(f"  {int(s)}")

except FileNotFoundError:
    print("❌ instruments_master.csv not found!")
    print("\nTry:")
    print("  dir *.csv")
    print("\nYou might have:")
    print("  - valid_instruments.csv")
    print("  - instruments_cache.csv")
    
except Exception as e:
    print(f"❌ Error: {e}")
