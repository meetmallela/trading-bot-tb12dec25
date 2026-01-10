"""
check_valid_instruments.py - Diagnostic for valid_instruments.csv
"""

import pandas as pd

# Load CSV
df = pd.read_csv('valid_instruments.csv')

print("="*80)
print("VALID_INSTRUMENTS.CSV DIAGNOSTIC")
print("="*80)

print(f"\nTotal instruments: {len(df)}")
print(f"\nColumns: {list(df.columns)}")

# Show unique symbols
print(f"\nUnique symbols ({len(df['symbol'].unique())}):")
for symbol in sorted(df['symbol'].unique())[:20]:
    count = len(df[df['symbol'] == symbol])
    print(f"  {symbol:15s} : {count:5d} instruments")

# Check for GOLDM specifically
print("\n" + "="*80)
print("GOLDM INSTRUMENTS:")
print("="*80)
goldm = df[df['symbol'] == 'GOLDM']
if len(goldm) > 0:
    print(f"Found {len(goldm)} GOLDM instruments")
    print("\nSample GOLDM strikes:")
    print(goldm[['symbol', 'strike', 'option_type', 'expiry_date', 'tick_size', 'lot_size', 'exchange']].head(10))
else:
    print("❌ NO GOLDM instruments found!")
    
    # Check for GOLD
    gold = df[df['symbol'] == 'GOLD']
    if len(gold) > 0:
        print(f"\n⚠️ Found {len(gold)} GOLD instruments (should be GOLDM)")
        print("\nSample GOLD strikes:")
        print(gold[['symbol', 'strike', 'option_type', 'expiry_date']].head(10))

# Check for NIFTY
print("\n" + "="*80)
print("NIFTY INSTRUMENTS:")
print("="*80)
nifty = df[df['symbol'] == 'NIFTY']
if len(nifty) > 0:
    print(f"Found {len(nifty)} NIFTY instruments")
    print("\nSample NIFTY strikes for Dec 19:")
    nifty_dec19 = nifty[nifty['expiry_date'].str.contains('2025-12-19')]
    if len(nifty_dec19) > 0:
        print(nifty_dec19[['symbol', 'strike', 'option_type', 'expiry_date', 'tick_size', 'lot_size', 'exchange']].head(10))
    else:
        print("❌ No NIFTY instruments for 2025-12-19")
        print("\nAvailable NIFTY expiries:")
        print(nifty['expiry_date'].unique()[:10])
else:
    print("❌ NO NIFTY instruments found!")

# Check for SILVER
print("\n" + "="*80)
print("SILVER INSTRUMENTS:")
print("="*80)
silver = df[df['symbol'] == 'SILVER']
if len(silver) > 0:
    print(f"Found {len(silver)} SILVER instruments")
    print("\nSample SILVER strikes:")
    print(silver[['symbol', 'strike', 'option_type', 'expiry_date', 'tick_size', 'lot_size', 'exchange']].head(10))
else:
    print("❌ NO SILVER instruments found!")

# Check expiry date format
print("\n" + "="*80)
print("DATE FORMAT CHECK:")
print("="*80)
sample_dates = df['expiry_date'].head(10).tolist()
print("Sample expiry dates:", sample_dates)
print("\nExpected format: YYYY-MM-DD (e.g., 2025-12-19)")

# Check for our test strikes
print("\n" + "="*80)
print("TEST STRIKES CHECK:")
print("="*80)

test_instruments = [
    ('NIFTY', 25900, 'CE', '2025-12-19'),
    ('GOLDM', 138000, 'CE', '2026-01-27'),
    ('BANKNIFTY', 56000, 'PE', '2025-12-18'),
    ('SILVER', 206000, 'CE', '2026-01-27'),
    ('SENSEX', 85700, 'CE', '2025-12-20'),
]

for symbol, strike, opt_type, expiry in test_instruments:
    mask = (
        (df['symbol'] == symbol) &
        (df['strike'] == float(strike)) &
        (df['option_type'] == opt_type) &
        (df['expiry_date'] == expiry)
    )
    found = df[mask]
    if len(found) > 0:
        print(f"✅ {symbol:15s} {strike:6.0f} {opt_type} {expiry} : FOUND")
    else:
        print(f"❌ {symbol:15s} {strike:6.0f} {opt_type} {expiry} : NOT FOUND")
        
        # Try to find why
        symbol_match = df[df['symbol'] == symbol]
        if len(symbol_match) == 0:
            print(f"   → Symbol {symbol} doesn't exist in CSV")
        else:
            strike_match = symbol_match[symbol_match['strike'] == float(strike)]
            if len(strike_match) == 0:
                print(f"   → Strike {strike} not available for {symbol}")
                available_strikes = sorted(symbol_match['strike'].unique())
                print(f"   → Available strikes: {available_strikes[:10]}...")
            else:
                expiry_match = strike_match[strike_match['expiry_date'] == expiry]
                if len(expiry_match) == 0:
                    print(f"   → Expiry {expiry} not available")
                    print(f"   → Available expiries: {strike_match['expiry_date'].unique()}")

print("\n" + "="*80)