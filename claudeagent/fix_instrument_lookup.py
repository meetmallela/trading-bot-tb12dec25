"""
fix_instrument_lookup.py - CORRECTED VERSION
Handles float strikes from CSV (58200.0 → 58200)
Works with your exact CSV format
"""

import pandas as pd
from datetime import datetime

def load_instruments_with_expiry_lookup(csv_path):
    """
    Load instruments with expiry-aware lookup
    
    CSV Format (YOUR format):
    symbol,strike,option_type,expiry_date,tick_size,lot_size,exchange,instrument_type
    NIFTY,25050.0,CE,2026-01-27,0.05,65,NFO,CE
    
    Returns:
        by_tradingsymbol: {tradingsymbol → instrument_data}
        by_symbol_strike_type: {(symbol, strike, type) → [instruments sorted by expiry]}
    """
    
    df = pd.read_csv(csv_path)
    
    print(f"[LOADING] Reading {len(df)} instruments from {csv_path}")
    print(f"[COLUMNS] {df.columns.tolist()}")
    
    by_tradingsymbol = {}
    by_symbol_strike_type = {}
    
    for _, row in df.iterrows():
        symbol = row['symbol']
        
        # CRITICAL FIX: Convert float strike to int
        # CSV has: 58200.0
        # Need: 58200 (int) for key matching
        strike = int(float(row['strike']))
        
        option_type = row['option_type']
        expiry_date = row['expiry_date']
        
        # Read lot_size from column 6
        lot_size = int(row['lot_size'])
        
        exchange = row['exchange']
        
        # Convert expiry to datetime
        expiry_dt = pd.to_datetime(expiry_date)
        
        # Build tradingsymbol: SYMBOL + YY + MMM + DD + STRIKE + TYPE
        # Example: BANKNIFTY26JAN2858200PE
        exp_str = expiry_dt.strftime('%y%b%d').upper()  # 26JAN28
        tradingsymbol = f"{symbol}{exp_str}{strike}{option_type}"
        
        # Create instrument dict
        instrument = {
            'tradingsymbol': tradingsymbol,
            'symbol': symbol,
            'strike': strike,  # Now int
            'option_type': option_type,
            'expiry_date': expiry_date,
            'expiry_dt': expiry_dt,
            'exchange': exchange,
            'lot_size': lot_size
        }
        
        # Add to tradingsymbol lookup
        by_tradingsymbol[tradingsymbol] = instrument
        
        # Add to symbol+strike+type lookup (with int strike!)
        key = (symbol, strike, option_type)  # strike is int
        
        if key not in by_symbol_strike_type:
            by_symbol_strike_type[key] = []
        
        by_symbol_strike_type[key].append(instrument)
    
    # Sort each list by expiry (nearest first)
    for key in by_symbol_strike_type:
        by_symbol_strike_type[key].sort(key=lambda x: x['expiry_dt'])
    
    print(f"[OK] Loaded {len(by_tradingsymbol)} tradingsymbols")
    print(f"[OK] Unique symbol+strike+type combinations: {len(by_symbol_strike_type)}")
    
    # Show lot sizes
    print("\n[LOT SIZES] Verified:")
    for symbol_name in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']:
        # Find any instrument with this symbol
        matching = [
            inst_list[0]['lot_size'] 
            for (sym, _, _), inst_list in by_symbol_strike_type.items() 
            if sym == symbol_name
        ]
        if matching:
            print(f"   {symbol_name}: {matching[0]}")
    
    return by_tradingsymbol, by_symbol_strike_type


def find_nearest_expiry_instrument(symbol, strike, option_type, by_symbol_strike_type, reference_date=None):
    """
    Find instrument with NEAREST expiry for given symbol+strike+type
    
    Args:
        symbol: 'NIFTY', 'BANKNIFTY', etc
        strike: Strike price (int or float)
        option_type: 'CE' or 'PE'
        by_symbol_strike_type: Dict from load_instruments_with_expiry_lookup()
        reference_date: Date to search from (default: now)
    
    Returns:
        instrument dict or None
    """
    
    if reference_date is None:
        reference_date = datetime.now()
    
    # Convert reference_date to datetime if string
    if isinstance(reference_date, str):
        try:
            reference_date = datetime.fromisoformat(reference_date.replace('Z', '+00:00'))
            if reference_date.tzinfo:
                reference_date = reference_date.replace(tzinfo=None)
        except:
            reference_date = datetime.now()
    
    # CRITICAL FIX: Ensure strike is int for key matching
    strike = int(strike)
    
    key = (symbol, strike, option_type)
    
    if key not in by_symbol_strike_type:
        print(f"[LOOKUP] Key not found: {key}")
        return None
    
    # Get all instruments for this combination
    instruments = by_symbol_strike_type[key]
    
    # Filter future expiries only
    future_instruments = [
        inst for inst in instruments
        if inst['expiry_dt'] > reference_date
    ]
    
    if not future_instruments:
        print(f"[LOOKUP] No future expiries for {key}")
        return None
    
    # Return nearest (first in sorted list)
    nearest = future_instruments[0]
    
    print(f"[LOOKUP] Found: {nearest['tradingsymbol']} | Expiry: {nearest['expiry_date']} | Lot: {nearest['lot_size']}")
    
    return nearest


# Test when run directly
if __name__ == '__main__':
    import sys
    _master_lib = r"C:\Users\meetm\OneDrive\Desktop\GCPPythonCode\MasterConfiguration\lib"
    if _master_lib not in sys.path:
        sys.path.insert(0, _master_lib)
    from master_resource import get_instruments_path

    print("="*70)
    print("TESTING fix_instrument_lookup.py")
    print("="*70)

    # Load from MasterConfiguration
    by_ts, by_sst = load_instruments_with_expiry_lookup(get_instruments_path())
    
    print("\n" + "="*70)
    print("TEST 1: NIFTY 25000 CE")
    print("="*70)
    result = find_nearest_expiry_instrument(
        symbol='NIFTY',
        strike=25000,
        option_type='CE',
        by_symbol_strike_type=by_sst,
        reference_date=datetime(2026, 1, 27, 9, 0)
    )
    if result:
        print(f"✓ {result['tradingsymbol']} | Lot: {result['lot_size']}")
    else:
        print("✗ Not found")
    
    print("\n" + "="*70)
    print("TEST 2: BANKNIFTY 58200 PE")
    print("="*70)
    result = find_nearest_expiry_instrument(
        symbol='BANKNIFTY',
        strike=58200,
        option_type='PE',
        by_symbol_strike_type=by_sst,
        reference_date=datetime(2026, 1, 27, 9, 0)
    )
    if result:
        print(f"✓ {result['tradingsymbol']} | Lot: {result['lot_size']}")
    else:
        print("✗ Not found")
    
    print("\n" + "="*70)
    print("TEST 3: FINNIFTY 22000 CE")
    print("="*70)
    result = find_nearest_expiry_instrument(
        symbol='FINNIFTY',
        strike=22000,
        option_type='CE',
        by_symbol_strike_type=by_sst,
        reference_date=datetime(2026, 1, 27, 9, 0)
    )
    if result:
        print(f"✓ {result['tradingsymbol']} | Lot: {result['lot_size']}")
    else:
        print("✗ Not found")
    
    print("\n" + "="*70)
    print("✅ ALL TESTS COMPLETE")
    print("="*70)
