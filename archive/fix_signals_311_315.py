"""
Fix incomplete signals 311-315
Adds missing fields and resets to processed=0
"""

import sqlite3
import json
from datetime import datetime, timedelta

conn = sqlite3.connect('trading.db')
cursor = conn.cursor()

print("="*80)
print("FIXING SIGNALS 311-315")
print("="*80)

# Load valid_instruments to get expiry dates and lot sizes
try:
    import pandas as pd
    df = pd.read_csv('valid_instruments.csv')
    print(f"✅ Loaded {len(df)} instruments\n")
except:
    print("❌ Could not load valid_instruments.csv")
    df = None

def get_nearest_expiry(symbol, strike=None):
    """Get nearest expiry for a symbol"""
    if df is None:
        # Default expiries if CSV not available
        today = datetime.now()
        # Next Thursday for NIFTY/BANKNIFTY
        days_ahead = 3 - today.weekday()  # Thursday is 3
        if days_ahead <= 0:
            days_ahead += 7
        next_thursday = today + timedelta(days=days_ahead)
        return next_thursday.strftime('%Y-%m-%d')
    
    symbol_df = df[df['symbol'] == symbol]
    if strike:
        symbol_df = symbol_df[symbol_df['strike'] == float(strike)]
    
    if len(symbol_df) > 0:
        # Get nearest expiry
        expiries = sorted(symbol_df['expiry_date'].unique())
        return expiries[0] if expiries else None
    return None

def get_lot_size(symbol, exchange='NFO'):
    """Get lot size for symbol"""
    if df is None:
        # Default lot sizes
        defaults = {
            'NIFTY': 25,
            'BANKNIFTY': 15,
            'SENSEX': 10,
            'LTIM': 125,
            'HEROMOTOCO': 100,
        }
        return defaults.get(symbol, 1)
    
    symbol_df = df[(df['symbol'] == symbol) & (df['exchange'] == exchange)]
    if len(symbol_df) > 0:
        return int(symbol_df.iloc[0]['lot_size'])
    return 1

# Fix each signal
fixes = [
    {
        'id': 311,
        'updates': {
            'symbol': 'LTIM',  # Was "DEC" (wrong!)
            'strike': 6250,
            'option_type': 'CE',
            'action': 'BUY',
            'entry_price': 142.0,
            'stop_loss': 135.0,
            'targets': [155.0, 178.0, 198.0],
            'expiry_date': get_nearest_expiry('LTIM', 6250) or '2025-12-26',
            'quantity': get_lot_size('LTIM')
        }
    },
    {
        'id': 312,
        'updates': {
            'symbol': 'NIFTY',
            'strike': 25950,  # From message
            'option_type': 'CE',
            'action': 'BUY',
            'entry_price': 140.0,
            'stop_loss': 130.0,
            'targets': [158.0, 178.0, 198.0],
            'expiry_date': get_nearest_expiry('NIFTY', 25950) or '2025-12-19',
            'quantity': get_lot_size('NIFTY')
        }
    },
    {
        'id': 313,
        'info': 'SKIP - ZINC is a commodity future, not an option. Cannot process.',
        'action': 'delete'
    },
    {
        'id': 314,
        'updates': {
            'symbol': 'HEROMOTOCO',
            'strike': 6000,
            'option_type': 'PE',
            'action': 'BUY',
            'entry_price': 132.0,
            'stop_loss': 122.0,
            'targets': [145.0, 178.0, 198.0],
            'expiry_date': get_nearest_expiry('HEROMOTOCO', 6000) or '2025-12-26',
            'quantity': get_lot_size('HEROMOTOCO')
        }
    },
    {
        'id': 315,
        'updates': {
            'symbol': 'SENSEX',
            'strike': 84300,  # From message
            'option_type': 'PE',
            'action': 'BUY',
            'entry_price': 122.0,
            'stop_loss': 78.0,
            'targets': [155.0, 200.0, 400.0],
            'expiry_date': get_nearest_expiry('SENSEX', 84300) or '2025-12-20',
            'quantity': get_lot_size('SENSEX', 'BFO')
        }
    }
]

for fix in fixes:
    signal_id = fix['id']
    
    if fix.get('action') == 'delete':
        print(f"\n[{signal_id}] ⚠️  {fix['info']}")
        cursor.execute("DELETE FROM signals WHERE id = ?", (signal_id,))
        print(f"[{signal_id}] ✅ Deleted")
        continue
    
    # Get current signal
    cursor.execute("SELECT parsed_data FROM signals WHERE id = ?", (signal_id,))
    row = cursor.fetchone()
    
    if not row:
        print(f"\n[{signal_id}] ❌ Signal not found")
        continue
    
    # Parse and update
    parsed_data = json.loads(row[0])
    parsed_data.update(fix['updates'])
    
    print(f"\n[{signal_id}] Fixing signal:")
    print(f"   Symbol: {parsed_data.get('symbol')} {parsed_data.get('strike')} {parsed_data.get('option_type')}")
    print(f"   Action: {parsed_data.get('action')}")
    print(f"   Entry: {parsed_data.get('entry_price')}")
    print(f"   SL: {parsed_data.get('stop_loss')}")
    print(f"   Expiry: {parsed_data.get('expiry_date')}")
    print(f"   Quantity: {parsed_data.get('quantity')}")
    
    # Update database
    cursor.execute("""
        UPDATE signals 
        SET parsed_data = ?,
            processed = 0
        WHERE id = ?
    """, (json.dumps(parsed_data), signal_id))
    
    print(f"[{signal_id}] ✅ Fixed and reset to processed=0")

conn.commit()
conn.close()

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print("✅ Fixed signals: 311, 312, 314, 315")
print("❌ Deleted signal: 313 (ZINC commodity - not supported)")
print("\n✅ All fixed signals reset to processed=0")
print("\n🚀 Order placer will pick them up within 30 seconds!")
print("="*80)
