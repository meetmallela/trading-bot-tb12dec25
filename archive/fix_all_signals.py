"""
Universal Signal Fixer - Fix ALL incomplete signals
Adds missing fields: expiry_date, quantity, action, stop_loss
"""

import sqlite3
import json
from datetime import datetime, timedelta
import pandas as pd

# Load valid instruments
try:
    df = pd.read_csv('valid_instruments.csv')
    print(f"✅ Loaded {len(df)} instruments from CSV\n")
except:
    print("❌ Could not load valid_instruments.csv")
    df = None

def get_nearest_expiry(symbol, strike=None):
    """Get nearest expiry for a symbol"""
    if df is None:
        # Default: next Thursday
        today = datetime.now()
        days_ahead = 3 - today.weekday()
        if days_ahead <= 0:
            days_ahead += 7
        next_thursday = today + timedelta(days=days_ahead)
        return next_thursday.strftime('%Y-%m-%d')
    
    symbol_df = df[df['symbol'] == symbol]
    if strike:
        symbol_df = symbol_df[symbol_df['strike'] == float(strike)]
    
    if len(symbol_df) > 0:
        expiries = sorted(symbol_df['expiry_date'].unique())
        return expiries[0] if expiries else None
    return None

def get_lot_size(symbol, exchange='NFO'):
    """Get lot size for symbol"""
    if df is None:
        defaults = {
            'NIFTY': 25, 'BANKNIFTY': 15, 'SENSEX': 10,
            'LTIM': 150, 'HEROMOTOCO': 100, 'NATURALGAS': 1250
        }
        return defaults.get(symbol, 1)
    
    symbol_df = df[(df['symbol'] == symbol) & (df['exchange'] == exchange)]
    if len(symbol_df) > 0:
        return int(symbol_df.iloc[0]['lot_size'])
    return 1

# Connect to database
conn = sqlite3.connect('trading.db')
cursor = conn.cursor()

print("="*80)
print("UNIVERSAL SIGNAL FIXER")
print("="*80)

# Get all unprocessed signals
cursor.execute("""
    SELECT id, raw_text, parsed_data 
    FROM signals 
    WHERE processed = 0
    ORDER BY id
""")

signals = cursor.fetchall()

if not signals:
    print("\n✅ No unprocessed signals to fix!")
else:
    print(f"\n⚠️  Found {len(signals)} unprocessed signals\n")
    
    fixed_count = 0
    skip_count = 0
    
    for signal_id, raw_text, parsed_json in signals:
        try:
            parsed = json.loads(parsed_json)
            
            print(f"[{signal_id}] Checking signal...")
            print(f"   Raw: {raw_text[:60]}...")
            
            # Check required fields
            required = ['symbol', 'strike', 'option_type', 'action', 
                       'entry_price', 'stop_loss', 'expiry_date', 'quantity']
            missing = [f for f in required if f not in parsed or parsed[f] is None]
            
            if not missing:
                print(f"[{signal_id}] ✅ Already complete!")
                continue
            
            print(f"[{signal_id}] ⚠️  Missing: {', '.join(missing)}")
            
            # Symbol validation - skip if symbol is wrong
            symbol = parsed.get('symbol')
            if not symbol or symbol in ['DEC', 'ZINC', 'GAS']:
                print(f"[{signal_id}] ❌ Invalid symbol '{symbol}' - SKIPPING")
                cursor.execute("UPDATE signals SET processed = 1 WHERE id = ?", (signal_id,))
                skip_count += 1
                continue
            
            # Fix symbol mapping
            symbol_map = {
                'NATURALGAS': 'NATURALGAS',
                'CRUDEOIL': 'CRUDEOIL',
                'GOLD': 'GOLDM',
            }
            if symbol in symbol_map:
                parsed['symbol'] = symbol_map[symbol]
                symbol = parsed['symbol']
            
            # Add missing fields
            needs_update = False
            
            # Action (default to BUY if missing)
            if 'action' not in parsed or not parsed['action']:
                if 'SELL' in raw_text.upper() or 'SHORT' in raw_text.upper():
                    parsed['action'] = 'SELL'
                else:
                    parsed['action'] = 'BUY'
                needs_update = True
            
            # Stop loss (calculate if missing)
            if 'stop_loss' not in parsed or not parsed['stop_loss']:
                entry = parsed.get('entry_price')
                if entry:
                    # Default: 5% below for BUY, 5% above for SELL
                    if parsed['action'] == 'BUY':
                        parsed['stop_loss'] = entry * 0.95
                    else:
                        parsed['stop_loss'] = entry * 1.05
                    needs_update = True
            
            # Expiry date
            if 'expiry_date' not in parsed or not parsed['expiry_date']:
                strike = parsed.get('strike')
                expiry = get_nearest_expiry(symbol, strike)
                if expiry:
                    parsed['expiry_date'] = expiry
                    needs_update = True
                else:
                    print(f"[{signal_id}] ❌ Cannot find expiry - SKIPPING")
                    cursor.execute("UPDATE signals SET processed = 1 WHERE id = ?", (signal_id,))
                    skip_count += 1
                    continue
            
            # Quantity (lot size)
            if 'quantity' not in parsed or not parsed['quantity']:
                lot_size = get_lot_size(symbol)
                parsed['quantity'] = lot_size
                needs_update = True
            
            if needs_update:
                # Update database
                cursor.execute("""
                    UPDATE signals 
                    SET parsed_data = ?
                    WHERE id = ?
                """, (json.dumps(parsed), signal_id))
                
                print(f"[{signal_id}] ✅ FIXED!")
                print(f"   Symbol: {parsed['symbol']} {parsed['strike']} {parsed['option_type']}")
                print(f"   Action: {parsed['action']}")
                print(f"   Entry: {parsed['entry_price']} | SL: {parsed['stop_loss']}")
                print(f"   Expiry: {parsed['expiry_date']} | Qty: {parsed['quantity']}")
                fixed_count += 1
            
        except Exception as e:
            print(f"[{signal_id}] ❌ Error: {e}")
            skip_count += 1
        
        print()
    
    conn.commit()
    
    print("="*80)
    print("SUMMARY")
    print("="*80)
    print(f"✅ Fixed: {fixed_count} signals")
    print(f"❌ Skipped: {skip_count} signals")
    print(f"Total: {len(signals)} signals")
    print("="*80)
    
    if fixed_count > 0:
        print("\n✅ Fixed signals will be processed in next order_placer cycle!")
        print("   (within 30 seconds)")

conn.close()
