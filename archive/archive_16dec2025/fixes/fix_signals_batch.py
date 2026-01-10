"""
fix_signals_batch.py - Fix existing signals with missing data
"""

import sqlite3
import json

def fix_signals():
    """Fix signals 276-279 with correct data"""
    
    conn = sqlite3.connect('trading.db')
    cursor = conn.cursor()
    
    print("="*80)
    print("FIXING SIGNALS 276-279")
    print("="*80)
    
    fixes = [
        {
            'id': 276,
            'symbol': 'SILVER',
            'expiry_date': '2026-01-27'
        },
        {
            'id': 277,
            'symbol': 'GOLD',
            'expiry_date': '2026-01-27'
        },
        {
            'id': 278,
            'symbol': 'GOLD',
            'expiry_date': '2026-01-27'
        },
        {
            'id': 279,
            'symbol': 'NATURALGAS',  # Fix GAS → NATURALGAS
            'expiry_date': '2025-12-23'
        }
    ]
    
    for fix in fixes:
        signal_id = fix['id']
        
        # Get current data
        cursor.execute("SELECT parsed_data FROM signals WHERE id = ?", (signal_id,))
        row = cursor.fetchone()
        
        if not row:
            print(f"Signal {signal_id} not found")
            continue
        
        parsed_data = json.loads(row[0])
        
        # Apply fixes
        if 'symbol' in fix:
            old_symbol = parsed_data.get('symbol')
            parsed_data['symbol'] = fix['symbol']
            print(f"\nSignal {signal_id}:")
            print(f"  Symbol: {old_symbol} → {fix['symbol']}")
        
        if 'expiry_date' in fix:
            parsed_data['expiry_date'] = fix['expiry_date']
            parsed_data['expiry_auto_calculated'] = True
            print(f"  Expiry: {fix['expiry_date']}")
        
        # Mark as commodity if it's a commodity symbol
        commodity_symbols = ['GOLD', 'SILVER', 'NATURALGAS', 'CRUDEOIL', 'COPPER', 'ZINC']
        if parsed_data.get('symbol') in commodity_symbols:
            parsed_data['commodity'] = True
            print(f"  Commodity: true")
        
        # Update signal
        cursor.execute(
            "UPDATE signals SET parsed_data = ?, processed = 0 WHERE id = ?",
            (json.dumps(parsed_data), signal_id)
        )
    
    conn.commit()
    
    print("\n" + "="*80)
    print("VERIFICATION")
    print("="*80)
    
    # Show updated signals
    cursor.execute("""
        SELECT id, 
               json_extract(parsed_data, '$.symbol'),
               json_extract(parsed_data, '$.strike'),
               json_extract(parsed_data, '$.option_type'),
               json_extract(parsed_data, '$.expiry_date'),
               processed
        FROM signals 
        WHERE id IN (276, 277, 278, 279)
        ORDER BY id
    """)
    
    print("\nFixed signals:")
    print("-"*80)
    for row in cursor.fetchall():
        print(f"ID: {row[0]} | {row[1]} {row[2]} {row[3]} | Exp: {row[4]} | Processed: {row[5]}")
    
    conn.close()
    
    print("\n" + "="*80)
    print("✅ SIGNALS FIXED!")
    print("="*80)
    print("\nThese signals will be reprocessed by order_placer in next cycle (30 seconds)")
    print("\n")


if __name__ == "__main__":
    fix_signals()
