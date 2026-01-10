"""
fix_missing_expiry.py - Auto-fix signals missing expiry_date
"""

import sqlite3
import json
from datetime import datetime

def fix_missing_expiry():
    conn = sqlite3.connect('trading.db')
    cursor = conn.cursor()
    
    # Find signals missing expiry_date
    cursor.execute("""
        SELECT id, parsed_data, raw_text
        FROM signals 
        WHERE processed = 0
        AND parsed_data IS NOT NULL
        AND parsed_data NOT LIKE '%expiry_date%'
    """)
    
    signals = cursor.fetchall()
    
    print(f"\n{'='*80}")
    print(f"FOUND {len(signals)} SIGNALS MISSING EXPIRY DATE")
    print("="*80)
    
    if not signals:
        print("All signals have expiry dates! ✓")
        conn.close()
        return
    
    fixed = 0
    
    for signal_id, parsed_data_str, raw_text in signals:
        try:
            parsed = json.loads(parsed_data_str)
            symbol = parsed.get('symbol')
            
            if not symbol:
                continue
            
            # Determine expiry based on symbol
            if symbol in ['GOLD', 'GOLDM', 'SILVER', 'SILVERM']:
                # Next available GOLD expiry
                expiry = '2026-01-27'  # Jan 2026
                reason = "GOLD/SILVER -> Jan 27, 2026"
            
            elif symbol in ['CRUDEOIL', 'NATURALGAS']:
                # Next available commodity expiry
                today = datetime.now()
                if today.month == 12 and today.day < 19:
                    expiry = '2025-12-19'
                    reason = "December 19, 2025"
                else:
                    expiry = '2026-01-19'
                    reason = "January 19, 2026"
            
            elif symbol in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'SENSEX', 'BANKEX']:
                # Index options - use nearest weekly/monthly
                expiry = '2025-12-19'  # Placeholder - order_placer calculates
                reason = "Index -> order_placer calculates"
            
            else:
                print(f"  Signal #{signal_id}: Unknown symbol '{symbol}' - skipping")
                continue
            
            # Add expiry_date
            parsed['expiry_date'] = expiry
            
            # Update database
            cursor.execute("""
                UPDATE signals 
                SET parsed_data = ?
                WHERE id = ?
            """, (json.dumps(parsed), signal_id))
            
            print(f"  ✓ Signal #{signal_id} ({symbol}): Added expiry = {expiry} ({reason})")
            fixed += 1
        
        except Exception as e:
            print(f"  ✗ Signal #{signal_id}: Error - {e}")
    
    conn.commit()
    conn.close()
    
    print(f"\n{'='*80}")
    print(f"FIXED {fixed} SIGNALS")
    print("="*80)
    print("\nThese signals will be processed in next order_placer cycle!")

if __name__ == "__main__":
    fix_missing_expiry()
