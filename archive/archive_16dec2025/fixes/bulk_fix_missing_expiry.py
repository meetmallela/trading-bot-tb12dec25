"""
bulk_fix_missing_expiry.py - Fix ALL signals missing expiry_date
"""

import sqlite3
import json
from datetime import datetime

def bulk_fix_expiry():
    conn = sqlite3.connect('trading.db')
    cursor = conn.cursor()
    
    # Find all signals with complete data but missing expiry_date
    cursor.execute("""
        SELECT id, parsed_data, raw_text, timestamp
        FROM signals 
        WHERE parsed_data IS NOT NULL
        AND parsed_data LIKE '%"symbol"%'
        AND parsed_data NOT LIKE '%expiry_date%'
        ORDER BY id
    """)
    
    signals = cursor.fetchall()
    
    print(f"\n{'='*80}")
    print(f"FOUND {len(signals)} SIGNALS MISSING EXPIRY DATE")
    print("="*80)
    
    if not signals:
        print("All signals have expiry dates!")
        conn.close()
        return
    
    # Expiry mapping
    expiry_map = {
        'GOLD': '2026-01-27',
        'GOLDM': '2026-01-27',
        'SILVER': '2026-01-27',
        'SILVERM': '2026-01-27',
        'CRUDEOIL': '2025-12-19',
        'NATURALGAS': '2025-12-23',
        'COPPER': '2025-12-31',
        'ZINC': '2025-12-31'
    }
    
    fixed = 0
    skipped = 0
    
    for signal_id, parsed_data_str, raw_text, timestamp in signals:
        try:
            parsed = json.loads(parsed_data_str)
            symbol = parsed.get('symbol')
            
            if not symbol:
                print(f"  Signal #{signal_id}: No symbol - skipping")
                skipped += 1
                continue
            
            # Check if it has required fields
            has_action = parsed.get('action') is not None
            has_strike = parsed.get('strike') is not None
            has_option_type = parsed.get('option_type') is not None
            has_entry = parsed.get('entry_price') is not None
            has_sl = parsed.get('stop_loss') is not None
            
            # Only fix if it's a complete signal (except expiry)
            if not (has_action and has_strike and has_option_type and has_entry and has_sl):
                print(f"  Signal #{signal_id} ({symbol}): Incomplete - skipping")
                skipped += 1
                continue
            
            # Get expiry for this symbol
            expiry = expiry_map.get(symbol)
            
            if not expiry:
                print(f"  Signal #{signal_id}: Unknown symbol '{symbol}' - skipping")
                skipped += 1
                continue
            
            # Add expiry_date
            parsed['expiry_date'] = expiry
            
            # Update database
            cursor.execute("""
                UPDATE signals 
                SET 
                    parsed_data = ?,
                    processed = 0
                WHERE id = ?
            """, (json.dumps(parsed), signal_id))
            
            print(f"  ✓ Signal #{signal_id} ({symbol} {parsed.get('strike')} {parsed.get('option_type')}): Added expiry = {expiry}, set processed=0")
            fixed += 1
        
        except Exception as e:
            print(f"  ✗ Signal #{signal_id}: Error - {e}")
            skipped += 1
    
    conn.commit()
    conn.close()
    
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print("="*80)
    print(f"Fixed: {fixed} signals")
    print(f"Skipped: {skipped} signals")
    print(f"\n{fixed} signals will be processed in next order_placer cycle!")
    
    if fixed > 0:
        print(f"\n{'='*80}")
        print("NEXT STEPS")
        print("="*80)
        print("1. order_placer will pick these up in next cycle (~30 seconds)")
        print("2. Monitor: Get-Content order_placer.log -Wait -Tail 30")
        print("3. Expected: Orders will be placed for all fixed signals")
        print("4. Verify: Check 'orders' table for new entries")

if __name__ == "__main__":
    bulk_fix_expiry()
