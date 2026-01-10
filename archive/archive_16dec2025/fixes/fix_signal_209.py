"""
fix_signal_209_now.py - Check and fix signal 209 with correct expiry
"""

import sqlite3
import json

def fix_signal_209():
    conn = sqlite3.connect('trading.db')
    cursor = conn.cursor()
    
    print("\n" + "="*80)
    print("CHECKING SIGNAL #209")
    print("="*80)
    
    # Get current state
    cursor.execute("SELECT id, parsed_data, processed FROM signals WHERE id = 209")
    row = cursor.fetchone()
    
    if not row:
        print("Signal #209 not found!")
        conn.close()
        return
    
    signal_id, parsed_data_str, processed = row
    
    print(f"\nCurrent state:")
    print(f"  Processed: {processed}")
    print(f"  Parsed data:")
    
    if parsed_data_str:
        parsed_data = json.loads(parsed_data_str)
        print(json.dumps(parsed_data, indent=4))
        
        # Check if expiry_date exists
        expiry = parsed_data.get('expiry_date')
        print(f"\n  Expiry date: {expiry if expiry else '[MISSING]'}")
        
        if not expiry:
            print("\n" + "="*80)
            print("FIXING: Adding expiry_date")
            print("="*80)
            
            # Add expiry date
            parsed_data['expiry_date'] = '2026-01-27'
            
            # Update database
            cursor.execute("""
                UPDATE signals 
                SET 
                    parsed_data = ?,
                    processed = 0
                WHERE id = 209
            """, (json.dumps(parsed_data),))
            
            conn.commit()
            
            print("\n[OK] Fixed!")
            print("  Added: expiry_date = 2026-01-27")
            print("  Set: processed = 0")
            
            # Verify
            cursor.execute("SELECT parsed_data FROM signals WHERE id = 209")
            new_data = json.loads(cursor.fetchone()[0])
            print("\nNew parsed data:")
            print(json.dumps(new_data, indent=4))
            
        else:
            print(f"\n[INFO] Expiry date already exists: {expiry}")
            
            if processed == 1:
                print("\n[ACTION] Signal is marked as processed.")
                print("Setting processed = 0 to retry...")
                
                cursor.execute("UPDATE signals SET processed = 0 WHERE id = 209")
                conn.commit()
                
                print("[OK] Signal will be retried in next cycle")
    else:
        print("  [NULL] - Signal not parsed!")
    
    conn.close()
    
    print("\n" + "="*80)
    print("NEXT STEPS")
    print("="*80)
    print("1. order_placer will pick this up in next cycle (~30 seconds)")
    print("2. Monitor with: Get-Content order_placer.log -Wait -Tail 20")
    print("3. Look for: Processing Signal #209")
    print("4. Should see: Found tradingsymbol: GOLD26JAN136000CE")
    print("="*80 + "\n")

if __name__ == "__main__":
    fix_signal_209()
