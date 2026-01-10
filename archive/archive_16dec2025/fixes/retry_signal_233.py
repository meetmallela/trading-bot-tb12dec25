"""
retry_signal_233.py - Quick script to retry signal #233
"""

import sqlite3

def retry_signal():
    conn = sqlite3.connect('trading.db')
    cursor = conn.cursor()
    
    # Mark signal #233 as unprocessed
    cursor.execute("UPDATE signals SET processed = 0 WHERE id = 233")
    
    conn.commit()
    
    # Verify
    cursor.execute("SELECT id, processed FROM signals WHERE id = 233")
    result = cursor.fetchone()
    
    print("\n" + "="*60)
    print("SIGNAL #233 MARKED FOR RETRY")
    print("="*60)
    print(f"Signal ID: {result[0]}")
    print(f"Processed: {result[1]} (0 = will be picked up)")
    print("\n" + "="*60)
    print("NEXT STEPS:")
    print("="*60)
    print("1. Ensure order_placer is running with UPDATED file")
    print("2. order_placer will process it in ~30 seconds")
    print("3. Monitor: Get-Content order_placer.log -Wait -Tail 20")
    print("\nExpected output:")
    print("  [TASK] Processing Signal #233: NATURALGAS 360 PE")
    print("  [INFO] Looking for MCX instrument")
    print("  [OK] Found instrument: NATURALGAS25DEC360PE")
    print("  [SEND] Placing ENTRY order...")
    print("     Quantity: 1250 (= 1 lot NATURALGAS)")
    print("  [OK] Entry order placed: 240XXXXXXXX")
    print("="*60 + "\n")
    
    conn.close()

if __name__ == "__main__":
    retry_signal()
