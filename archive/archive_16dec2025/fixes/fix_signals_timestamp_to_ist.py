"""
fix_signals_timestamp_to_ist.py - Convert GMT timestamps to IST in signals table
Temporary fix: Converts existing data from GMT to IST (+5:30)
"""

import sqlite3
from datetime import datetime, timedelta

def convert_timestamps_to_ist(db_path='trading.db'):
    """Convert all timestamps in signals table from GMT to IST"""
    
    print("\n" + "="*80)
    print("CONVERT SIGNALS TIMESTAMPS: GMT → IST")
    print("="*80)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check if timestamp column exists
    cursor.execute("PRAGMA table_info(signals)")
    columns = [row[1] for row in cursor.fetchall()]
    
    if 'timestamp' not in columns:
        print("[ERROR] 'timestamp' column not found in signals table")
        conn.close()
        return False
    
    # Count total records
    cursor.execute("SELECT COUNT(*) FROM signals")
    total = cursor.fetchone()[0]
    
    if total == 0:
        print("[INFO] No signals to convert")
        conn.close()
        return True
    
    print(f"\nFound {total} signals to convert")
    
    # Show sample BEFORE
    print("\n" + "-"*80)
    print("BEFORE CONVERSION (Sample):")
    print("-"*80)
    cursor.execute("SELECT id, timestamp FROM signals ORDER BY id DESC LIMIT 5")
    for row in cursor.fetchall():
        print(f"Signal #{row[0]}: {row[1]} (GMT)")
    
    # Ask for confirmation
    print("\n" + "-"*80)
    response = input("Convert all timestamps from GMT to IST? (yes/no): ").strip().lower()
    if response != 'yes':
        print("Conversion cancelled")
        conn.close()
        return False
    
    # Convert timestamps
    print("\n[INFO] Converting timestamps...")
    
    try:
        # Get all records
        cursor.execute("SELECT id, timestamp FROM signals")
        records = cursor.fetchall()
        
        converted_count = 0
        failed_count = 0
        
        for signal_id, timestamp_str in records:
            try:
                # Parse GMT timestamp
                # Handle different formats
                formats_to_try = [
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%d %H:%M:%S.%f',
                    '%Y-%m-%dT%H:%M:%S',
                    '%Y-%m-%dT%H:%M:%S.%f'
                ]
                
                gmt_dt = None
                for fmt in formats_to_try:
                    try:
                        gmt_dt = datetime.strptime(timestamp_str.split('.')[0], fmt.split('.')[0])
                        break
                    except:
                        continue
                
                if not gmt_dt:
                    print(f"[WARNING] Could not parse timestamp for signal {signal_id}: {timestamp_str}")
                    failed_count += 1
                    continue
                
                # Add 5 hours 30 minutes for IST
                ist_dt = gmt_dt + timedelta(hours=5, minutes=30)
                ist_str = ist_dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # Update record
                cursor.execute("UPDATE signals SET timestamp = ? WHERE id = ?", (ist_str, signal_id))
                converted_count += 1
                
                if converted_count % 100 == 0:
                    print(f"[INFO] Converted {converted_count}/{total} signals...")
                
            except Exception as e:
                print(f"[ERROR] Failed to convert signal {signal_id}: {e}")
                failed_count += 1
        
        conn.commit()
        
        print(f"\n[OK] Conversion complete!")
        print(f"  Converted: {converted_count}")
        print(f"  Failed: {failed_count}")
        
        # Show sample AFTER
        print("\n" + "-"*80)
        print("AFTER CONVERSION (Sample):")
        print("-"*80)
        cursor.execute("SELECT id, timestamp FROM signals ORDER BY id DESC LIMIT 5")
        for row in cursor.fetchall():
            print(f"Signal #{row[0]}: {row[1]} (IST)")
        
        print("\n" + "="*80)
        print("TIMESTAMPS CONVERTED TO IST")
        print("="*80)
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"\n[ERROR] Conversion failed: {e}")
        conn.rollback()
        conn.close()
        return False


def verify_conversion(db_path='trading.db'):
    """Verify timestamps are in IST range"""
    print("\n" + "="*80)
    print("VERIFICATION - Check if timestamps are in IST")
    print("="*80)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check hour distribution
    cursor.execute("""
        SELECT 
            CAST(strftime('%H', timestamp) AS INTEGER) as hour,
            COUNT(*) as count
        FROM signals
        GROUP BY hour
        ORDER BY hour
    """)
    
    print("\nSignals by hour:")
    print("-"*40)
    
    ist_hours = 0
    gmt_hours = 0
    
    for hour, count in cursor.fetchall():
        print(f"Hour {hour:02d}: {count} signals")
        
        # IST trading hours: 9-15 (9:30 AM - 3:30 PM)
        if 9 <= hour <= 15:
            ist_hours += count
        # GMT equivalent would be: 4-10 (4:00 AM - 10:00 AM GMT)
        elif 4 <= hour <= 10:
            gmt_hours += count
    
    print("-"*40)
    
    if ist_hours > gmt_hours:
        print(f"\n[OK] Timestamps appear to be in IST")
        print(f"     Signals in IST hours (9-15): {ist_hours}")
        print(f"     Signals in GMT hours (4-10): {gmt_hours}")
    else:
        print(f"\n[WARNING] Timestamps might still be in GMT")
        print(f"     Signals in IST hours (9-15): {ist_hours}")
        print(f"     Signals in GMT hours (4-10): {gmt_hours}")
    
    conn.close()


def main():
    db_path = 'trading.db'
    
    # Create backup first
    import shutil
    import os
    
    if os.path.exists(db_path):
        backup_path = f"trading_backup_before_ist_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy2(db_path, backup_path)
        print(f"\n[OK] Backup created: {backup_path}")
    
    # Convert
    success = convert_timestamps_to_ist(db_path)
    
    if success:
        # Verify
        verify_conversion(db_path)
        
        print("\n[OK] All done!")
        print("\nNext step: Update telegram_reader to store future timestamps in IST")
        print("Run: Replace your telegram_reader with the IST version")
    else:
        print("\n[ERROR] Conversion failed")


if __name__ == "__main__":
    main()
