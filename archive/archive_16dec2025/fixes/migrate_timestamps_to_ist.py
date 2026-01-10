"""
migrate_timestamps_to_ist.py - Convert existing UTC timestamps to IST
Run this script ONCE to convert your existing database
"""

import sqlite3
from datetime import datetime, timedelta

IST_OFFSET = timedelta(hours=5, minutes=30)

def migrate_timestamps():
    """Convert all UTC timestamps in database to IST"""
    
    db_name = 'trading.db'
    
    print("\n" + "="*80)
    print("TIMESTAMP MIGRATION: UTC → IST")
    print("="*80)
    
    try:
        # Connect to database
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        
        # Check if signals table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='signals'")
        if not cursor.fetchone():
            print("\n❌ Error: 'signals' table not found in database")
            print("   Make sure trading.db exists and has data")
            return
        
        # Get all signals
        cursor.execute("SELECT id, timestamp FROM signals ORDER BY id")
        signals = cursor.fetchall()
        
        if len(signals) == 0:
            print("\n⚠️  No signals found in database")
            print("   Database is empty - nothing to migrate")
            conn.close()
            return
        
        print(f"\n📊 Found {len(signals)} signals in database")
        
        # Show sample of current timestamps
        print(f"\n📅 Sample current timestamps (first 5):")
        for i, (signal_id, timestamp) in enumerate(signals[:5], 1):
            print(f"   {i}. Signal #{signal_id}: {timestamp}")
        
        # Calculate what they'll become
        print(f"\n📅 After conversion to IST (+5:30):")
        for i, (signal_id, timestamp) in enumerate(signals[:5], 1):
            try:
                if timestamp and len(timestamp) >= 19:
                    utc_dt = datetime.strptime(timestamp[:19], '%Y-%m-%d %H:%M:%S')
                    ist_dt = utc_dt + IST_OFFSET
                    ist_str = ist_dt.strftime('%Y-%m-%d %H:%M:%S')
                    print(f"   {i}. Signal #{signal_id}: {ist_str}")
            except:
                print(f"   {i}. Signal #{signal_id}: [invalid format]")
        
        # Ask for confirmation
        print("\n" + "="*80)
        print("⚠️  WARNING: This will modify ALL timestamps in your database!")
        print("   - Current timestamps will be converted from UTC to IST")
        print("   - Each timestamp will have 5 hours 30 minutes added")
        print("   - This operation CANNOT be undone")
        print("   - Make sure you have a backup of trading.db if needed")
        print("="*80)
        
        response = input("\nDo you want to proceed with the migration? Type 'YES' to confirm: ")
        
        if response != 'YES':
            print("\n❌ Migration cancelled")
            print("   No changes were made to the database")
            conn.close()
            return
        
        print("\n🔄 Starting migration...")
        
        # Convert each timestamp
        converted = 0
        errors = 0
        
        for signal_id, timestamp_str in signals:
            try:
                # Parse UTC timestamp
                if timestamp_str and len(timestamp_str) >= 19:
                    utc_dt = datetime.strptime(timestamp_str[:19], '%Y-%m-%d %H:%M:%S')
                    
                    # Add IST offset
                    ist_dt = utc_dt + IST_OFFSET
                    ist_str = ist_dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Update database
                    cursor.execute("""
                        UPDATE signals 
                        SET timestamp = ? 
                        WHERE id = ?
                    """, (ist_str, signal_id))
                    
                    converted += 1
                    
                    # Show progress
                    if converted % 10 == 0:
                        print(f"   Converted {converted}/{len(signals)}... ", end='', flush=True)
                        if converted % 50 == 0:
                            print()
                else:
                    errors += 1
                    print(f"\n   ⚠️  Skipping signal {signal_id}: invalid timestamp format")
                
            except Exception as e:
                errors += 1
                print(f"\n   ⚠️  Error converting signal {signal_id}: {e}")
        
        print()  # New line after progress
        
        # Commit changes
        print("\n💾 Saving changes to database...")
        conn.commit()
        
        # Verify conversion
        cursor.execute("SELECT id, timestamp FROM signals ORDER BY id LIMIT 5")
        updated = cursor.fetchall()
        
        print(f"\n✅ Migration completed!")
        print(f"   - Successfully converted: {converted}")
        if errors > 0:
            print(f"   - Errors/Skipped: {errors}")
        
        print(f"\n📅 Updated timestamps (first 5):")
        for signal_id, timestamp in updated:
            print(f"   Signal #{signal_id}: {timestamp}")
        
        conn.close()
        
        print("\n" + "="*80)
        print("✅ All timestamps are now in IST!")
        print("   Your queries will now show times in Indian Standard Time")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        print("   No changes were made to the database")


def verify_migration():
    """Verify that timestamps are in IST format"""
    
    print("\n" + "="*80)
    print("VERIFICATION: Checking if timestamps are in IST")
    print("="*80)
    
    try:
        conn = sqlite3.connect('trading.db')
        cursor = conn.cursor()
        
        # Get some recent signals
        cursor.execute("SELECT id, timestamp FROM signals ORDER BY id DESC LIMIT 10")
        signals = cursor.fetchall()
        
        if not signals:
            print("\n⚠️  No signals in database")
            return
        
        print(f"\n📊 Recent signals (last 10):")
        for signal_id, timestamp in signals:
            print(f"   Signal #{signal_id}: {timestamp}")
        
        # Check if times look reasonable for IST
        # IST business hours would be 09:30 - 15:30
        print(f"\n💡 If these times show 09:30-15:30 range, they're in IST")
        print(f"   If they show 04:00-10:00 range, they're still in UTC")
        
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Verification failed: {e}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--verify':
        verify_migration()
    else:
        migrate_timestamps()
        