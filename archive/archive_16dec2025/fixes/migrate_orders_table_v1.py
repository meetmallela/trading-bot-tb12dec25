"""
migrate_orders_table.py - Add sl_flag column to existing orders table
Run this once to update your database schema
"""

import sqlite3
import sys
from datetime import datetime

def backup_database(db_path='trading.db'):
    """Create backup of database"""
    import shutil
    backup_path = f"trading_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    try:
        shutil.copy2(db_path, backup_path)
        print(f"Backup created: {backup_path}")
        return True
    except Exception as e:
        print(f"ERROR: Could not create backup: {e}")
        return False

def check_column_exists(conn, table_name, column_name):
    """Check if column exists in table"""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    return column_name in columns

def migrate_orders_table(db_path='trading.db'):
    """Add sl_flag and other missing columns to orders table"""
    
    print("\n" + "="*80)
    print("DATABASE MIGRATION - ORDERS TABLE")
    print("="*80)
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get current table structure
    cursor.execute("PRAGMA table_info(orders)")
    current_columns = {row[1]: row for row in cursor.fetchall()}
    
    print(f"\nCurrent columns in orders table:")
    for col_name in current_columns.keys():
        print(f"  - {col_name}")
    
    # Define required columns
    required_columns = {
        'sl_flag': ('TEXT', 'TO_BE_PLACED'),
        'entry_status': ('TEXT', 'PENDING'),
        'entry_filled_at': ('TEXT', None),
        'sl_placed_at': ('TEXT', None),
        'trigger_price': ('REAL', None),
        'updated_at': ('TEXT', None),
        'created_at': ('TEXT', None)
    }
    
    # Check which columns need to be added
    columns_to_add = []
    for col_name, (col_type, default_value) in required_columns.items():
        if col_name not in current_columns:
            columns_to_add.append((col_name, col_type, default_value))
    
    if not columns_to_add:
        print("\nAll required columns already exist!")
        print("="*80 + "\n")
        conn.close()
        return True
    
    print(f"\nColumns to add:")
    for col_name, col_type, default_value in columns_to_add:
        print(f"  + {col_name} ({col_type}) - default: {default_value}")
    
    # Confirm with user
    response = input("\nProceed with migration? (yes/no): ").strip().lower()
    if response != 'yes':
        print("Migration cancelled")
        conn.close()
        return False
    
    # Add missing columns
    try:
        for col_name, col_type, default_value in columns_to_add:
            if default_value is not None:
                cursor.execute(f"""
                    ALTER TABLE orders 
                    ADD COLUMN {col_name} {col_type} DEFAULT '{default_value}'
                """)
            else:
                cursor.execute(f"""
                    ALTER TABLE orders 
                    ADD COLUMN {col_name} {col_type}
                """)
            print(f"Added column: {col_name}")
        
        # Set default values for existing rows
        now = datetime.now().isoformat()
        
        # Update sl_flag for existing entries without SL
        cursor.execute("""
            UPDATE orders 
            SET sl_flag = CASE 
                WHEN sl_order_id IS NOT NULL THEN 'ORDER_PLACED'
                ELSE 'TO_BE_PLACED'
            END
            WHERE sl_flag IS NULL
        """)
        
        # Update entry_status if NULL
        cursor.execute("""
            UPDATE orders 
            SET entry_status = 'PENDING'
            WHERE entry_status IS NULL
        """)
        
        # Update updated_at if NULL
        cursor.execute("""
            UPDATE orders 
            SET updated_at = ?
            WHERE updated_at IS NULL
        """, (now,))
        
        # Update created_at if NULL
        cursor.execute("""
            UPDATE orders 
            SET created_at = ?
            WHERE created_at IS NULL
        """, (now,))
        
        # Set trigger_price = stop_loss if NULL
        cursor.execute("""
            UPDATE orders 
            SET trigger_price = stop_loss
            WHERE trigger_price IS NULL AND stop_loss IS NOT NULL
        """)
        
        conn.commit()
        
        print("\nMigration completed successfully!")
        
        # Show updated table structure
        cursor.execute("PRAGMA table_info(orders)")
        print("\nUpdated columns in orders table:")
        for row in cursor.fetchall():
            print(f"  - {row[1]} ({row[2]})")
        
        # Show some sample data
        cursor.execute("SELECT COUNT(*) FROM orders")
        count = cursor.fetchone()[0]
        print(f"\nTotal orders in table: {count}")
        
        if count > 0:
            cursor.execute("""
                SELECT entry_order_id, sl_flag, entry_status 
                FROM orders 
                LIMIT 5
            """)
            print("\nSample data:")
            for row in cursor.fetchall():
                print(f"  Order: {row[0]}, SL Flag: {row[1]}, Status: {row[2]}")
        
        print("\n" + "="*80)
        print("DATABASE MIGRATION COMPLETE")
        print("="*80 + "\n")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"\nERROR during migration: {e}")
        conn.rollback()
        conn.close()
        return False

def main():
    db_path = 'trading.db'
    
    print("\nThis script will add missing columns to the orders table")
    print("Required columns: sl_flag, entry_status, entry_filled_at, etc.")
    
    # Check if database exists
    import os
    if not os.path.exists(db_path):
        print(f"\nERROR: Database not found: {db_path}")
        return
    
    # Create backup
    print("\nStep 1: Creating backup...")
    if not backup_database(db_path):
        print("Backup failed. Aborting migration.")
        return
    
    # Run migration
    print("\nStep 2: Running migration...")
    success = migrate_orders_table(db_path)
    
    if success:
        print("\nYou can now run:")
        print("  python order_placer_db_enhanced.py --continuous")
        print("  python sl_monitor.py")
    else:
        print("\nMigration failed. Your database has not been changed.")
        print("Check the error messages above.")

if __name__ == "__main__":
    main()