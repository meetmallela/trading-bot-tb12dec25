"""
check_database.py
Check what tables exist in the database
"""

import sqlite3

DB_FILE = 'telegram_signals.db'

print("="*80)
print("DATABASE STRUCTURE CHECK")
print("="*80)
print()

try:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    if not tables:
        print("❌ No tables found in database!")
    else:
        print(f"✓ Found {len(tables)} table(s):")
        print()
        
        for table in tables:
            table_name = table[0]
            print(f"Table: {table_name}")
            print("-"*80)
            
            # Get column info
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            
            print("Columns:")
            for col in columns:
                col_id, col_name, col_type, not_null, default, pk = col
                print(f"  {col_name:20s} {col_type:15s} {'PK' if pk else ''}")
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cursor.fetchone()[0]
            print(f"\nTotal rows: {count}")
            
            # Show sample data
            if count > 0:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3;")
                samples = cursor.fetchall()
                print("\nSample rows:")
                for i, row in enumerate(samples, 1):
                    print(f"  Row {i}: {row[:5]}...")  # Show first 5 columns
            
            print()
            print("="*80)
            print()
    
    conn.close()
    
except FileNotFoundError:
    print(f"❌ Database file not found: {DB_FILE}")
except Exception as e:
    print(f"❌ Error: {e}")

print("\nNext: Update extract_training_examples.py with correct table name")
