"""
find_databases.py
Find all SQLite database files in the directory
"""

import os
import sqlite3

print("="*80)
print("SEARCHING FOR DATABASE FILES")
print("="*80)
print()

# Look for .db files
db_files = [f for f in os.listdir('.') if f.endswith('.db')]

if not db_files:
    print("❌ No .db files found in current directory")
else:
    print(f"✓ Found {len(db_files)} database file(s):")
    print()
    
    for db_file in db_files:
        size = os.path.getsize(db_file) / 1024  # KB
        print(f"File: {db_file} ({size:.1f} KB)")
        print("-"*80)
        
        try:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            
            # Get tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            if tables:
                print(f"  Tables: {len(tables)}")
                for table in tables:
                    table_name = table[0]
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                    count = cursor.fetchone()[0]
                    print(f"    - {table_name}: {count} rows")
            else:
                print("  No tables (empty database)")
            
            conn.close()
            
        except Exception as e:
            print(f"  Error reading: {e}")
        
        print()

print("="*80)

# Check logs for database references
print("\nChecking telegram reader code for database file...")
print("-"*80)

try:
    with open('telegram_reader_jp_trained.py', 'r', encoding='utf-8') as f:
        content = f.read()
        
    # Look for database file references
    import re
    db_refs = re.findall(r'["\']([^"\']*\.db)["\']', content)
    
    if db_refs:
        print("Database files referenced in code:")
        for ref in set(db_refs):
            print(f"  - {ref}")
            exists = "✓ EXISTS" if os.path.exists(ref) else "✗ NOT FOUND"
            print(f"    {exists}")
    else:
        print("No database file references found in telegram_reader_jp_trained.py")
        
except FileNotFoundError:
    print("telegram_reader_jp_trained.py not found")

print()
print("="*80)
