"""
Add instrument_type column to signals table
"""

import sqlite3

print("Adding instrument_type column to database...")

try:
    conn = sqlite3.connect('trading.db')
    cursor = conn.cursor()
    
    # Add column
    cursor.execute("ALTER TABLE signals ADD COLUMN instrument_type TEXT DEFAULT 'OPTIONS'")
    
    conn.commit()
    conn.close()
    
    print("✅ SUCCESS! Column added.")
    print("\nVerifying...")
    
    # Verify
    conn = sqlite3.connect('trading.db')
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(signals)")
    columns = cursor.fetchall()
    
    print("\nCurrent columns in 'signals' table:")
    for col in columns:
        print(f"  - {col[1]} ({col[2]})")
    
    conn.close()
    
    print("\n✅ Database updated successfully!")
    print("You can now restart telegram_reader_production.py")
    
except sqlite3.OperationalError as e:
    if "duplicate column name" in str(e):
        print("✅ Column already exists! No action needed.")
    else:
        print(f"❌ Error: {e}")
except Exception as e:
    print(f"❌ Error: {e}")
