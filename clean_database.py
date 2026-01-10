import sqlite3
import os
from datetime import datetime

# Database path
db_path = 'trading.db'

# Check if database exists
if os.path.exists(db_path):
    # Backup first
    backup_name = f'trading_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db'
    os.system(f'cp {db_path} {backup_name}')
    print(f"✓ Backup created: {backup_name}")
    
    # Connect
    db = sqlite3.connect(db_path)
    cursor = db.cursor()
    
    # Check if table exists and count
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='signals'")
    table_exists = cursor.fetchone()
    
    if table_exists:
        cursor.execute("SELECT COUNT(*) FROM signals")
        old_count = cursor.fetchone()[0]
        print(f"📊 Found {old_count} old signals")
    else:
        old_count = 0
        print("📊 No signals table found")
    
    # Drop and recreate with new schema
    cursor.execute("DROP TABLE IF EXISTS signals")
    cursor.execute('''
        CREATE TABLE signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT,
            channel_name TEXT,
            message_id INTEGER,
            raw_text TEXT,
            parsed_data TEXT,
            timestamp TEXT,
            processed INTEGER DEFAULT 0,
            UNIQUE(channel_id, message_id)
        )
    ''')
    db.commit()
    
    # Verify
    cursor.execute("SELECT COUNT(*) FROM signals")
    new_count = cursor.fetchone()[0]
    
    print(f"✓ Database cleaned! {old_count} old signals removed")
    print(f"✓ Fresh database ready with new schema (composite unique key)")
    print(f"✓ Current signals: {new_count}")
    
    db.close()
else:
    print("❌ No database found at 'trading.db'")
    print("Database will be created fresh when telegram_reader starts")

