"""
database.py - Database handler with channel_name support
"""

import sqlite3
import json
from datetime import datetime

class TradingDatabase:
    """Database handler for trading signals and orders"""
    
    def __init__(self, db_path='trading.db'):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()
    
    def _create_tables(self):
        """Create necessary tables if they don't exist"""
        cursor = self.conn.cursor()
        
        # Signals table - stores parsed trading signals
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS signals (
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
        """)
        
        # Orders table - stores placed orders
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER,
                entry_order_id TEXT,
                sl_order_id TEXT,
                tradingsymbol TEXT,
                action TEXT,
                quantity INTEGER,
                entry_price REAL,
                stop_loss REAL,
                trigger_price REAL,
                entry_status TEXT DEFAULT 'PENDING',
                sl_flag TEXT DEFAULT 'TO_BE_PLACED',
                entry_placed_at TEXT,
                entry_filled_at TEXT,
                sl_placed_at TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        """)
        
        self.conn.commit()
        print("[OK] Database tables ready")
    
    def insert_signal(self, channel_id, channel_name, message_id, message_text, parsed_data, timestamp):
        """
        Insert a parsed signal into database
        
        Args:
            channel_id: Telegram channel ID
            channel_name: Telegram channel name
            message_id: Message ID
            message_text: Raw message text
            parsed_data: Parsed signal dictionary
            timestamp: Message timestamp
            
        Returns:
            signal_id if successful, None if duplicate or error
        """
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                INSERT INTO signals 
                (channel_id, channel_name, message_id, raw_text, parsed_data, timestamp, processed)
                VALUES (?, ?, ?, ?, ?, ?, 0)
            """, (
                channel_id,
                channel_name,
                message_id,
                message_text,
                json.dumps(parsed_data),
                timestamp
            ))
            
            self.conn.commit()
            return cursor.lastrowid
            
        except sqlite3.IntegrityError:
            # Duplicate signal
            return None
        except Exception as e:
            print(f"[ERROR] Database insert error: {e}")
            return None
    
    def get_pending_signals(self):
        """Get all unprocessed signals"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM signals 
            WHERE processed = 0
            ORDER BY timestamp ASC
        """)
        return cursor.fetchall()
    
    def mark_signal_processed(self, signal_id):
        """Mark signal as processed"""
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE signals 
            SET processed = 1 
            WHERE id = ?
        """, (signal_id,))
        self.conn.commit()
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()


if __name__ == "__main__":
    # Test database
    db = TradingDatabase()
    print("[OK] Database initialized successfully")
    
    # Test signal insertion
    test_signal = {
        "symbol": "NIFTY",
        "strike": 25900,
        "option_type": "CE",
        "action": "BUY",
        "entry_price": 140,
        "stop_loss": 130
    }
    
    signal_id = db.insert_signal(
        channel_id="-1002380215256",
        channel_name="TEST_CHANNEL",
        message_id=999999,
        message_text="TEST MESSAGE",
        parsed_data=test_signal,
        timestamp=datetime.now().isoformat()
    )
    
    if signal_id:
        print(f"[OK] Test signal inserted: ID {signal_id}")
    else:
        print("[WARN] Signal already exists or error")
    
    db.close()
