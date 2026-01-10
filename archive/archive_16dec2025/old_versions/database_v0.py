"""
database.py - SQLite database for storing trading signals
Simple implementation for telegram signal storage
"""

import sqlite3
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class TradingDatabase:
    """Simple SQLite database for storing trading signals"""
    
    def __init__(self, db_name="trading.db"):
        self.db_name = db_name
        self.connection = None
        self.cursor = None
        self._connect()
        self._create_tables()
    
    def _connect(self):
        """Connect to SQLite database"""
        try:
            self.connection = sqlite3.connect(self.db_name)
            self.cursor = self.connection.cursor()
            logger.info(f"Connected to database: {self.db_name}")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def _create_tables(self):
        """Create necessary tables if they don't exist"""
        
        # Signals table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT UNIQUE,
                channel_id TEXT,
                raw_text TEXT,
                parsed_data TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                processed INTEGER DEFAULT 0
            )
        """)
        
        # Orders table (optional, for future use)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER,
                order_id TEXT,
                symbol TEXT,
                action TEXT,
                quantity INTEGER,
                entry_price REAL,
                stop_loss REAL,
                status TEXT DEFAULT 'PENDING',
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (signal_id) REFERENCES signals(id)
            )
        """)
        
        self.connection.commit()
        logger.info("Database tables ready")
    
    def insert_signal(self, message_id, channel_id, raw_text, parsed_data):
        """
        Insert a new signal into database
        
        Args:
            message_id: Telegram message ID
            channel_id: Telegram channel ID
            raw_text: Original message text
            parsed_data: Parsed signal dictionary
            
        Returns:
            signal_id if inserted, None if duplicate
        """
        try:
            # Convert parsed_data dict to JSON string
            parsed_json = json.dumps(parsed_data)
            
            self.cursor.execute("""
                INSERT INTO signals (message_id, channel_id, raw_text, parsed_data)
                VALUES (?, ?, ?, ?)
            """, (message_id, channel_id, raw_text, parsed_json))
            
            self.connection.commit()
            signal_id = self.cursor.lastrowid
            
            logger.info(f"Signal inserted: ID={signal_id}")
            return signal_id
            
        except sqlite3.IntegrityError:
            # Duplicate message_id
            logger.debug(f"Duplicate signal: message_id={message_id}")
            return None
        except Exception as e:
            logger.error(f"Failed to insert signal: {e}")
            return None
    
    def get_unprocessed_signals(self, limit=100):
        """
        Get unprocessed signals from database
        
        Returns:
            List of signal dictionaries
        """
        try:
            self.cursor.execute("""
                SELECT id, message_id, channel_id, raw_text, parsed_data, timestamp
                FROM signals
                WHERE processed = 0
                ORDER BY timestamp ASC
                LIMIT ?
            """, (limit,))
            
            rows = self.cursor.fetchall()
            
            signals = []
            for row in rows:
                signal = {
                    'id': row[0],
                    'message_id': row[1],
                    'channel_id': row[2],
                    'raw_text': row[3],
                    'parsed_data': json.loads(row[4]),
                    'timestamp': row[5]
                }
                signals.append(signal)
            
            return signals
            
        except Exception as e:
            logger.error(f"Failed to get unprocessed signals: {e}")
            return []
    
    def mark_signal_processed(self, signal_id, success=True, error=None):
        """
        Mark a signal as processed
        
        Args:
            signal_id: Signal ID
            success: Whether processing was successful
            error: Error message if failed
        """
        try:
            self.cursor.execute("""
                UPDATE signals
                SET processed = 1
                WHERE id = ?
            """, (signal_id,))
            
            self.connection.commit()
            logger.info(f"Signal {signal_id} marked as processed")
            
        except Exception as e:
            logger.error(f"Failed to mark signal processed: {e}")
    
    def get_signal_count(self):
        """Get total number of signals"""
        try:
            self.cursor.execute("SELECT COUNT(*) FROM signals")
            count = self.cursor.fetchone()[0]
            return count
        except Exception as e:
            logger.error(f"Failed to get signal count: {e}")
            return 0
    
    def get_today_signals(self):
        """Get signals from today"""
        try:
            self.cursor.execute("""
                SELECT COUNT(*)
                FROM signals
                WHERE DATE(timestamp) = DATE('now')
            """)
            count = self.cursor.fetchone()[0]
            return count
        except Exception as e:
            logger.error(f"Failed to get today's signal count: {e}")
            return 0
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")


# ==================== TESTING ====================

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Test database
    db = TradingDatabase("test_trading.db")
    
    # Test insert
    test_signal = {
        'action': 'BUY',
        'symbol': 'NIFTY',
        'strike': 24200,
        'option_type': 'CE',
        'entry_price': 165,
        'stop_loss': 150,
        'targets': [180, 195]
    }
    
    signal_id = db.insert_signal(
        message_id="test_123",
        channel_id="-1001234567890",
        raw_text="BUY NIFTY 24200 CE @ 165 SL 150 TGT 180, 195",
        parsed_data=test_signal
    )
    
    print(f"\n✅ Signal inserted with ID: {signal_id}")
    
    # Test retrieve
    signals = db.get_unprocessed_signals()
    print(f"\n✅ Found {len(signals)} unprocessed signals")
    
    if signals:
        print(f"\nSample signal:")
        print(json.dumps(signals[0], indent=2))
    
    # Test counts
    print(f"\n✅ Total signals: {db.get_signal_count()}")
    print(f"✅ Today's signals: {db.get_today_signals()}")
    
    db.close()
    
    print("\n🎉 Database test successful!")
