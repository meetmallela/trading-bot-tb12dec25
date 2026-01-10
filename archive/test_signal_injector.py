"""
test_signal_injector.py - Simulate incoming Telegram messages

Tests the complete pipeline:
1. Parse test messages
2. Insert into signals table
3. Watch order_placer pick them up
4. Verify orders are placed

Usage:
    python test_signal_injector.py
"""

import sqlite3
import json
from datetime import datetime
import logging

# Use the same parser as telegram_reader
try:
    from signal_parser_enhanced_v2 import EnhancedSignalParser
    parser_type = "EnhancedSignalParser"
except:
    try:
        from signal_parser_with_claude_fallback import SignalParserWithClaudeFallback
        parser_type = "SignalParserWithClaudeFallback"
        # You'll need to add your Claude API key here if using this parser
        CLAUDE_API_KEY = "sk-ant-api03-YOUR_KEY_HERE"
    except:
        print("ERROR: No parser found!")
        exit(1)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - TEST_INJECTOR - %(message)s'
)

# Test messages - good variety of formats
TEST_MESSAGES = [
    {
        "message": """NIFTY 25900 CE
BUY ABOVE 140
SL 130
TARGET 150/160/170""",
        "channel_id": "-1002380215256",
        "channel_name": "TEST_NIFTY",
        "description": "Simple NIFTY call"
    },
    {
        "message": """COMMODITY MCX TRADE
BUY GOLD 138000 CE
NEAR LEVEL - 1300
TARGET - 1400/1500/1800
STOPLOSS - 1100
EXPIRY - DECEMBER""",
        "channel_id": "-1002770917134",
        "channel_name": "TEST_MCX",
        "description": "MCX GOLD trade"
    },
    {
        "message": """BANKNIFTY 56000 PE
Above 180
Sl 170
Target 200/210/220""",
        "channel_id": "-1002380215256",
        "channel_name": "TEST_BANKNIFTY",
        "description": "BANKNIFTY put"
    },
    {
        "message": """BUY SILVER 206000 CE
NEAR LEVEL - 3100
TARGET - 5000/8000/9000
STOPLOSS - 2700
EXPIRY - DECEMBER""",
        "channel_id": "-1002770917134",
        "channel_name": "TEST_MCX",
        "description": "MCX SILVER trade"
    },
    {
        "message": """SENSEX 85700 CE
Above 120
Sl 110
Target 135/140""",
        "channel_id": "-1002380215256",
        "channel_name": "TEST_SENSEX",
        "description": "SENSEX call"
    }
]


class TestSignalInjector:
    """Inject test signals into database"""
    
    def __init__(self):
        self.db_path = 'trading.db'
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        
        # Initialize parser
        if parser_type == "EnhancedSignalParser":
            self.parser = EnhancedSignalParser(
                rules_file='parsing_rules_enhanced_v2.json',
                instruments_cache='instruments_cache.csv'
            )
        else:
            self.parser = SignalParserWithClaudeFallback(
                claude_api_key=CLAUDE_API_KEY,
                rules_file='parsing_rules_enhanced_v2.json'
            )
        
        logging.info(f"[OK] Using parser: {parser_type}")
    
    def inject_test_signals(self):
        """Parse and inject test messages"""
        
        logging.info("="*80)
        logging.info("TEST SIGNAL INJECTOR - Starting")
        logging.info("="*80)
        
        successful = 0
        failed = 0
        
        for i, test in enumerate(TEST_MESSAGES, 1):
            logging.info(f"\n[TEST {i}/{len(TEST_MESSAGES)}] {test['description']}")
            logging.info("-"*80)
            logging.info(f"Message:\n{test['message']}")
            logging.info("-"*80)
            
            # Parse the message
            parsed_data = self.parser.parse(test['message'], channel_id=test['channel_id'])
            
            if parsed_data:
                logging.info(f"[✓ PARSED] Successfully parsed!")
                self._print_parsed_data(parsed_data)
                
                # Insert into database
                signal_id = self._insert_signal(
                    test['channel_id'],
                    test['channel_name'],
                    i,  # message_id
                    test['message'],
                    parsed_data
                )
                
                if signal_id:
                    logging.info(f"[✓ INSERTED] Signal ID: {signal_id}")
                    successful += 1
                else:
                    logging.error(f"[✗ FAILED] Could not insert into database")
                    failed += 1
            else:
                logging.error(f"[✗ FAILED] Could not parse message")
                failed += 1
            
            logging.info("")
        
        # Summary
        logging.info("="*80)
        logging.info("SUMMARY")
        logging.info("="*80)
        logging.info(f"✓ Successful: {successful}")
        logging.info(f"✗ Failed: {failed}")
        logging.info(f"Total: {len(TEST_MESSAGES)}")
        logging.info("="*80)
        
        if successful > 0:
            logging.info("\n[NEXT STEPS]")
            logging.info("1. Check signals table:")
            logging.info("   sqlite3 trading.db \"SELECT id, json_extract(parsed_data, '$.symbol'), json_extract(parsed_data, '$.strike'), processed FROM signals ORDER BY id DESC LIMIT 5;\"")
            logging.info("\n2. Watch order_placer log:")
            logging.info("   Get-Content order_placer.log -Wait -Tail 20")
            logging.info("\n3. Order placer should pick up these signals within 30 seconds!")
        
        return successful, failed
    
    def _print_parsed_data(self, data):
        """Print parsed data in readable format"""
        logging.info("Parsed fields:")
        for key in ['symbol', 'strike', 'option_type', 'action', 'entry_price', 'stop_loss', 'expiry_date', 'quantity']:
            value = data.get(key, 'MISSING')
            symbol = "✓" if value != 'MISSING' else "✗"
            logging.info(f"  {symbol} {key:15s}: {value}")
    
    def _insert_signal(self, channel_id, channel_name, message_id, message_text, parsed_data):
        """Insert signal into database"""
        try:
            cursor = self.conn.cursor()
            now = datetime.now().isoformat()
            
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
                now
            ))
            
            self.conn.commit()
            return cursor.lastrowid
            
        except sqlite3.IntegrityError:
            logging.warning("[WARN] Signal already exists (duplicate)")
            return None
        except Exception as e:
            logging.error(f"[ERROR] Database error: {e}")
            return None
    
    def check_pending_signals(self):
        """Check how many signals are pending"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM signals WHERE processed = 0")
        count = cursor.fetchone()[0]
        
        logging.info(f"\n[INFO] Pending signals: {count}")
        
        if count > 0:
            cursor.execute("""
                SELECT id, json_extract(parsed_data, '$.symbol'), 
                       json_extract(parsed_data, '$.strike'),
                       json_extract(parsed_data, '$.option_type')
                FROM signals WHERE processed = 0 
                ORDER BY id DESC LIMIT 10
            """)
            
            logging.info("\nPending signals:")
            for row in cursor.fetchall():
                logging.info(f"  Signal {row[0]}: {row[1]} {row[2]} {row[3]}")
        
        return count
    
    def check_recent_orders(self):
        """Check if any orders were placed"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT signal_id, tradingsymbol, entry_order_id, entry_status
            FROM orders 
            WHERE created_at >= datetime('now', '-5 minutes')
            ORDER BY id DESC LIMIT 10
        """)
        
        orders = cursor.fetchall()
        
        if orders:
            logging.info(f"\n[INFO] Recent orders: {len(orders)}")
            logging.info("\nRecent orders:")
            for row in orders:
                logging.info(f"  Signal {row[0]}: {row[1]} | Order: {row[2]} | Status: {row[3]}")
        else:
            logging.info("\n[INFO] No orders placed yet (order_placer may not be running)")
        
        return len(orders)


def main():
    """Main function"""
    
    injector = TestSignalInjector()
    
    # Inject test signals
    successful, failed = injector.inject_test_signals()
    
    # Check status
    if successful > 0:
        logging.info("\n" + "="*80)
        logging.info("MONITORING")
        logging.info("="*80)
        
        # Check pending signals
        pending = injector.check_pending_signals()
        
        # Check if orders were placed
        orders = injector.check_recent_orders()
        
        logging.info("\n" + "="*80)
        logging.info("STATUS")
        logging.info("="*80)
        logging.info(f"Pending signals: {pending}")
        logging.info(f"Orders placed: {orders}")
        
        if pending > 0 and orders == 0:
            logging.info("\n[NOTE] Signals are pending but no orders placed yet.")
            logging.info("Make sure order_placer is running:")
            logging.info("  python order_placer_db_enhanced.py --continuous")
    
    injector.conn.close()


if __name__ == "__main__":
    main()
