
"""
batch_parse_historical.py
Parse all historical messages and export to CSV for manual review
"""

import sqlite3
import csv
import logging
import sys
from jp_channel_parser import JPChannelParser
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('BATCH_PARSE')

def batch_parse_to_csv(db_path='premium_signals.db', output_csv='training_batch.csv'):
    """Parse all historical messages and export to CSV"""
    
    # Check if database exists
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM signals")
        count = cursor.fetchone()[0]
        
        if count == 0:
            logger.error("[ERROR] Database is empty!")
            logger.info("")
            logger.info("You need to fetch messages first:")
            logger.info("  python fetch_historical_messages.py")
            logger.info("")
            logger.info("This will:")
            logger.info("1. Connect to Telegram")
            logger.info("2. Fetch last 30 days of messages from JP channel")
            logger.info("3. Store in jp_signals.db")
            logger.info("4. Then run batch_parse_historical.py again")
            conn.close()
            return
        
        logger.info(f"[FOUND] {count} messages in database")
    except Exception as e:
        logger.error(f"[ERROR] Database not found or empty: {e}")
        logger.info("Run: python fetch_historical_messages.py first!")
        return
    
    # Initialize parser
    logger.info("[INIT] Initializing parser...")
    parser = JPChannelParser(instruments_csv='valid_instruments.csv')
    
    # Load messages
    cursor.execute("SELECT id, raw_text, timestamp FROM signals ORDER BY timestamp")
    messages = cursor.fetchall()
    conn.close()
    
    logger.info(f"[START] Processing {len(messages)} messages...")
    print()
    
    # Prepare CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow([
            'id', 'timestamp', 'raw_message',
            'parsed_symbol', 'parsed_strike', 'parsed_option_type',
            'parsed_entry_price', 'parsed_stop_loss', 'parsed_expiry_date',
            'parsed_tradingsymbol', 'parsed_exchange', 'parsed_quantity',
            'parse_status', 'parser_type',
            # Manual correction columns
            'correct_symbol', 'correct_strike', 'correct_option_type',
            'correct_entry_price', 'correct_stop_loss', 'correct_expiry_date',
            'is_valid', 'notes'
        ])
        
        # Parse each message
        success = 0
        failed = 0
        skipped = 0
        
        for msg_id, raw_text, timestamp in messages:
            # Parse with message date
            result = parser.parse(raw_text, message_date=timestamp)
            
            if result:
                success += 1
                writer.writerow([
                    msg_id, timestamp, raw_text,
                    result.get('symbol', ''),
                    result.get('strike', ''),
                    result.get('option_type', ''),
                    result.get('entry_price', ''),
                    result.get('stop_loss', ''),
                    result.get('expiry_date', ''),
                    result.get('tradingsymbol', ''),
                    result.get('exchange', ''),
                    result.get('quantity', ''),
                    'SUCCESS', result.get('message_type', 'regex'),
                    '', '', '', '', '', '', 'yes', ''
                ])
                print(f"[{msg_id:3d}] ✓ {result['symbol']} {result['strike']} {result['option_type']}")
            
            elif result is None:
                skipped += 1
                writer.writerow([
                    msg_id, timestamp, raw_text,
                    '', '', '', '', '', '', '', '', '',
                    'SKIPPED', 'non_trading',
                    '', '', '', '', '', '', 'skip', 'Update/commentary'
                ])
                print(f"[{msg_id:3d}] - Skipped")
            
            else:
                failed += 1
                writer.writerow([
                    msg_id, timestamp, raw_text,
                    '', '', '', '', '', '', '', '', '',
                    'FAILED', 'none',
                    '', '', '', '', '', '', 'no', 'Manual parsing needed'
                ])
                print(f"[{msg_id:3d}] ✗ Failed")
    
    # Summary
    print()
    print("="*70)
    print("BATCH PARSING COMPLETE")
    print("="*70)
    print(f"Total messages: {len(messages)}")
    print(f"Successfully parsed: {success}")
    print(f"Failed to parse: {failed}")
    print(f"Skipped (non-trading): {skipped}")
    if len(messages) > 0:
        print(f"Success rate: {(success/len(messages)*100):.1f}%")
    print()
    print(f"Output file: {output_csv}")
    print()
    print("NEXT STEPS:")
    print("1. Open training_batch.csv in Excel/Google Sheets")
    print("2. Review parsed signals (verify SUCCESS rows are correct)")
    print("3. Fix FAILED rows (fill correct_* columns)")
    print("4. Set is_valid='yes' for good, 'no' for bad")
    print("5. Save as training_batch_corrected.csv")
    print("6. Run: python import_training_data.py")
    print("="*70)

if __name__ == "__main__":
    batch_parse_to_csv()
