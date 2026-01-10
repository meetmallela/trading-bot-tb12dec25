"""
import_training_data.py
Import manually corrected training data into Knowledge Base
"""

import csv
import sqlite3
import json
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('IMPORT_KB')

def import_corrections_to_kb(csv_path='training_batch_corrected.csv', kb_db='jp_kb.db'):
    """Import corrected training data into KB"""
    
    logger.info(f"[START] Importing from {csv_path}")
    
    # Create KB database
    conn = sqlite3.connect(kb_db)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS training_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_message TEXT,
            parsed_data JSON,
            correct_data JSON,
            is_valid TEXT,
            notes TEXT,
            timestamp TEXT,
            channel_id TEXT
        )
    ''')
    conn.commit()
    
    # Read CSV
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        imported = 0
        skipped_count = 0
        corrected_count = 0
        
        for row in reader:
            # Skip actual skip/commentary rows
            if row['parse_status'] == 'SKIPPED':
                skipped_count += 1
                continue
            
            # Check if valid
            is_valid = row['is_valid'].strip().lower()
            
            # Determine if this is a correction or just confirmation
            has_correction = bool(row['correct_expiry_date'].strip())
            
            # If expiry was corrected, it's a valid training example!
            # Even if is_valid says "No", the correction itself is valuable
            if not has_correction and is_valid not in ['yes', 'y', '1', 'true']:
                skipped_count += 1
                continue
            
            # Build correct data
            if has_correction:
                # User made corrections (expiry date)
                corrected_count += 1
                correct_data = {
                    'symbol': row['parsed_symbol'] if not row['correct_symbol'].strip() else row['correct_symbol'].strip().upper(),
                    'strike': int(row['parsed_strike']) if row['parsed_strike'] else 0,
                    'option_type': row['parsed_option_type'] if not row['correct_option_type'].strip() else row['correct_option_type'].strip().upper(),
                    'entry_price': float(row['parsed_entry_price']) if row['parsed_entry_price'] else 0,
                    'stop_loss': float(row['parsed_stop_loss']) if row['parsed_stop_loss'] else 0,
                    'expiry_date': row['correct_expiry_date'].strip(),
                    'action': 'BUY'
                }
            elif row['parse_status'] == 'SUCCESS':
                # Parser was correct, use parsed data
                correct_data = {
                    'symbol': row['parsed_symbol'],
                    'strike': int(row['parsed_strike']) if row['parsed_strike'] else 0,
                    'option_type': row['parsed_option_type'],
                    'entry_price': float(row['parsed_entry_price']) if row['parsed_entry_price'] else 0,
                    'stop_loss': float(row['parsed_stop_loss']) if row['parsed_stop_loss'] else 0,
                    'expiry_date': row['parsed_expiry_date'],
                    'action': 'BUY'
                }
            else:
                # Failed to parse and no correction
                skipped_count += 1
                continue
            
            # Parsed data (what parser thought)
            parsed_data = None
            if row['parse_status'] == 'SUCCESS':
                parsed_data = {
                    'symbol': row['parsed_symbol'],
                    'strike': int(row['parsed_strike']) if row['parsed_strike'] else 0,
                    'option_type': row['parsed_option_type'],
                    'entry_price': float(row['parsed_entry_price']) if row['parsed_entry_price'] else 0,
                    'stop_loss': float(row['parsed_stop_loss']) if row['parsed_stop_loss'] else 0,
                    'expiry_date': row['parsed_expiry_date']
                }
            
            # Insert into KB
            conn.execute("""
                INSERT INTO training_data 
                (raw_message, parsed_data, correct_data, is_valid, notes, timestamp, channel_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                row['raw_message'],
                json.dumps(parsed_data) if parsed_data else None,
                json.dumps(correct_data),
                row['is_valid'],
                row.get('notes', ''),
                row.get('timestamp', datetime.now().isoformat()),
                '-1003282204738'
            ))
            
            imported += 1
            
            # Log progress
            if imported % 10 == 0:
                logger.info(f"  Imported {imported} examples...")
        
        conn.commit()
        conn.close()
        
        print("")
        print("="*70)
        print("TRAINING DATA IMPORT COMPLETE")
        print("="*70)
        print(f"Total rows processed: {imported + skipped_count}")
        print(f"Imported to KB: {imported}")
        print(f"  - Parser correct: {imported - corrected_count}")
        print(f"  - User corrected: {corrected_count}")
        print(f"Skipped (invalid): {skipped_count}")
        print("")
        print(f"Knowledge Base: {kb_db}")
        print(f"Training examples: {imported}")
        print("")
        print("KEY LEARNINGS FOR CLAUDE:")
        
        # Analyze corrections
        if corrected_count > 0:
            print(f"  - {corrected_count} corrections made by user")
            print("  - Most common: Expiry date calculations")
            print("  - NIFTY: Nearest Thursday from message date")
            print("  - SENSEX: Nearest Friday from message date")
            print("  - Stocks: Last Thursday of message month")
        
        print("")
        print("NEXT STEPS:")
        print("1. Test the trained parser:")
        print("   python -c \"from jp_channel_agent_trained import JPChannelAgentTrained; agent = JPChannelAgentTrained('YOUR_API_KEY'); print(agent.parse('CDSL 1640 CE 51.5'))\"")
        print("")
        print("2. Deploy trained telegram reader:")
        print("   python telegram_reader_jp_trained.py")
        print("")
        print("="*70)


if __name__ == "__main__":
    import sys
    
    csv_file = 'training_batch_corrected.csv'
    kb_file = 'jp_kb.db'
    
    # Check if CSV exists
    import os
    if not os.path.exists(csv_file):
        print(f"[ERROR] {csv_file} not found!")
        print("")
        print("Make sure you:")
        print("1. Ran: python batch_parse_historical.py")
        print("2. Reviewed: training_batch.csv in Excel")
        print("3. Saved as: training_batch_corrected.csv")
        sys.exit(1)
    
    import_corrections_to_kb(csv_file, kb_file)
