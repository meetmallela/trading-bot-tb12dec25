"""
inject_complete_signals.py - Direct SQL injection with COMPLETE signal data

Bypasses parser entirely and injects signals with ALL required fields.
"""

import sqlite3
import json
from datetime import datetime

# Complete signals with ALL 8 required fields - USING REAL STRIKES FROM CSV
COMPLETE_SIGNALS = [
    {
        "channel_id": "-1002380215256",
        "channel_name": "TEST_NIFTY",
        "message_id": 1001,
        "raw_text": "NIFTY 24000 CE BUY ABOVE 140 SL 130",
        "parsed_data": {
            "symbol": "NIFTY",
            "strike": 24000,  # Real strike
            "option_type": "CE",
            "action": "BUY",
            "entry_price": 140.0,
            "stop_loss": 130.0,
            "targets": [150, 160, 170],
            "expiry_date": "2025-12-23",  # Real expiry
            "quantity": 25
        }
    },
    {
        "channel_id": "-1002770917134",
        "channel_name": "TEST_MCX",
        "message_id": 1002,
        "raw_text": "COMMODITY MCX TRADE BUY GOLD 118000 CE",
        "parsed_data": {
            "symbol": "GOLDM",  # Using GOLDM
            "strike": 118000,  # Real strike from CSV
            "option_type": "CE",
            "action": "BUY",
            "entry_price": 1300.0,
            "stop_loss": 1100.0,
            "targets": [1400, 1500, 1800],
            "expiry_date": "2025-12-29",  # Real expiry
            "quantity": 1
        }
    },
    {
        "channel_id": "-1002380215256",
        "channel_name": "TEST_BANKNIFTY",
        "message_id": 1003,
        "raw_text": "BANKNIFTY 52000 PE BUY ABOVE 180",
        "parsed_data": {
            "symbol": "BANKNIFTY",
            "strike": 52000,  # Real strike
            "option_type": "PE",
            "action": "BUY",
            "entry_price": 180.0,
            "stop_loss": 170.0,
            "targets": [200, 210, 220],
            "expiry_date": "2025-12-30",  # Real expiry
            "quantity": 15
        }
    },
    {
        "channel_id": "-1002770917134",
        "channel_name": "TEST_MCX",
        "message_id": 1004,
        "raw_text": "BUY SILVER 110000 CE",
        "parsed_data": {
            "symbol": "SILVER",
            "strike": 110000,  # Real strike from CSV
            "option_type": "CE",
            "action": "BUY",
            "entry_price": 3100.0,
            "stop_loss": 2700.0,
            "targets": [5000, 8000, 9000],
            "expiry_date": "2025-12-24",  # Real expiry
            "quantity": 1
        }
    },
    {
        "channel_id": "-1002380215256",
        "channel_name": "TEST_SENSEX",
        "message_id": 1005,
        "raw_text": "SENSEX 85000 CE BUY ABOVE 120",
        "parsed_data": {
            "symbol": "SENSEX",
            "strike": 85000,  # Real strike
            "option_type": "CE",
            "action": "BUY",
            "entry_price": 120.0,
            "stop_loss": 110.0,
            "targets": [135, 140],
            "expiry_date": "2025-12-18",  # Real expiry
            "quantity": 10
        }
    }
]


def inject_signals():
    """Inject complete signals directly into database"""
    
    conn = sqlite3.connect('trading.db')
    cursor = conn.cursor()
    
    print("="*80)
    print("DIRECT SQL INJECTION - Complete Signals")
    print("="*80)
    
    successful = 0
    
    for i, signal in enumerate(COMPLETE_SIGNALS, 1):
        print(f"\n[{i}/5] Injecting: {signal['parsed_data']['symbol']} "
              f"{signal['parsed_data']['strike']} {signal['parsed_data']['option_type']}")
        
        # Show all fields
        print("  Fields:")
        for key in ['symbol', 'strike', 'option_type', 'action', 'entry_price', 
                    'stop_loss', 'expiry_date', 'quantity']:
            value = signal['parsed_data'].get(key)
            print(f"    ✓ {key:15s}: {value}")
        
        try:
            cursor.execute("""
                INSERT INTO signals 
                (channel_id, channel_name, message_id, raw_text, parsed_data, timestamp, processed)
                VALUES (?, ?, ?, ?, ?, ?, 0)
            """, (
                signal['channel_id'],
                signal['channel_name'],
                signal['message_id'],
                signal['raw_text'],
                json.dumps(signal['parsed_data']),
                datetime.now().isoformat()
            ))
            
            signal_id = cursor.lastrowid
            print(f"  ✓ Inserted as Signal ID: {signal_id}")
            successful += 1
            
        except sqlite3.IntegrityError:
            print(f"  ⚠ Signal already exists (duplicate)")
        except Exception as e:
            print(f"  ✗ Error: {e}")
    
    conn.commit()
    conn.close()
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"✓ Successful: {successful}")
    print(f"Total: {len(COMPLETE_SIGNALS)}")
    print("="*80)
    
    if successful > 0:
        print("\n[NEXT] Start order_placer to process these signals:")
        print("  python order_placer_db_enhanced.py --continuous")
        print("\nOr check pending signals:")
        print('  sqlite3 trading.db "SELECT id, json_extract(parsed_data, \'$.symbol\'), '
              'json_extract(parsed_data, \'$.expiry_date\'), processed FROM signals '
              'WHERE processed = 0;"')


if __name__ == "__main__":
    inject_signals()
