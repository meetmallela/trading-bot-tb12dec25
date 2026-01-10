import sqlite3
import pandas as pd
import json
import os
import logging
from signal_parser_with_claude_fallback import SignalParserWithClaudeFallback

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def run_checks():
    logging.info("--- STARTING PRE-FLIGHT CHECKS ---")
    
    # 1. Check Configuration Files
    files = ['kite_config.json', 'telegram_config.json', 'parsing_rules_enhanced_v2.json', 'valid_instruments.csv']
    for f in files:
        if os.path.exists(f):
            logging.info(f"[✓] Found {f}")
        else:
            logging.error(f"[X] Missing {f}!")

    # 2. Check Database Connectivity
    try:
        conn = sqlite3.connect('trading.db')
        logging.info("[✓] Database 'trading.db' is accessible")
        conn.close()
    except Exception as e:
        logging.error(f"[X] Database error: {e}")

    # 3. Test Mini-Routing Logic (Dry Run)
    logging.info("--- TESTING MINI-ROUTING LOGIC ---")
    parser = SignalParserWithClaudeFallback(claude_api_key="TEST_KEY")
    
    test_messages = [
        "BUY CRUDEOIL 5300. PE NEAR 160",
        "BUY GOLD 75000 CE NEAR 200"
    ]
    
    for msg in test_messages:
        result = parser._extract_with_regex(msg)
        if result and 'M' in result.get('symbol', ''):
            logging.info(f"[✓] SUCCESS: Parsed '{msg}' as {result['symbol']}")
        else:
            logging.error(f"[X] FAILURE: Could not route '{msg}' to Mini contract")

    # 4. Check Instrument File Freshness
    try:
        df = pd.read_csv('valid_instruments.csv')
        sample = df.head(1)
        logging.info(f"[✓] valid_instruments.csv loaded. Total instruments: {len(df)}")
    except Exception as e:
        logging.error(f"[X] Could not read instruments file: {e}")

    logging.info("--- CHECKS COMPLETE ---")

if __name__ == "__main__":
    run_checks()