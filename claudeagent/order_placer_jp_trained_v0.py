"""
order_placer_jp_trained.py
Order placer for JP channel trained signals
Reads from: jp_signals_trained.db
Places orders via Zerodha Kite
"""

import sqlite3
import json
import logging
import time
import argparse
import pandas as pd
import sys
import io
from datetime import datetime
from kiteconnect import KiteConnect

# Fix Windows encoding
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - ORDER_PLACER_JP - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('order_placer_jp.log', encoding='utf-8'),
        logging.StreamHandler(
            io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
            if sys.platform == 'win32' else sys.stdout
        )
    ]
)

def initialize_kite_with_retry(config):
    """Retries connection infinitely to handle internet/DNS flickers."""
    while True:
        try:
            logging.info("[CONNECT] Attempting Kite login...")
            kite = KiteConnect(api_key=config['api_key'])
            kite.set_access_token(config['access_token'])
            kite.profile()
            logging.info("[OK] Kite Connected successfully.")
            return kite
        except Exception as e:
            logging.error(f"[RETRY] Connection failed: {e}. Checking in 10s...")
            time.sleep(10)

class OrderPlacerJPTrained:
    def __init__(self, kite, test_mode=False):
        print("[INFO] Initializing JP Order Placer...")
        self.kite = kite
        self.test_mode = test_mode
        self.db_path = 'jp_signals_trained.db'  # JP database
        
        # Load instruments CSV (optional - parser provides tradingsymbol)
        try:
            print("[INFO] Loading instruments CSV...")
            self.instruments = pd.read_csv('valid_instruments.csv')
            logging.info(f"[OK] Loaded {len(self.instruments)} instruments from CSV")
            print("[OK] CSV loaded successfully")
        except FileNotFoundError:
            print("[WARN] valid_instruments.csv not found - will use tradingsymbol from parser")
            logging.warning("[WARN] CSV not found - relying on parser-provided tradingsymbol")
            self.instruments = pd.DataFrame()
        except Exception as e:
            print(f"[WARN] Failed to load CSV: {e}")
            logging.error(f"[ERROR] Failed to load instruments CSV: {e}")
            self.instruments = pd.DataFrame()
        
        print("[OK] JP Order Placer initialized")
    
    def get_tradingsymbol(self, data):
        """Get tradingsymbol - parser already provides it!"""
        # Parser provides correct tradingsymbol
        if 'tradingsymbol' in data and data['tradingsymbol']:
            tradingsymbol = data['tradingsymbol']
            
            # Validate if tradingsymbol exists in instruments CSV
            if not self.instruments.empty:
                try:
                    # Check if tradingsymbol exists
                    exists = tradingsymbol in self.instruments['tradingsymbol'].values
                    
                    if not exists:
                        logging.warning(f"[WARN] {tradingsymbol} not found in instruments CSV")
                        logging.warning(f"[WARN] This instrument may not be available for trading")
                        logging.warning(f"[WARN] Skipping order to avoid errors")
                        return None
                except Exception as e:
                    logging.warning(f"[WARN] Could not validate instrument: {e}")
                    # Continue anyway - don't block valid orders
            
            return tradingsymbol
        
        # Fallback: build it manually
        try:
            symbol = data['symbol']
            strike = data['strike']
            option_type = data['option_type']
            expiry_date = data['expiry_date']
            
            # Convert expiry to format: 25DEC or 26JAN
            from datetime import datetime
            exp_dt = datetime.strptime(expiry_date, '%Y-%m-%d')
            exp_str = exp_dt.strftime('%y%b').upper()
            
            tradingsymbol = f"{symbol}{exp_str}{strike}{option_type}"
            logging.info(f"[BUILD] Tradingsymbol: {tradingsymbol}")
            return tradingsymbol
            
        except Exception as e:
            logging.error(f"[ERROR] Failed to build tradingsymbol: {e}")
            return None
    
    def place_order_with_retry(self, signal_data, variety='regular', max_retries=3):
        """Place order with network retry logic"""
        
        if not self.test_mode:
            # Validate tradingsymbol exists before placing order
            tradingsymbol = self.get_tradingsymbol(signal_data)
            if not tradingsymbol:
                logging.error(f"[SKIP] No valid tradingsymbol - instrument may not exist")
                return {'success': False, 'error': 'Invalid tradingsymbol'}
        
        tradingsymbol = signal_data.get('tradingsymbol')
        exchange = signal_data.get('exchange', 'NFO')
        action = signal_data.get('action', 'BUY')
        quantity = signal_data.get('quantity', 1)
        
        logging.info(f"[ORDER] {action} {quantity} {tradingsymbol} @ {exchange}")
        
        if self.test_mode:
            logging.info("[TEST MODE] Order would be placed:")
            logging.info(f"  Symbol: {tradingsymbol}")
            logging.info(f"  Exchange: {exchange}")
            logging.info(f"  Action: {action}")
            logging.info(f"  Quantity: {quantity}")
            logging.info(f"  Type: MARKET")
            return {'test_mode': True, 'order_id': 'TEST123'}
        
        # Place order with retry
        for attempt in range(max_retries):
            try:
                order_id = self.kite.place_order(
                    variety=variety,
                    exchange=exchange,
                    tradingsymbol=tradingsymbol,
                    transaction_type=action,
                    quantity=quantity,
                    order_type='MARKET',
                    product='NRML'  # Changed from MIS to NRML
                )
                
                logging.info(f"[SUCCESS] Order placed: {order_id}")
                return {'order_id': order_id, 'success': True}
                
            except Exception as e:
                logging.error(f"[RETRY {attempt+1}/{max_retries}] Order failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    logging.error(f"[FAILED] Order failed after {max_retries} attempts")
                    return {'success': False, 'error': str(e)}
    
    def process_unprocessed_signals(self):
        """Process all unprocessed signals from JP database - with 30-min freshness check"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Get unprocessed signals
            cursor.execute("""
                SELECT * FROM signals 
                WHERE processed = 0 
                ORDER BY id ASC
            """)
            
            signals = cursor.fetchall()
            
            if not signals:
                logging.info("[INFO] No unprocessed signals")
                return
            
            # Check signal freshness (30 minutes = 1800 seconds)
            from datetime import datetime, timedelta
            now = datetime.now()
            cutoff_time = now - timedelta(minutes=30)
            
            fresh_signals = []
            stale_count = 0
            
            for signal in signals:
                # Parse timestamp
                signal_time_str = signal['timestamp']
                try:
                    # Handle different timestamp formats
                    if 'T' in signal_time_str:
                        signal_time = datetime.fromisoformat(signal_time_str.replace('Z', '+00:00'))
                    else:
                        signal_time = datetime.strptime(signal_time_str, '%Y-%m-%d %H:%M:%S')
                    
                    # Check if signal is fresh (within 30 minutes)
                    if signal_time >= cutoff_time:
                        fresh_signals.append(signal)
                    else:
                        stale_count += 1
                        # Mark stale signal as processed to avoid retrying
                        cursor.execute("""
                            UPDATE signals 
                            SET processed = 1 
                            WHERE id = ?
                        """, (signal['id'],))
                        logging.warning(f"[STALE] Signal {signal['id']} is {(now - signal_time).seconds // 60} min old - SKIPPED")
                
                except Exception as e:
                    logging.error(f"[ERROR] Could not parse timestamp for signal {signal['id']}: {e}")
                    fresh_signals.append(signal)  # Process anyway if timestamp parsing fails
            
            conn.commit()
            
            if stale_count > 0:
                logging.warning(f"[STALE] Skipped {stale_count} old signals (>30 min)")
            
            if not fresh_signals:
                logging.info("[INFO] No fresh signals to process")
                return
            
            logging.info(f"[FOUND] {len(fresh_signals)} fresh signals (ignoring {stale_count} stale)")
            
            for signal in fresh_signals:
                signal_id = signal['id']
                raw_text = signal['raw_text']
                parsed_data_str = signal['parsed_data']
                
                logging.info("")
                logging.info("="*70)
                logging.info(f"[SIGNAL {signal_id}] {signal['channel_name']}")
                logging.info(f"[RAW] {raw_text[:60]}...")
                
                try:
                    # Parse JSON data
                    parsed_data = json.loads(parsed_data_str)
                    
                    # Show parsed info
                    logging.info(f"[PARSED] {parsed_data.get('symbol')} {parsed_data.get('strike')} {parsed_data.get('option_type')}")
                    logging.info(f"   Entry: {parsed_data.get('entry_price')} | SL: {parsed_data.get('stop_loss')}")
                    logging.info(f"   Tradingsymbol: {parsed_data.get('tradingsymbol')}")
                    logging.info(f"   Exchange: {parsed_data.get('exchange')} | Quantity: {parsed_data.get('quantity')}")
                    
                    # Place order
                    result = self.place_order_with_retry(parsed_data)
                    
                    if result and result.get('success', False):
                        # Mark as processed - SUCCESS
                        cursor.execute("""
                            UPDATE signals 
                            SET processed = 1 
                            WHERE id = ?
                        """, (signal_id,))
                        conn.commit()
                        logging.info(f"[PROCESSED] Signal {signal_id} marked as processed")
                    else:
                        # Mark as processed - FAILED (don't retry forever!)
                        cursor.execute("""
                            UPDATE signals 
                            SET processed = 1 
                            WHERE id = ?
                        """, (signal_id,))
                        conn.commit()
                        logging.error(f"[SKIP] Signal {signal_id} - order failed, marked as processed to avoid retries")
                
                except json.JSONDecodeError as e:
                    logging.error(f"[ERROR] Failed to parse JSON: {e}")
                except Exception as e:
                    logging.error(f"[ERROR] Failed to process signal {signal_id}: {e}")
                    import traceback
                    traceback.print_exc()
                
                logging.info("="*70)
            
            conn.close()
            
        except sqlite3.OperationalError as e:
            logging.error(f"[ERROR] Database error: {e}")
            logging.info("[HINT] Make sure telegram_reader_jp_trained.py is running and creating signals")
        except Exception as e:
            logging.error(f"[ERROR] {e}")
            import traceback
            traceback.print_exc()
    
    def continuous_monitor(self, interval=5):
        """Continuously monitor for new signals"""
        logging.info(f"[START] Monitoring jp_signals_trained.db every {interval} seconds")
        logging.info("[INFO] Press Ctrl+C to stop")
        
        while True:
            try:
                self.process_unprocessed_signals()
                time.sleep(interval)
            except KeyboardInterrupt:
                logging.info("\n[STOP] Monitoring stopped by user")
                break
            except Exception as e:
                logging.error(f"[ERROR] {e}")
                time.sleep(interval)

def main():
    parser = argparse.ArgumentParser(description='JP Order Placer - Trained Agent Signals')
    parser.add_argument('--test', action='store_true', help='Test mode (no real orders)')
    parser.add_argument('--continuous', action='store_true', help='Continuous monitoring mode')
    parser.add_argument('--interval', type=int, default=5, help='Monitoring interval in seconds')
    
    args = parser.parse_args()
    
    print("")
    print("="*70)
    print("JP ORDER PLACER - TRAINED AGENT")
    print("="*70)
    print(f"Mode: {'TEST' if args.test else 'LIVE'}")
    print(f"Database: jp_signals_trained.db")
    print(f"Monitoring: {'Yes' if args.continuous else 'One-time'}")
    print("="*70)
    print("")
    
    # Load Kite config
    try:
        with open('kite_config.json', 'r') as f:
            kite_config = json.load(f)
    except FileNotFoundError:
        print("[ERROR] kite_config.json not found!")
        return
    
    # Initialize Kite
    if not args.test:
        kite = initialize_kite_with_retry(kite_config)
    else:
        kite = None
        print("[TEST MODE] Skipping Kite login")
    
    # Initialize order placer
    placer = OrderPlacerJPTrained(kite, test_mode=args.test)
    
    # Run
    if args.continuous:
        placer.continuous_monitor(interval=args.interval)
    else:
        placer.process_unprocessed_signals()

if __name__ == '__main__':
    main()
