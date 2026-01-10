"""
COMPLETE ORDER PLACER WITH FUTURES SUPPORT

Key changes:
1. place_futures_order() method added inside OrderPlacerProduction class
2. process_signal() modified to check instrument_type
3. Handles both OPTIONS and FUTURES
"""

import sqlite3
import json
import logging
import time
import argparse
import pandas as pd
import requests
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
    format='%(asctime)s - ORDER_PLACER - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('order_placer.log', encoding='utf-8'),
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

class OrderPlacerProduction:
    def __init__(self, kite, test_mode=False):
        print("[INFO] Initializing Order Placer...")
        self.kite = kite
        self.test_mode = test_mode
        self.db_path = 'trading.db'
        
        # Load instruments CSV (optional - parser provides tradingsymbol)
        try:
            print("[INFO] Loading instruments CSV...")
            self.instruments = pd.read_csv('valid_instruments.csv')
            logging.info(f"[OK] Loaded {len(self.instruments)} instruments from CSV")
            logging.info(f"[INFO] CSV Columns: {list(self.instruments.columns)}")
            
            # Detect column names (different CSVs might have different naming)
            self.col_map = self.detect_columns()
            
            # Show sample row
            if len(self.instruments) > 0:
                sample = self.instruments.iloc[0]
                ts_col = self.col_map.get('tradingsymbol')
                if ts_col and ts_col in sample:
                    logging.info(f"[INFO] Sample: symbol={sample.get('symbol')}, tradingsymbol={sample.get(ts_col)}")
            
            print("[OK] CSV loaded successfully")
        except FileNotFoundError:
            print("[WARN] valid_instruments.csv not found - will use tradingsymbol from parser")
            logging.warning("[WARN] CSV not found - relying on parser-provided tradingsymbol")
            self.instruments = pd.DataFrame()
            self.col_map = {}
        except Exception as e:
            print(f"[WARN] Failed to load CSV: {e}")
            logging.error(f"[ERROR] Failed to load instruments CSV: {e}")
            import traceback
            traceback.print_exc()
            self.instruments = pd.DataFrame()
            self.col_map = {}
        
        print("[OK] Order Placer initialized")
    
    def detect_columns(self):
        """Detect column names in CSV (different formats use different names)"""
        col_map = {}
        cols = [c.lower() for c in self.instruments.columns]
        
        # Tradingsymbol variations
        for possible in ['tradingsymbol', 'trading_symbol', 'symbol_name', 'instrument_token']:
            for actual_col in self.instruments.columns:
                if actual_col.lower() == possible:
                    col_map['tradingsymbol'] = actual_col
                    break
            if 'tradingsymbol' in col_map:
                break
        
        # Strike variations
        for possible in ['strike', 'strike_price']:
            for actual_col in self.instruments.columns:
                if actual_col.lower() == possible:
                    col_map['strike'] = actual_col
                    break
            if 'strike' in col_map:
                break
        
        # Expiry variations
        for possible in ['expiry_date', 'expiry', 'expiration', 'maturity_date']:
            for actual_col in self.instruments.columns:
                if actual_col.lower() == possible:
                    col_map['expiry_date'] = actual_col
                    break
            if 'expiry_date' in col_map:
                break
        
        # Option type variations
        for possible in ['option_type', 'instrument_type', 'type']:
            for actual_col in self.instruments.columns:
                if actual_col.lower() == possible:
                    col_map['option_type'] = actual_col
                    break
            if 'option_type' in col_map:
                break
        
        logging.info(f"[INFO] Column mapping: {col_map}")
        return col_map

    # =========================
    # FUTURES ORDER PLACEMENT
    # =========================
    def place_futures_order(self, signal_data):
        """
        Place FUTURES order on MCX
        
        Signal data for futures:
        - symbol: GOLD, SILVER, CRUDEOIL, etc.
        - action: BUY/SELL
        - entry_price: Entry price
        - stop_loss: SL price
        - quantity: Lot size (auto-added by parser)
        - tradingsymbol: GOLD25FEBFUT (auto-built by parser)
        - exchange: MCX (auto-added by parser)
        """
        try:
            symbol = signal_data['symbol']
            action = signal_data['action']
            entry_price = signal_data['entry_price']
            stop_loss = signal_data['stop_loss']
            quantity = signal_data['quantity']
            tradingsymbol = signal_data.get('tradingsymbol', f"{symbol}FUT")
            exchange = signal_data.get('exchange', 'MCX')
            
            logging.info(f"\n{'='*80}")
            logging.info(f"[FUTURES ORDER] {tradingsymbol}")
            logging.info(f"{'='*80}")
            logging.info(f"  Symbol: {symbol}")
            logging.info(f"  Action: {action}")
            logging.info(f"  Quantity: {quantity}")
            logging.info(f"  Entry: ₹{entry_price}")
            logging.info(f"  Stop Loss: ₹{stop_loss}")
            logging.info(f"  Exchange: {exchange}")
            
            if self.test_mode:
                logging.info(f"[TEST MODE] Would place FUTURES order: {tradingsymbol}")
                return {
                    'order_id': f'TEST_{int(time.time())}',
                    'status': 'TEST',
                    'tradingsymbol': tradingsymbol,
                    'quantity': quantity,
                    'entry_price': entry_price
                }
            
            # Place MARKET order for futures
            order_params = {
                'tradingsymbol': tradingsymbol,
                'exchange': exchange,
                'transaction_type': action,  # BUY or SELL
                'quantity': quantity,
                'order_type': 'MARKET',  # Market order for fast execution
                'product': 'MIS',  # Intraday
                'validity': 'DAY'
            }
            
            logging.info(f"[SEND] Placing FUTURES order...")
            logging.info(f"  Params: {order_params}")
            
            # Place order with retry logic
            max_retries = 3
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    order_id = self.kite.place_order(
                        variety='regular',
                        **order_params
                    )
                    
                    logging.info(f"[✓ OK] Futures order placed! Order ID: {order_id}")
                    
                    return {
                        'order_id': order_id,
                        'status': 'PLACED',
                        'tradingsymbol': tradingsymbol,
                        'quantity': quantity,
                        'entry_price': entry_price
                    }
                    
                except Exception as e:
                    error_str = str(e)
                    if 'Connection aborted' in error_str or 'timeout' in error_str.lower():
                        if attempt < max_retries - 1:
                            logging.warning(f"[RETRY] Connection error, retrying in {retry_delay}s...")
                            time.sleep(retry_delay)
                            retry_delay *= 2
                            continue
                    raise  # Re-raise if not a connection error or final attempt
            
            return None
            
        except Exception as e:
            logging.error(f"[✗ ERROR] Failed to place futures order: {e}")
            import traceback
            traceback.print_exc()
            return None

    # ======================================
    # EXISTING OPTIONS ORDER PLACEMENT
    # ======================================
    def find_exact_tradingsymbol(self, data):
        """
        Maps NIFTY 25800 CE + Expiry -> NIFTY25DEC25800CE
        Maps CRUDEOIL 5300 PE + Expiry -> CRUDEOIL25DEC5300PE
        """
        try:
            # Check if required fields exist
            required = ['symbol', 'strike', 'option_type', 'expiry_date']
            missing = [f for f in required if f not in data or data[f] is None]
            
            if missing:
                logging.error(f"[ERROR] Missing fields in signal: {missing}")
                return None
            
            # Check if we have column mappings
            if not hasattr(self, 'col_map') or not self.col_map:
                logging.error(f"[ERROR] Column mapping not initialized")
                return None
            
            # Get actual column names from mapping
            strike_col = self.col_map.get('strike', 'strike')
            option_col = self.col_map.get('option_type', 'option_type')
            expiry_col = self.col_map.get('expiry_date', 'expiry_date')
            ts_col = self.col_map.get('tradingsymbol')
            
            if not ts_col:
                logging.error(f"[ERROR] Could not find tradingsymbol column in CSV")
                logging.error(f"[ERROR] Available columns: {list(self.instruments.columns)}")
                return None
            
            # Filter CSV for the correct instrument
            mask = (self.instruments['symbol'] == data['symbol']) & \
                   (self.instruments[strike_col] == float(data['strike'])) & \
                   (self.instruments[option_col] == data['option_type']) & \
                   (self.instruments[expiry_col] == data['expiry_date'])
            
            matches = self.instruments[mask]
            
            if not matches.empty:
                # Access tradingsymbol using detected column name
                tradingsymbol = str(matches.iloc[0][ts_col])
                logging.info(f"[OK] Found tradingsymbol: {tradingsymbol}")
                return tradingsymbol
            else:
                logging.error(f"[ERROR] No match in CSV for: {data['symbol']} {data['strike']} {data['option_type']} {data['expiry_date']}")
                # Show what's available for this symbol
                symbol_matches = self.instruments[self.instruments['symbol'] == data['symbol']]
                if not symbol_matches.empty:
                    logging.info(f"   Available strikes for {data['symbol']}: {sorted(symbol_matches[strike_col].unique())[:10]}")
                    logging.info(f"   Available expiries: {sorted(symbol_matches[expiry_col].unique())[:5]}")
                else:
                    logging.error(f"   Symbol '{data['symbol']}' not found in CSV at all!")
                    # Show some available symbols
                    available_symbols = self.instruments['symbol'].unique()[:20]
                    logging.info(f"   Available symbols (first 20): {list(available_symbols)}")
                return None
                
        except KeyError as e:
            logging.error(f"[ERROR] KeyError in find_exact_tradingsymbol: {e}")
            logging.error(f"[ERROR] Column map: {getattr(self, 'col_map', 'NOT SET')}")
            logging.error(f"[ERROR] CSV columns: {list(self.instruments.columns)}")
            return None
        except Exception as e:
            logging.error(f"[ERROR] Exception in find_exact_tradingsymbol: {e}")
            import traceback
            traceback.print_exc()
            return None

    def place_options_order(self, signal_data):
        """Place OPTIONS order (existing logic)"""
        try:
            # Get tradingsymbol
            tradingsymbol = signal_data.get('tradingsymbol')
            
            if not tradingsymbol:
                # Try to find from CSV
                tradingsymbol = self.find_exact_tradingsymbol(signal_data)
            
            if not tradingsymbol:
                logging.error(f"[ERROR] Could not determine tradingsymbol")
                return None
            
            # Existing options order logic
            action = signal_data['action']
            quantity = signal_data['quantity']
            entry_price = signal_data['entry_price']
            stop_loss = signal_data['stop_loss']
            exchange = signal_data.get('exchange', 'NFO')
            
            logging.info(f"\n{'='*80}")
            logging.info(f"[OPTIONS ORDER] {tradingsymbol}")
            logging.info(f"{'='*80}")
            logging.info(f"  Action: {action}")
            logging.info(f"  Quantity: {quantity}")
            logging.info(f"  Entry: ₹{entry_price}")
            logging.info(f"  Stop Loss: ₹{stop_loss}")
            
            if self.test_mode:
                logging.info(f"[TEST MODE] Would place OPTIONS order")
                return {
                    'order_id': f'TEST_{int(time.time())}',
                    'status': 'TEST',
                    'tradingsymbol': tradingsymbol,
                    'quantity': quantity,
                    'entry_price': entry_price
                }
            
            # Place MARKET order
            order_params = {
                'tradingsymbol': tradingsymbol,
                'exchange': exchange,
                'transaction_type': action,
                'quantity': quantity,
                'order_type': 'MARKET',
                'product': 'MIS',
                'validity': 'DAY'
            }
            
            logging.info(f"[SEND] Placing OPTIONS order...")
            
            # Retry logic
            max_retries = 3
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    order_id = self.kite.place_order(variety='regular', **order_params)
                    logging.info(f"[✓ OK] Options order placed! Order ID: {order_id}")
                    
                    return {
                        'order_id': order_id,
                        'status': 'PLACED',
                        'tradingsymbol': tradingsymbol,
                        'quantity': quantity,
                        'entry_price': entry_price
                    }
                    
                except Exception as e:
                    error_str = str(e)
                    if 'Connection aborted' in error_str or 'timeout' in error_str.lower():
                        if attempt < max_retries - 1:
                            logging.warning(f"[RETRY] Connection error, retrying in {retry_delay}s...")
                            time.sleep(retry_delay)
                            retry_delay *= 2
                            continue
                    raise
            
            return None
            
        except Exception as e:
            logging.error(f"[✗ ERROR] Failed to place options order: {e}")
            import traceback
            traceback.print_exc()
            return None

    # ======================================
    # MAIN SIGNAL PROCESSING - HANDLES BOTH
    # ======================================
    def process_signal(self, signal_record):
        """
        Process signal - HANDLES BOTH OPTIONS AND FUTURES
        """
        try:
            signal_id = signal_record[0]
            raw_text = signal_record[4]
            parsed_json = signal_record[5]
            
            # Parse JSON
            signal_data = json.loads(parsed_json)
            
            # Check instrument type
            instrument_type = signal_data.get('instrument_type', 'OPTIONS')
            
            logging.info(f"\n{'='*80}")
            logging.info(f"[PROCESSING] Signal #{signal_id} | Type: {instrument_type}")
            logging.info(f"{'='*80}")
            logging.info(f"Raw: {raw_text[:100]}...")
            
            # Route to appropriate handler
            if instrument_type == 'FUTURES':
                # ===== FUTURES =====
                symbol = signal_data.get('symbol')
                action = signal_data.get('action')
                entry_price = signal_data.get('entry_price')
                stop_loss = signal_data.get('stop_loss')
                
                logging.info(f"[FUTURES] {symbol} | {action} @ {entry_price} | SL: {stop_loss}")
                
                # Place futures order
                order_result = self.place_futures_order(signal_data)
                
                if order_result:
                    # Save to database
                    self.save_order_to_db(
                        signal_id=signal_id,
                        order_id=order_result['order_id'],
                        order_type='ENTRY',
                        status=order_result['status'],
                        tradingsymbol=order_result['tradingsymbol'],
                        quantity=order_result['quantity'],
                        price=order_result['entry_price'],
                        stop_loss=stop_loss
                    )
                    
                    # Mark signal as processed
                    self.mark_signal_processed(signal_id)
                    logging.info(f"[✓ SUCCESS] Futures order completed")
                else:
                    logging.error(f"[✗ FAILED] Could not place futures order")
            
            else:
                # ===== OPTIONS =====
                symbol = signal_data.get('symbol')
                strike = signal_data.get('strike')
                option_type = signal_data.get('option_type')
                action = signal_data.get('action')
                entry_price = signal_data.get('entry_price')
                stop_loss = signal_data.get('stop_loss')
                
                logging.info(f"[OPTIONS] {symbol} {strike} {option_type} | {action} @ {entry_price} | SL: {stop_loss}")
                
                # Place options order
                order_result = self.place_options_order(signal_data)
                
                if order_result:
                    # Save to database
                    self.save_order_to_db(
                        signal_id=signal_id,
                        order_id=order_result['order_id'],
                        order_type='ENTRY',
                        status=order_result['status'],
                        tradingsymbol=order_result['tradingsymbol'],
                        quantity=order_result['quantity'],
                        price=order_result['entry_price'],
                        stop_loss=stop_loss
                    )
                    
                    # Mark signal as processed
                    self.mark_signal_processed(signal_id)
                    logging.info(f"[✓ SUCCESS] Options order completed")
                else:
                    logging.error(f"[✗ FAILED] Could not place options order")
                    
        except Exception as e:
            logging.error(f"[ERROR] Failed to process signal: {e}")
            import traceback
            traceback.print_exc()

    def save_order_to_db(self, signal_id, order_id, order_type, status, tradingsymbol, quantity, price, stop_loss=None):
        """Save order to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO orders 
                (signal_id, order_id, order_type, status, tradingsymbol, quantity, price, stop_loss, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal_id,
                order_id,
                order_type,
                status,
                tradingsymbol,
                quantity,
                price,
                stop_loss,
                datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
            
            logging.info(f"[DB] Saved order to database: Order ID {order_id}")
            
        except Exception as e:
            logging.error(f"[ERROR] Failed to save order: {e}")

    def mark_signal_processed(self, signal_id):
        """Mark signal as processed"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE signals 
                SET processed = 1
                WHERE id = ?
            """, (signal_id,))
            
            conn.commit()
            conn.close()
            
            logging.info(f"[DB] Marked signal #{signal_id} as processed")
            
        except Exception as e:
            logging.error(f"[ERROR] Failed to mark signal processed: {e}")

    def get_unprocessed_signals(self):
        """Get unprocessed signals from database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT id, channel_id, channel_name, message_id, raw_text, parsed_data, timestamp
                FROM signals
                WHERE processed = 0
                ORDER BY id ASC
            """)
            
            signals = cursor.fetchall()
            conn.close()
            
            return signals
            
        except Exception as e:
            logging.error(f"[ERROR] Failed to get signals: {e}")
            return []

    def run_continuous(self, interval=30):
        """Run in continuous mode"""
        logging.info(f"[START] Continuous mode (interval: {interval}s)")
        
        while True:
            try:
                signals = self.get_unprocessed_signals()
                
                if signals:
                    logging.info(f"[FOUND] {len(signals)} unprocessed signals")
                    
                    for signal in signals:
                        self.process_signal(signal)
                        time.sleep(2)  # Small delay between orders
                else:
                    logging.info(f"[IDLE] No unprocessed signals")
                
                time.sleep(interval)
                
            except KeyboardInterrupt:
                logging.info("\n[STOP] Shutting down...")
                break
            except Exception as e:
                logging.error(f"[ERROR] Error in continuous loop: {e}")
                time.sleep(interval)

# ======================================
# MAIN
# ======================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true', help='Test mode (no real orders)')
    parser.add_argument('--continuous', action='store_true', help='Run continuously')
    parser.add_argument('--interval', type=int, default=30, help='Polling interval in seconds')
    args = parser.parse_args()
    
    # Load config
    config = {}
    with open('config.txt', 'r') as f:
        for line in f:
            if '=' in line:
                key, value = line.strip().split('=', 1)
                config[key.strip()] = value.strip()
    
    # Initialize Kite
    kite = initialize_kite_with_retry(config)
    
    # Initialize order placer
    order_placer = OrderPlacerProduction(kite, test_mode=args.test)
    
    if args.test:
        print("\n" + "="*80)
        print("TEST MODE - NO REAL ORDERS WILL BE PLACED")
        print("="*80 + "\n")
    
    if args.continuous:
        order_placer.run_continuous(interval=args.interval)
    else:
        # Process once
        signals = order_placer.get_unprocessed_signals()
        if signals:
            logging.info(f"[FOUND] {len(signals)} unprocessed signals")
            for signal in signals:
                order_placer.process_signal(signal)
        else:
            logging.info("[IDLE] No unprocessed signals")

if __name__ == '__main__':
    main()
