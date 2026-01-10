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
            import traceback
            traceback.print_exc()
            return None
        except Exception as e:
            logging.error(f"[ERROR] Exception in find_exact_tradingsymbol: {e}")
            import traceback
            traceback.print_exc()
            return None

    def validate_signal_data(self, data):
        """Validate that signal has all required fields"""
        # Check instrument type
        instrument_type = data.get('instrument_type', 'OPTIONS')
        
        if instrument_type == 'FUTURES':
            # Futures validation
            required_fields = ['symbol', 'action', 'entry_price', 'stop_loss', 'quantity']
        else:
            # Options validation
            required_fields = [
                'symbol', 'strike', 'option_type', 'action', 
                'entry_price', 'stop_loss', 'expiry_date', 'quantity'
            ]
        
        missing = [f for f in required_fields if f not in data or data[f] is None]
        
        if missing:
            logging.error(f"[VALIDATION FAILED] Missing fields: {missing}")
            return False
        
        return True
    
    def generate_order_tag(self, channel_name):
        """Generate order tag from channel name (max 20 chars)
        Format: BOT:channel_abbr
        """
        # Truncate channel name to fit in tag (max 20 chars total)
        # BOT: takes 4 chars, leaving 16 for channel
        channel_abbr = channel_name[:16] if len(channel_name) <= 16 else channel_name[:16]
        tag = f"BOT:{channel_abbr}"
        return tag
    
    def place_futures_order(self, signal_data, channel_name="TG"):
        """Place FUTURES order on MCX"""
        try:
            symbol = signal_data['symbol']
            action = signal_data['action']
            quantity = signal_data['quantity']
            tradingsymbol = signal_data.get('tradingsymbol', f"{symbol}FUT")
            exchange = signal_data.get('exchange', 'MCX')
            
            logging.info(f"[FUTURES ORDER] {tradingsymbol}")
            logging.info(f"  Action: {action} | Qty: {quantity}")
            
            if self.test_mode:
                logging.info("[TEST MODE] Would place futures order")
                return {'order_id': f'TEST_{int(time.time())}', 'status': 'TEST'}
            
            # Place market order with retry logic
            max_retries = 3
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    # MCX commodity options require LIMIT orders (not MARKET)
                    if exchange == 'MCX':
                        # For MCX, use LIMIT order with market protection
                        # Set limit price slightly away from entry price
                        entry_price = signal_data.get('entry_price', 0)
                        
                        if action.upper() == 'BUY':
                            limit_price = entry_price * 1.05  # 5% above entry
                        else:
                            limit_price = entry_price * 0.95  # 5% below entry
                        
                        # CRITICAL: Round to tick size (MCX commodities use 0.05 tick size)
                        tick_size = 0.05
                        limit_price = round(limit_price / tick_size) * tick_size
                        # Format to 2 decimal places to avoid floating point issues
                        limit_price = round(limit_price, 2)
                        
                        # Generate tag
                        order_tag = self.generate_order_tag(channel_name)
                        
                        order_id = self.kite.place_order(
                            variety=self.kite.VARIETY_REGULAR,
                            exchange=exchange,
                            tradingsymbol=tradingsymbol,
                            transaction_type=self.kite.TRANSACTION_TYPE_BUY if action.upper() == 'BUY' else self.kite.TRANSACTION_TYPE_SELL,
                            quantity=int(quantity),
                            product=self.kite.PRODUCT_MIS,
                            order_type=self.kite.ORDER_TYPE_LIMIT,  # LIMIT for MCX
                            price=limit_price,
                            tag=order_tag
                        )
                        logging.info(f"[MCX LIMIT] Order placed with limit price: {limit_price} (rounded to tick size 0.05)")
                        logging.info(f"[TAG] Order tagged as: {order_tag}")
                    else:
                        # For NFO/BFO, use MARKET order as usual
                        order_tag = self.generate_order_tag(channel_name)
                        
                        order_id = self.kite.place_order(
                            variety=self.kite.VARIETY_REGULAR,
                            exchange=exchange,
                            tradingsymbol=tradingsymbol,
                            transaction_type=self.kite.TRANSACTION_TYPE_BUY if action.upper() == 'BUY' else self.kite.TRANSACTION_TYPE_SELL,
                            quantity=int(quantity),
                            product=self.kite.PRODUCT_MIS,
                            order_type=self.kite.ORDER_TYPE_MARKET,
                            tag=order_tag
                        )
                        logging.info(f"[TAG] Order tagged as: {order_tag}")
                    
                    logging.info(f"[SUCCESS] Futures order placed! Order ID: {order_id}")
                    return {'order_id': order_id, 'status': 'PLACED'}
                    
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, Exception) as e:
                    if attempt < max_retries - 1:
                        logging.warning(f"[RETRY] Attempt {attempt + 1} failed: {e}. Retrying in {retry_delay}s...")
                        time.sleep(retry_delay)
                    else:
                        logging.error(f"[FAILED] All {max_retries} attempts failed: {e}")
                        raise
            
            return None
            
        except Exception as e:
            logging.error(f"[ERROR] Failed to place futures order: {e}")
            import traceback
            traceback.print_exc()
            return None

    def process_pending_signals(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get signals that haven't been processed yet
        cursor.execute("SELECT * FROM signals WHERE processed = 0")
        signals = cursor.fetchall()
        
        if not signals:
            logging.info("[STATUS] No pending signals to process")
        else:
            logging.info(f"[STATUS] Processing {len(signals)} pending signals")
        
        for sig in signals:
            try:
                logging.info("")
                logging.info(f"[SIGNAL {sig['id']}] Channel: {sig['channel_name']}")
                
                # Parse signal data
                data = json.loads(sig['parsed_data'])
                
                # Check instrument type
                instrument_type = data.get('instrument_type', 'OPTIONS')
                
                # Show what we got
                if instrument_type == 'FUTURES':
                    logging.info(f"   [FUTURES] Symbol: {data.get('symbol')}")
                    logging.info(f"   Action: {data.get('action')} | Qty: {data.get('quantity')} | Entry: {data.get('entry_price')}")
                else:
                    logging.info(f"   [OPTIONS] Symbol: {data.get('symbol')} {data.get('strike')} {data.get('option_type')}")
                    logging.info(f"   Action: {data.get('action')} | Qty: {data.get('quantity')} | Entry: {data.get('entry_price')}")
                
                # Validate signal
                if not self.validate_signal_data(data):
                    logging.error(f"[SKIP] Signal {sig['id']} - validation failed")
                    cursor.execute("UPDATE signals SET processed = -1 WHERE id = ?", (sig['id'],))
                    conn.commit()
                    continue
                
                # ========================================
                # HANDLE FUTURES
                # ========================================
                if instrument_type == 'FUTURES':
                    logging.info(f"[FUTURES SIGNAL] Processing...")
                    
                    order_result = self.place_futures_order(data, sig['channel_name'])
                    
                    if order_result:
                        # Mark as processed
                        cursor.execute("UPDATE signals SET processed = 1 WHERE id = ?", (sig['id'],))
                        conn.commit()
                        logging.info(f"[OK] Futures signal {sig['id']} processed successfully")
                    else:
                        logging.error(f"[FAILED] Futures signal {sig['id']} - order placement failed")
                        cursor.execute("UPDATE signals SET processed = -1 WHERE id = ?", (sig['id'],))
                        conn.commit()
                    
                    continue  # Skip to next signal
                
                # ========================================
                # HANDLE OPTIONS (existing code)
                # ========================================
                
                # Get trading symbol - Check if parser already provided it
                if data.get('tradingsymbol'):
                    trading_symbol = data['tradingsymbol']
                    logging.info(f"[OK] Using tradingsymbol from parser: {trading_symbol}")
                else:
                    # Fallback: Find in CSV if not provided by parser
                    trading_symbol = self.find_exact_tradingsymbol(data)
                    if not trading_symbol:
                        logging.error(f"[SKIP] Signal {sig['id']} - tradingsymbol not found")
                        cursor.execute("UPDATE signals SET processed = -1 WHERE id = ?", (sig['id'],))
                        conn.commit()
                        continue

                logging.info(f"[EXECUTE] Placing order for {trading_symbol}")
                
                if not self.test_mode:
                    # Determine Exchange - use from parsed data if available
                    if data.get('exchange'):
                        exchange = data['exchange']
                        logging.info(f"   Using exchange from parser: {exchange}")
                    else:
                        # Fallback: determine from symbol
                        symbol = data['symbol'].upper()
                        if symbol in ['CRUDEOIL', 'CRUDEOILM', 'GOLD', 'GOLDM', 'GOLDPETAL', 'SILVER', 'SILVERM', 'SILVERMIC', 'NATURALGAS', 'COPPER', 'ZINC', 'LEAD', 'NICKEL', 'ALUMINIUM']:
                            exchange = "MCX"
                        elif symbol in ['SENSEX', 'BANKEX']:
                            exchange = "BFO"
                        else:
                            exchange = "NFO"
                        logging.info(f"   Exchange determined: {exchange}")
                    
                    # ACTUAL KITE API CALL with retry logic for network errors
                    max_retries = 3
                    retry_delay = 2
                    
                    for attempt in range(max_retries):
                        try:
                            # MCX commodity options require LIMIT orders (not MARKET)
                            if exchange == 'MCX':
                                # For MCX, use LIMIT order with market protection
                                # Set limit price slightly away from entry price
                                entry_price = data.get('entry_price', 0)
                                
                                if data['action'].upper() == 'BUY':
                                    limit_price = entry_price * 1.05  # 5% above entry
                                else:
                                    limit_price = entry_price * 0.95  # 5% below entry
                                
                                # CRITICAL: Round to tick size (MCX commodities use 0.05 tick size)
                                tick_size = 0.05
                                limit_price = round(limit_price / tick_size) * tick_size
                                # Format to 2 decimal places to avoid floating point issues
                                limit_price = round(limit_price, 2)
                                
                                # Generate tag
                                order_tag = self.generate_order_tag(sig['channel_name'])
                                
                                order_id = self.kite.place_order(
                                    variety=self.kite.VARIETY_REGULAR,
                                    exchange=exchange,
                                    tradingsymbol=trading_symbol,
                                    transaction_type=self.kite.TRANSACTION_TYPE_BUY if data['action'].upper() == 'BUY' else self.kite.TRANSACTION_TYPE_SELL,
                                    quantity=int(data['quantity']),
                                    product=self.kite.PRODUCT_MIS,
                                    order_type=self.kite.ORDER_TYPE_LIMIT,  # LIMIT for MCX
                                    price=limit_price,
                                    tag=order_tag
                                )
                                logging.info(f"[MCX LIMIT] Order placed with limit price: {limit_price} (rounded to tick size 0.05)")
                                logging.info(f"[TAG] Order tagged as: {order_tag}")
                            else:
                                # For NFO/BFO, use MARKET order as usual
                                order_tag = self.generate_order_tag(sig['channel_name'])
                                
                                order_id = self.kite.place_order(
                                    variety=self.kite.VARIETY_REGULAR,
                                    exchange=exchange,
                                    tradingsymbol=trading_symbol,
                                    transaction_type=self.kite.TRANSACTION_TYPE_BUY if data['action'].upper() == 'BUY' else self.kite.TRANSACTION_TYPE_SELL,
                                    quantity=int(data['quantity']),
                                    product=self.kite.PRODUCT_MIS,
                                    order_type=self.kite.ORDER_TYPE_MARKET,
                                    tag=order_tag
                                )
                                logging.info(f"[TAG] Order tagged as: {order_tag}")
                            
                            logging.info(f"[SUCCESS] Kite Order ID: {order_id}")
                            break  # Success, exit retry loop
                            
                        except (requests.exceptions.ConnectionError, 
                                requests.exceptions.Timeout,
                                Exception) as e:
                            if attempt < max_retries - 1:
                                logging.warning(f"[RETRY] Attempt {attempt + 1} failed: {e}. Retrying in {retry_delay}s...")
                                time.sleep(retry_delay)
                            else:
                                # Final attempt failed
                                logging.error(f"[FAILED] All {max_retries} attempts failed: {e}")
                                raise  # Re-raise to be caught by outer exception handler
                else:
                    # Test mode - show what would be placed
                    symbol = data['symbol'].upper()
                    if symbol in ['CRUDEOIL', 'CRUDEOILM', 'GOLD', 'GOLDM', 'SILVER', 'SILVERM']:
                        exchange = "MCX"
                    elif symbol in ['SENSEX', 'BANKEX']:
                        exchange = "BFO"
                    else:
                        exchange = "NFO"
                    logging.info(f"   Exchange: {exchange}")
                    logging.info("[TEST MODE] Order simulation success.")

                # Mark as processed successfully
                cursor.execute("UPDATE signals SET processed = 1 WHERE id = ?", (sig['id'],))
                conn.commit()
                logging.info(f"[OK] Signal {sig['id']} processed successfully")

            except KeyError as e:
                logging.error(f"[ERROR] Signal {sig['id']} - Missing key: {e}")
                logging.error(f"   Available keys: {list(data.keys()) if 'data' in locals() else 'N/A'}")
                cursor.execute("UPDATE signals SET processed = -1 WHERE id = ?", (sig['id'],))
                conn.commit()
                
            except (requests.exceptions.ConnectionError, 
                    requests.exceptions.Timeout) as e:
                # Network error - leave signal unprocessed for retry next cycle
                logging.warning(f"[NETWORK ERROR] Signal {sig['id']} - {e}")
                logging.warning(f"   Leaving unprocessed for retry in next cycle")
                # Don't update processed status - will retry next time
                
            except Exception as e:
                error_str = str(e)
                # Check if it's a network-related error
                if 'Connection' in error_str or 'connection' in error_str or 'Timeout' in error_str:
                    logging.warning(f"[NETWORK ERROR] Signal {sig['id']} - {e}")
                    logging.warning(f"   Leaving unprocessed for retry in next cycle")
                    # Don't mark as processed - will retry
                else:
                    # Real error - mark as failed
                    logging.error(f"[ERROR] Signal {sig['id']} - Exception: {e}")
                    import traceback
                    traceback.print_exc()
                    cursor.execute("UPDATE signals SET processed = -1 WHERE id = ?", (sig['id'],))
                    conn.commit()
        
        conn.close()

def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--continuous', action='store_true', help='Run in a loop')
        parser.add_argument('--interval', type=int, default=5, help='Check interval (seconds)')
        parser.add_argument('--test', action='store_true', help='Simulate without real orders')
        args = parser.parse_args()

        print("[INFO] Starting Order Placer...")
        
        # Load credentials
        try:
            with open('kite_config.json', 'r') as f:
                config = json.load(f)
            print("[OK] Config loaded")
        except FileNotFoundError:
            print("[ERROR] kite_config.json not found!")
            return
        except Exception as e:
            print(f"[ERROR] Config load error: {e}")
            return

        # Initialize with Auto-Retry logic
        print("[INFO] Connecting to Kite...")
        kite = initialize_kite_with_retry(config)
        
        print("[INFO] Initializing Order Placer...")
        placer = OrderPlacerProduction(kite, test_mode=args.test)

        logging.info(f"[START] Order Placer active (Mode: {'TEST' if args.test else 'LIVE'})")
        
        while True:
            try:
                placer.process_pending_signals()
            except KeyboardInterrupt:
                logging.info("\n[STOP] Shutting down...")
                break
            except Exception as e:
                logging.error(f"Loop error: {e}")
                import traceback
                traceback.print_exc()
                
            if not args.continuous: 
                break
            time.sleep(args.interval)
    
    except Exception as e:
        print(f"[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

