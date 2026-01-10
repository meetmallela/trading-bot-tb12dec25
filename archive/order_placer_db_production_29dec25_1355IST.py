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
        self.kite = kite
        self.test_mode = test_mode
        self.db_path = 'trading.db'
        
        # Load instruments CSV
        try:
            self.instruments = pd.read_csv('valid_instruments.csv')
            logging.info(f"[OK] Loaded {len(self.instruments)} instruments from CSV")
            logging.info(f"[INFO] CSV Columns: {list(self.instruments.columns)}")
            
            # Detect column names (different CSVs might have different naming)
            self.col_map = self.detect_columns()
            
            # Show sample row
            if len(self.instruments) > 0:
                sample = self.instruments.iloc[0]
                ts_col = self.col_map.get('tradingsymbol')
                logging.info(f"[INFO] Sample: symbol={sample.get('symbol')}, tradingsymbol={sample.get(ts_col) if ts_col else 'NOT FOUND'}")
        except Exception as e:
            logging.error(f"[ERROR] Failed to load instruments CSV: {e}")
            self.instruments = pd.DataFrame()
            self.col_map = {}
    
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
        required_fields = [
            'symbol', 'strike', 'option_type', 'action', 
            'entry_price', 'stop_loss', 'expiry_date', 'quantity'
        ]
        
        missing = [f for f in required_fields if f not in data or data[f] is None]
        
        if missing:
            logging.error(f"[VALIDATION FAILED] Missing fields: {missing}")
            return False
        
        return True

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
                
                # Show what we got
                logging.info(f"   Symbol: {data.get('symbol')} {data.get('strike')} {data.get('option_type')}")
                logging.info(f"   Action: {data.get('action')} | Qty: {data.get('quantity')} | Entry: {data.get('entry_price')}")
                
                # Validate signal
                if not self.validate_signal_data(data):
                    logging.error(f"[SKIP] Signal {sig['id']} - validation failed")
                    cursor.execute("UPDATE signals SET processed = -1 WHERE id = ?", (sig['id'],))
                    conn.commit()
                    continue
                
                # Find the Zerodha Trading Symbol
                trading_symbol = self.find_exact_tradingsymbol(data)
                
                if not trading_symbol:
                    logging.error(f"[SKIP] Signal {sig['id']} - tradingsymbol not found")
                    cursor.execute("UPDATE signals SET processed = -1 WHERE id = ?", (sig['id'],))
                    conn.commit()
                    continue

                logging.info(f"[EXECUTE] Placing order for {trading_symbol}")
                
                if not self.test_mode:
                    # Determine Exchange based on Symbol
                    symbol = data['symbol'].upper()
                    
                    if symbol in ['CRUDEOIL', 'CRUDEOILM', 'GOLD', 'GOLDM', 'GOLDPETAL', 'SILVER', 'SILVERM', 'SILVERMIC', 'NATURALGAS', 'COPPER', 'ZINC', 'LEAD', 'NICKEL', 'ALUMINIUM']:
                        exchange = "MCX"
                    elif symbol in ['SENSEX', 'BANKEX']:
                        exchange = "BFO"
                    else:
                        exchange = "NFO"
                    
                    logging.info(f"   Exchange: {exchange}")
                    
                    # ACTUAL KITE API CALL
                    order_id = self.kite.place_order(
                        variety=self.kite.VARIETY_REGULAR,
                        exchange=exchange,
                        tradingsymbol=trading_symbol,
                        transaction_type=self.kite.TRANSACTION_TYPE_BUY if data['action'].upper() == 'BUY' else self.kite.TRANSACTION_TYPE_SELL,
                        quantity=int(data['quantity']),
                        product=self.kite.PRODUCT_NRML,
                        order_type=self.kite.ORDER_TYPE_MARKET
                    )
                    logging.info(f"[SUCCESS] Kite Order ID: {order_id}")
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
                
            except Exception as e:
                logging.error(f"[ERROR] Signal {sig['id']} - Exception: {e}")
                import traceback
                traceback.print_exc()
                cursor.execute("UPDATE signals SET processed = -1 WHERE id = ?", (sig['id'],))
                conn.commit()
        
        conn.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--continuous', action='store_true', help='Run in a loop')
    parser.add_argument('--interval', type=int, default=5, help='Check interval (seconds)')
    parser.add_argument('--test', action='store_true', help='Simulate without real orders')
    args = parser.parse_args()

    # Load credentials
    try:
        with open('kite_config.json', 'r') as f:
            config = json.load(f)
    except Exception as e:
        logging.error(f"Config load error: {e}")
        return

    # Initialize with Auto-Retry logic
    kite = initialize_kite_with_retry(config)
    placer = OrderPlacerProduction(kite, test_mode=args.test)

    logging.info(f"[START] Order Placer active (Mode: {'TEST' if args.test else 'LIVE'})")
    
    while True:
        try:
            placer.process_pending_signals()
        except Exception as e:
            logging.error(f"Loop error: {e}")
            import traceback
            traceback.print_exc()
            
        if not args.continuous: 
            break
        time.sleep(args.interval)

if __name__ == "__main__":
    main()
