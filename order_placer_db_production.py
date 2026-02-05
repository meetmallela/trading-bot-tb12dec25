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
from db_utils import transaction, TransactionError, get_db_connection
# Fix Windows encoding
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Configure logging
log_filename = f"order_placer_{datetime.now().strftime('%d%b%Y_%H_%M_%S')}.log".upper()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - ORDER_PLACER - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(
            io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
            if sys.platform == 'win32' else sys.stdout
        )
    ]
)

def initialize_kite_with_retry(config, max_retries=30, initial_delay=10):
    """
    Retries connection with timeout to handle internet/DNS flickers.

    Args:
        config: Kite configuration dict with api_key and access_token
        max_retries: Maximum number of retry attempts (default: 30 = ~5 minutes)
        initial_delay: Initial delay between retries in seconds (default: 10)

    Returns:
        KiteConnect instance on success

    Raises:
        RuntimeError: If all retries exhausted or auth error detected
    """
    delay = initial_delay

    for attempt in range(1, max_retries + 1):
        try:
            logging.info(f"[CONNECT] Attempting Kite login (attempt {attempt}/{max_retries})...")
            kite = KiteConnect(api_key=config['api_key'])
            kite.set_access_token(config['access_token'])
            kite.profile()
            logging.info("[OK] Kite Connected successfully.")
            return kite

        except Exception as e:
            error_str = str(e).lower()

            # Check for authentication errors - fail fast, don't retry
            auth_errors = ['invalid', 'token', 'expired', 'unauthorized', 'forbidden', 'api_key']
            if any(err in error_str for err in auth_errors):
                logging.error(f"[AUTH ERROR] {e}")
                logging.error("[FATAL] Authentication failed - check your api_key and access_token in kite_config.json")
                logging.error("[HINT] Access tokens expire daily. Regenerate at https://kite.zerodha.com/")
                raise RuntimeError(f"Authentication failed: {e}")

            # Network/transient error - retry with backoff
            if attempt < max_retries:
                logging.warning(f"[RETRY {attempt}/{max_retries}] Connection failed: {e}")
                logging.info(f"[WAIT] Retrying in {delay} seconds...")
                time.sleep(delay)
                # Exponential backoff capped at 60 seconds
                delay = min(delay * 1.5, 60)
            else:
                logging.error(f"[FATAL] All {max_retries} connection attempts failed")
                logging.error(f"[LAST ERROR] {e}")
                raise RuntimeError(f"Failed to connect to Kite after {max_retries} attempts: {e}")

class OrderPlacerProduction:
    def __init__(self, kite, test_mode=False):
        print("[INFO] Initializing Order Placer...")
        self.kite = kite
        self.test_mode = test_mode
        self.db_path = 'trading.db'

        # Add order tracking columns if they don't exist
        self._add_order_tracking_columns()

        # Load SL exits blacklist (shared with SL monitor)
        self.sl_exits_today = {}
        self._load_sl_exits()
        
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

    def _add_order_tracking_columns(self):
        """Add order_id and order_status columns to signals table if they don't exist"""
        try:
            with get_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                # Check existing columns
                cursor.execute("PRAGMA table_info(signals)")
                columns = [row[1] for row in cursor.fetchall()]

                if 'order_id' not in columns:
                    cursor.execute("ALTER TABLE signals ADD COLUMN order_id TEXT")
                    logging.info("[DB] Added order_id column for transaction tracking")

                if 'order_status' not in columns:
                    cursor.execute("ALTER TABLE signals ADD COLUMN order_status TEXT")
                    logging.info("[DB] Added order_status column for transaction tracking")

                conn.commit()
        except sqlite3.OperationalError as e:
            logging.debug(f"[DB] Column check: {e}")

    def _load_sl_exits(self):
        """Load SL exits blacklist from file"""
        try:
            import os
            from datetime import date
            
            if os.path.exists('sl_exits.json'):
                with open('sl_exits.json', 'r') as f:
                    data = json.load(f)
                    today = date.today().isoformat()
                    
                    # Only keep today's exits
                    self.sl_exits_today = {
                        k: v for k, v in data.items() 
                        if v == today
                    }
                    
                    if self.sl_exits_today:
                        logging.info(f"[INIT] Loaded {len(self.sl_exits_today)} blacklisted instruments (SL exits)")
                        for symbol in self.sl_exits_today:
                            logging.warning(f"  ⛔ {symbol} (NO RE-ENTRY today)")
        except Exception as e:
            logging.warning(f"[INIT] Could not load sl_exits: {e}")
    
    def is_blocked_from_reentry(self, tradingsymbol):
        """Check if instrument is blocked from re-entry due to SL exit"""
        from datetime import date
        
        if tradingsymbol in self.sl_exits_today:
            exit_date = self.sl_exits_today[tradingsymbol]
            today = date.today().isoformat()
            
            if exit_date == today:
                return True
        
        return False
    
    def check_existing_position(self, tradingsymbol):
        """Check if we already have an open position in this instrument"""
        try:
            if self.test_mode:
                return False
            
            positions = self.kite.positions()['net']
            for pos in positions:
                if pos['tradingsymbol'] == tradingsymbol and pos['quantity'] != 0:
                    logging.warning(f"[EXISTING] Already have position in {tradingsymbol}")
                    logging.warning(f"  Qty: {pos['quantity']} | Avg: {pos['average_price']}")
                    return True
            
            return False
        except Exception as e:
            logging.error(f"[ERROR] Failed to check existing positions: {e}")
            return False
    
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
        
        FIXED: Properly handles special characters and ensures tag <= 20 chars
        """
        # Be conservative: BOT: = 4 chars, leave 15 for channel (not 16)
        # This handles edge cases with special characters like ™, ©, emojis
        max_channel_len = 15
        channel_abbr = channel_name[:max_channel_len]
        tag = f"BOT:{channel_abbr}"
        
        # Double-check: If still > 20, trim more aggressively
        while len(tag) > 20 and len(channel_abbr) > 0:
            channel_abbr = channel_abbr[:-1]
            tag = f"BOT:{channel_abbr}"
        
        # Final safety check
        if len(tag) > 20:
            tag = "BOT:TG"  # Fallback to generic tag
            logging.warning(f"[TAG] Channel name too long, using generic tag")
        
        return tag
    
    def is_stock_option(self, tradingsymbol, exchange):
        """Detect if instrument is a stock option (not index option)
        
        Stock options have illiquidity issues and need LIMIT orders
        Index options (NIFTY, BANKNIFTY, etc.) can use MARKET orders
        
        Returns:
            True if stock option (use LIMIT)
            False if index option or other (use MARKET)
        """
        # Index options that support MARKET orders
        index_symbols = [
            'NIFTY', 'BANKNIFTY', 'FINNIFTY', 'SENSEX', 
            'MIDCPNIFTY', 'BANKEX', 'INDIA VIX'
        ]
        
        # Check if tradingsymbol starts with any index name
        tradingsymbol_upper = tradingsymbol.upper()
        for index in index_symbols:
            if tradingsymbol_upper.startswith(index):
                return False  # Index option - can use MARKET
        
        # If it's on NFO/BFO but not an index = stock option
        if exchange in ['NFO', 'BFO']:
            return True  # Stock option - use LIMIT
        
        # MCX is commodities, not stock options
        if exchange == 'MCX':
            return False
        
        return False  # Default: not a stock option
    
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

    def mark_signal_success(self, signal_id, order_id):
        """
        Mark signal as successfully processed with order tracking.
        Uses transaction to ensure atomicity.

        Args:
            signal_id: Signal ID in database
            order_id: Broker order ID returned from order placement
        """
        try:
            with transaction(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """UPDATE signals
                       SET processed = 1, order_id = ?, order_status = 'PLACED'
                       WHERE id = ? AND processed = 0""",
                    (str(order_id), signal_id)
                )
                if cursor.rowcount == 0:
                    logging.warning(f"[WARN] Signal {signal_id} already processed or not found")
                else:
                    logging.info(f"[OK] Signal {signal_id} marked as processed with order {order_id}")
        except TransactionError as e:
            logging.error(f"[DB ERROR] Failed to mark signal {signal_id}: {e}")
            raise

    def mark_signal_failed(self, signal_id, reason="ORDER_FAILED"):
        """
        Mark signal as failed with reason tracking.
        Uses transaction to ensure atomicity.

        Args:
            signal_id: Signal ID in database
            reason: Failure reason (ORDER_FAILED, VALIDATION_FAILED, etc.)
        """
        try:
            with transaction(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """UPDATE signals
                       SET processed = -1, order_status = ?
                       WHERE id = ? AND processed = 0""",
                    (reason, signal_id)
                )
                if cursor.rowcount > 0:
                    logging.info(f"[MARKED FAILED] Signal {signal_id}: {reason}")
        except TransactionError as e:
            logging.error(f"[DB ERROR] Failed to mark signal {signal_id} as failed: {e}")

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
                    self.mark_signal_failed(sig['id'], "VALIDATION_FAILED")
                    continue
                
                # ========================================
                # ANTI-REVENGE TRADING & AVERAGING DOWN CHECKS
                # ========================================
                
                # Get tradingsymbol for checks
                check_tradingsymbol = data.get('tradingsymbol')
                if not check_tradingsymbol and data.get('symbol'):
                    # Build tradingsymbol for checking
                    try:
                        from datetime import datetime as dt
                        symbol = data['symbol']
                        if instrument_type == 'OPTIONS':
                            strike = data.get('strike', '')
                            option_type = data.get('option_type', '')
                            expiry_date = data.get('expiry_date', '')
                            exp_dt = dt.strptime(expiry_date, '%Y-%m-%d')
                            exp_str = exp_dt.strftime('%y%b').upper()
                            check_tradingsymbol = f"{symbol}{exp_str}{strike}{option_type}"
                        else:
                            check_tradingsymbol = f"{symbol}FUT"
                    except (KeyError, ValueError, TypeError) as e:
                        logging.debug(f"Could not build tradingsymbol for duplicate check: {e}")
                
                if check_tradingsymbol:
                    # CHECK 1: Is instrument blacklisted due to SL exit today?
                    if self.is_blocked_from_reentry(check_tradingsymbol):
                        logging.error(f"[BLOCKED] {check_tradingsymbol} hit SL today - NO RE-ENTRY allowed!")
                        logging.error(f"[SKIP] Signal {sig['id']} - preventing revenge trading")
                        self.mark_signal_failed(sig['id'], "SL_REENTRY_BLOCKED")
                        continue

                    # CHECK 2: Do we already have a position in this instrument?
                    if self.check_existing_position(check_tradingsymbol):
                        logging.error(f"[BLOCKED] Already have position in {check_tradingsymbol}")
                        logging.error(f"[SKIP] Signal {sig['id']} - NO AVERAGING DOWN allowed!")
                        self.mark_signal_failed(sig['id'], "POSITION_EXISTS")
                        continue
                
                # ========================================
                # HANDLE FUTURES
                # ========================================
                if instrument_type == 'FUTURES':
                    logging.info(f"[FUTURES SIGNAL] Processing...")

                    order_result = self.place_futures_order(data, sig['channel_name'])

                    if order_result:
                        # Mark as processed with order tracking (transaction-safe)
                        order_id = order_result.get('order_id', 'UNKNOWN')
                        self.mark_signal_success(sig['id'], order_id)
                        logging.info(f"[OK] Futures signal {sig['id']} processed successfully")
                    else:
                        logging.error(f"[FAILED] Futures signal {sig['id']} - order placement failed")
                        self.mark_signal_failed(sig['id'], "FUTURES_ORDER_FAILED")

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
                        self.mark_signal_failed(sig['id'], "TRADINGSYMBOL_NOT_FOUND")
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
                        if symbol in ['CRUDEOIL', 'CRUDEOILM', 'CRUDE', 'GOLD', 'GOLDM', 'GOLDPETAL', 'SILVER', 'SILVERM', 'SILVERMIC', 'NATURALGAS', 'COPPER', 'ZINC', 'LEAD', 'NICKEL', 'ALUMINIUM']:
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
                                    product=self.kite.PRODUCT_NRML,  # Changed from MIS to NRML
                                    order_type=self.kite.ORDER_TYPE_LIMIT,  # LIMIT for MCX
                                    price=limit_price,
                                    tag=order_tag
                                )
                                logging.info(f"[MCX LIMIT] Order placed with limit price: {limit_price} (rounded to tick size 0.05)")
                                logging.info(f"[TAG] Order tagged as: {order_tag}")
                            else:
                                # For NFO/BFO - check if stock option (needs LIMIT) or index option (can use MARKET)
                                order_tag = self.generate_order_tag(sig['channel_name'])
                                
                                # Detect if this is a stock option
                                is_stock_opt = self.is_stock_option(trading_symbol, exchange)
                                
                                if is_stock_opt:
                                    # Stock options: Use LIMIT order at entry price (no buffer)
                                    # Zerodha blocks MARKET orders for illiquid stock options
                                    entry_price = data.get('entry_price', 0)
                                    
                                    # Use entry price directly (no 5% buffer)
                                    limit_price = entry_price
                                    
                                    # Round to tick size (0.05 for most options)
                                    tick_size = 0.05
                                    limit_price = round(limit_price / tick_size) * tick_size
                                    limit_price = round(limit_price, 2)
                                    
                                    order_id = self.kite.place_order(
                                        variety=self.kite.VARIETY_REGULAR,
                                        exchange=exchange,
                                        tradingsymbol=trading_symbol,
                                        transaction_type=self.kite.TRANSACTION_TYPE_BUY if data['action'].upper() == 'BUY' else self.kite.TRANSACTION_TYPE_SELL,
                                        quantity=int(data['quantity']),
                                        product=self.kite.PRODUCT_NRML,  # Changed from MIS to NRML
                                        order_type=self.kite.ORDER_TYPE_LIMIT,
                                        price=limit_price,
                                        tag=order_tag
                                    )
                                    logging.info(f"[STOCK OPTION] Using LIMIT order at {limit_price} (entry: {entry_price}, illiquid)")
                                    logging.info(f"[TAG] Order tagged as: {order_tag}")
                                else:
                                    # Index options: Use MARKET order as usual
                                    order_id = self.kite.place_order(
                                        variety=self.kite.VARIETY_REGULAR,
                                        exchange=exchange,
                                        tradingsymbol=trading_symbol,
                                        transaction_type=self.kite.TRANSACTION_TYPE_BUY if data['action'].upper() == 'BUY' else self.kite.TRANSACTION_TYPE_SELL,
                                        quantity=int(data['quantity']),
                                        product=self.kite.PRODUCT_NRML,  # Changed from MIS to NRML
                                        order_type=self.kite.ORDER_TYPE_MARKET,
                                        tag=order_tag
                                    )
                                    logging.info(f"[INDEX OPTION] Using MARKET order")
                                    logging.info(f"[TAG] Order tagged as: {order_tag}")
                            
                            logging.info(f"[SUCCESS] Kite Order ID: {order_id}")

                            # Mark as processed with order tracking (transaction-safe)
                            self.mark_signal_success(sig['id'], order_id)
                            logging.info(f"[OK] Signal {sig['id']} processed with order {order_id}")
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
                    if symbol in ['CRUDEOIL', 'CRUDEOILM', 'CRUDE', 'GOLD', 'GOLDM', 'GOLDPETAL', 'SILVER', 'SILVERM', 'SILVERMIC', 'NATURALGAS', 'COPPER', 'ZINC', 'LEAD', 'NICKEL', 'ALUMINIUM']:
                        exchange = "MCX"
                    elif symbol in ['SENSEX', 'BANKEX']:
                        exchange = "BFO"
                    else:
                        exchange = "NFO"
                    logging.info(f"   Exchange: {exchange}")
                    logging.info("[TEST MODE] Order simulation success.")

                    # Mark as processed in test mode (no real order_id)
                    self.mark_signal_success(sig['id'], "TEST_MODE")
                    logging.info(f"[OK] Signal {sig['id']} processed in test mode")

            except KeyError as e:
                logging.error(f"[ERROR] Signal {sig['id']} - Missing key: {e}")
                logging.error(f"   Available keys: {list(data.keys()) if 'data' in locals() else 'N/A'}")
                self.mark_signal_failed(sig['id'], f"MISSING_KEY:{e}")

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
                    self.mark_signal_failed(sig['id'], f"EXCEPTION:{type(e).__name__}")
        
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

