"""
order_placer_db.py - Enhanced with SL Flag Management
Reads from signals table, places entry orders, tracks SL status
"""

import sqlite3
import json
import time
import logging
import os
from datetime import datetime, timedelta
from kiteconnect import KiteConnect
import argparse

import csv

def find_instrument_csv(symbol, strike, option_type, expiry_date=None):
    """Quick function to find from CSV - handles date format differences"""
    try:
        with open('instruments_cache.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            strike_str = str(int(strike))
            
            for row in reader:
                # Match symbol (check if symbol is IN the tradingsymbol)
                if not (symbol.upper() in row.get('symbol', '').upper()):
                    continue
                
                # Match strike
                if row.get('strike') != strike_str:
                    continue
                
                # Match option type
                if row.get('option_type') != option_type.upper():
                    continue
                
                # Match expiry if provided
                if expiry_date:
                    csv_expiry = row.get('expiry_date', '')
                    
                    # Try to match different date formats
                    # Signal has: 2026-01-27 (YYYY-MM-DD)
                    # CSV has: 27-01-2026 (DD-MM-YYYY)
                    
                    if expiry_date in csv_expiry or csv_expiry in expiry_date:
                        # Direct match
                        pass
                    else:
                        # Try converting formats
                        try:
                            from datetime import datetime
                            # Parse signal date (YYYY-MM-DD)
                            signal_date = datetime.strptime(expiry_date, '%Y-%m-%d')
                            # Parse CSV date (DD-MM-YYYY)
                            csv_date = datetime.strptime(csv_expiry, '%d-%m-%Y')
                            
                            if signal_date.date() != csv_date.date():
                                continue
                        except:
                            # If date parsing fails, skip this row
                            continue
                
                # Found match!
                return row.get('symbol')  # Full trading symbol
        
        return None
    except Exception as e:
        print(f"Error reading CSV: {e}")
        import traceback
        traceback.print_exc()
        return None




# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - ORDER_PLACER - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('order_placer.log'),
        logging.StreamHandler()
    ]
)


def load_kite_config():
    """
    Load Kite credentials from kite_config.json
    
    Returns:
        dict with api_key and access_token, or None if file not found
    """
    config_file = 'kite_config.json'
    
    if not os.path.exists(config_file):
        logging.warning(f"[WARNING]  Config file not found: {config_file}")
        logging.warning("   Run: python auth_with_token_save.py to generate it")
        return None
    
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        api_key = config.get('api_key')
        access_token = config.get('access_token')
        user_name = config.get('user_name', 'Unknown')
        
        if not api_key or not access_token:
            logging.error("[ERROR] Invalid config file - missing api_key or access_token")
            return None
        
        logging.info(f"[OK] Loaded Kite config for user: {user_name}")
        return {
            'api_key': api_key,
            'access_token': access_token,
            'user_name': user_name
        }
        
    except Exception as e:
        logging.error(f"[ERROR] Error loading config file: {e}")
        return None

class OrderPlacerDB:
    def __init__(self, api_key=None, access_token=None, test_mode=False):
        """
        Initialize Order Placer with database connection
        Automatically loads credentials from kite_config.json if not provided
        
        Args:
            api_key: Zerodha API key (optional - will load from config)
            access_token: Zerodha access token (optional - will load from config)
            test_mode: If True, only log orders without placing them
        """
        self.test_mode = test_mode
        self.db_path = 'trading.db'
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        
        # Load credentials from config file if not provided
        if not api_key or not access_token:
            config = load_kite_config()
            if config:
                api_key = config['api_key']
                access_token = config['access_token']
                logging.info(f"[NOTE] Using credentials from kite_config.json")
            elif not test_mode:
                logging.error("[ERROR] No credentials provided and kite_config.json not found")
                logging.error("   Run: python auth_with_token_save.py")
        
        # Initialize Kite Connect if not in test mode
        if not test_mode and api_key and access_token:
            self.kite = KiteConnect(api_key=api_key)
            self.kite.set_access_token(access_token)
            logging.info("[OK] KiteConnect initialized successfully")
        else:
            self.kite = None
            if test_mode:
                logging.info("[WARNING]  Running in TEST MODE - no actual orders will be placed")
            else:
                logging.warning("[WARNING]  KiteConnect not initialized - provide api_key and access_token")
        
        # Create enhanced orders table with SL flag
        self._create_enhanced_orders_table()
    
    def _create_enhanced_orders_table(self):
        """Create orders table with SL flag if not exists"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER,
                entry_order_id TEXT,
                sl_order_id TEXT,
                tradingsymbol TEXT,
                action TEXT,
                quantity INTEGER,
                entry_price REAL,
                stop_loss REAL,
                trigger_price REAL,
                entry_status TEXT DEFAULT 'PENDING',
                sl_flag TEXT DEFAULT 'TO_BE_PLACED',
                entry_placed_at TEXT,
                entry_filled_at TEXT,
                sl_placed_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (signal_id) REFERENCES signals(id)
            )
        """)
        
        self.conn.commit()
        logging.info("[OK] Orders table ready with SL flag tracking")
    
    def _get_trading_symbol(self, signal_data):
        """
        Build trading symbol from signal data
        Uses Kite instruments list to find exact symbol with expiry
        
        Args:
            signal_data: Dict with symbol, strike, option_type
            
        Returns:
            Trading symbol like "NIFTY24D1624200CE"
        """
        try:
            if not self.kite:
                # In test mode, build a mock symbol
                symbol = signal_data.get('symbol', 'NIFTY')
                strike = signal_data.get('strike', 24200)
                option_type = signal_data.get('option_type', 'CE')
                return f"{symbol}24D16{strike}{option_type}"
            
            # Fetch instruments from Kite
            symbol_name = signal_data['symbol']
            strike = signal_data['strike']
            option_type = signal_data['option_type']
            expiry_date = signal_data.get('expiry_date')
            
            # FIRST: Try to find in CSV cache
            logging.info(f"[SEARCH] Looking for: {symbol_name} {strike} {option_type} exp={expiry_date}")
            
            tradingsymbol = find_instrument_csv(symbol_name, strike, option_type, expiry_date)
            
            if tradingsymbol:
                logging.info(f"[CSV] Found in cache: {tradingsymbol}")
                return tradingsymbol
            
            # FALLBACK: Use Kite API if CSV fails
            logging.warning(f"[CSV] Not found in cache, trying Kite API...")
            
            # Determine exchange based on symbol
            commodity_symbols = ['GOLD', 'GOLDM', 'SILVER', 'SILVERM', 'CRUDEOIL', 'NATURALGAS', 'COPPER', 'ZINC', 'LEAD', 'NICKEL']
            
            if symbol_name in commodity_symbols:
                # MCX commodities
                exchange = "MCX"
                instruments = self.kite.instruments(exchange)
                logging.info(f"[INFO] Looking for MCX instrument: {symbol_name} {strike} {option_type}")
                
                # For MCX, we need expiry_date from signal
                if not expiry_date:
                    logging.error(f"[ERROR] Missing expiry_date for commodity {symbol_name}")
                    return None
                
                # Convert expiry_date string to date object
                try:
                    target_expiry = datetime.strptime(expiry_date, '%Y-%m-%d').date()
                except:
                    logging.error(f"[ERROR] Invalid expiry_date format: {expiry_date}")
                    return None
                
            else:
                # NSE/NFO equity options
                exchange = "NFO"
                instruments = self.kite.instruments(exchange)
            
            # Calculate target expiry for equity options only (if not provided)
            if symbol_name not in commodity_symbols and not expiry_date:
                today = datetime.now()
                if 'BANK' in symbol_name:
                    # BANKNIFTY expires on Wednesday
                    days_ahead = 2 - today.weekday()
                    if days_ahead <= 0:
                        days_ahead += 7
                else:
                    # NIFTY expires on Thursday
                    days_ahead = 3 - today.weekday()
                    if days_ahead <= 0:
                        days_ahead += 7
                
                target_expiry = (today + timedelta(days=days_ahead)).date()
            elif expiry_date:
                # Use provided expiry date
                try:
                    target_expiry = datetime.strptime(expiry_date, '%Y-%m-%d').date()
                except:
                    logging.error(f"[ERROR] Invalid expiry_date format: {expiry_date}")
                    return None
            
            # Find matching instrument in Kite API
            for ins in instruments:
                if (ins['name'] == symbol_name and
                    ins['expiry'] == target_expiry and
                    ins['strike'] == strike and
                    ins['instrument_type'] == option_type):
                    logging.info(f"[KITE API] Found instrument: {ins['tradingsymbol']}")
                    return ins['tradingsymbol']
            
            logging.error(f"[ERROR] Instrument not found: {symbol_name} {strike} {option_type}")
            return None
            
        except Exception as e:
            logging.error(f"[ERROR] Error getting trading symbol: {e}")
            return None
    
    def _place_entry_order(self, signal_data, tradingsymbol):
        """
        Place entry order based on signal
        
        Args:
            signal_data: Dictionary containing parsed signal data
            tradingsymbol: Kite trading symbol
            
        Returns:
            order_id if successful, None otherwise
        """
        try:
            # Determine exchange for order placement
            commodity_symbols = ['GOLD', 'GOLDM', 'SILVER', 'SILVERM', 'CRUDEOIL', 'NATURALGAS', 'COPPER', 'ZINC', 'LEAD', 'NICKEL']
            
            if signal_data['symbol'] in commodity_symbols:
                exchange = self.kite.EXCHANGE_MCX
                # MCX OPTIONS lot sizes (different from futures!)
                # For OPTIONS: quantity = number of lots (NOT underlying units)
                # The lot size multiplier is handled by exchange
                lot_sizes = {
                    'GOLD': 1,          # 1 lot GOLD options
                    'GOLDM': 1,         # 1 lot GOLDM options
                    'SILVER': 1,        # 1 lot SILVER options
                    'SILVERM': 1,       # 1 lot SILVERM options
                    'CRUDEOIL': 1,      # 1 lot CRUDEOIL options
                    'NATURALGAS': 1,    # 1 lot NATURALGAS options (NOT 1250!)
                    'COPPER': 1,        # 1 lot COPPER options
                    'ZINC': 1,          # 1 lot ZINC options
                    'LEAD': 1,          # 1 lot LEAD options
                    'NICKEL': 1         # 1 lot NICKEL options
                }
                quantity = lot_sizes.get(signal_data['symbol'], 1)
                logging.info(f"[LOT] MCX Options: Placing {quantity} lot(s)")
            else:
                exchange = self.kite.EXCHANGE_NFO
                # Equity lot sizes - ACTUAL QUANTITY for 1 lot
                quantity = signal_data.get('quantity')
                if not quantity:
                    if 'BANK' in signal_data['symbol']:
                        quantity = 15  # 1 lot BANKNIFTY = 15 quantity
                    elif 'NIFTY' in signal_data['symbol']:
                        quantity = 25  # 1 lot NIFTY = 25 quantity (as of Dec 2024)
                    else:
                        quantity = 1   # Default for stocks/other indices
            
            # Calculate entry price (signal price + 1 tick)
            entry_price = float(signal_data['entry_price']) + 0.05
            
            # Round to tick size (0.05 for options)
            tick_size = 0.05
            entry_price = round(entry_price / tick_size) * tick_size
            
            if self.test_mode:
                logging.info(f"[TEST] TEST MODE - Would place entry order:")
                logging.info(f"   Symbol: {tradingsymbol}")
                logging.info(f"   Action: {signal_data['action']}")
                logging.info(f"   Quantity: {quantity}")
                logging.info(f"   Price: Rs.{entry_price}")
                return f"TEST_ENTRY_{int(time.time())}"
            
            if not self.kite:
                logging.error("[ERROR] KiteConnect not initialized - cannot place order")
                return None
            
            # Determine product type based on exchange
            if signal_data['symbol'] in commodity_symbols:
                product_type = self.kite.PRODUCT_NRML  # MCX requires NRML
                logging.info(f"[MCX] Using NRML product type for {tradingsymbol}")
            else:
                product_type = self.kite.PRODUCT_MIS  # Equity can use MIS
                logging.info(f"[NFO] Using MIS product type for {tradingsymbol}")
            
            # Place order using working Kite code format
            order_id = self.kite.place_order(
                variety=self.kite.VARIETY_REGULAR,
                exchange=exchange,  # NFO or MCX based on symbol
                tradingsymbol=tradingsymbol,
                transaction_type=signal_data['action'].upper(),  # BUY or SELL
                quantity=int(quantity),
                product=product_type,  # NRML for MCX, MIS for equity
                order_type=self.kite.ORDER_TYPE_LIMIT,
                price=entry_price,
                validity=self.kite.VALIDITY_DAY
            )
            
            logging.info(f"[OK] Entry order placed successfully. Order ID: {order_id}")
            return order_id
            
        except Exception as e:
            logging.error(f"[ERROR] Error placing entry order: {e}")
            return None
    
    def _save_order_to_db(self, signal_id, entry_order_id, signal_data, tradingsymbol):
        """
        Save order to database with SL flag = 'TO_BE_PLACED'
        
        Args:
            signal_id: Signal ID from signals table
            entry_order_id: Kite order ID
            signal_data: Parsed signal data
            tradingsymbol: Trading symbol
        """
        try:
            cursor = self.conn.cursor()
            now = datetime.now().isoformat()
            
            # Get quantity
            quantity = signal_data.get('quantity')
            if not quantity:
                if 'BANK' in signal_data['symbol']:
                    quantity = 15
                else:
                    quantity = 50
            
            # Calculate trigger price (same as stop loss)
            stop_loss = float(signal_data['stop_loss'])
            
            cursor.execute("""
                INSERT INTO orders (
                    signal_id, entry_order_id, tradingsymbol, action, quantity,
                    entry_price, stop_loss, trigger_price,
                    entry_status, sl_flag, entry_placed_at, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 'TO_BE_PLACED', ?, ?, ?)
            """, (
                signal_id,
                entry_order_id,
                tradingsymbol,
                signal_data['action'],
                int(quantity),
                float(signal_data['entry_price']),
                stop_loss,
                stop_loss,  # trigger_price = stop_loss
                now,
                now,
                now
            ))
            
            self.conn.commit()
            logging.info(f"[SAVE] Order saved to database - SL flag: TO_BE_PLACED")
            
        except Exception as e:
            logging.error(f"[ERROR] Error saving order to database: {e}")
    
    def _mark_signal_processed(self, signal_id):
        """Mark signal as processed in database"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                UPDATE signals 
                SET processed = 1 
                WHERE id = ?
            """, (signal_id,))
            self.conn.commit()
            logging.info(f"[OK] Signal {signal_id} marked as processed")
        except Exception as e:
            logging.error(f"[ERROR] Error marking signal as processed: {e}")
    
    def process_pending_signals(self):
        """Process all unprocessed signals from database"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT id, parsed_data, raw_text, channel_id, timestamp
            FROM signals 
            WHERE processed = 0
            ORDER BY timestamp ASC
        """)
        
        pending_signals = cursor.fetchall()
        
        if not pending_signals:
            logging.info("[DONE] No pending signals to process")
            return
        
        logging.info(f"[INFO] Found {len(pending_signals)} pending signals to process")
        print("\n" + "="*80)
        
        for signal in pending_signals:
            signal_id = signal['id']
            parsed_data_str = signal['parsed_data']
            
            try:
                # Parse the JSON string to dictionary
                if isinstance(parsed_data_str, str):
                    signal_data = json.loads(parsed_data_str)
                else:
                    signal_data = parsed_data_str
                
                # Build display name
                display_name = f"{signal_data.get('symbol')} {signal_data.get('strike')} {signal_data.get('option_type')}"
                
                logging.info(f"\n{'='*80}")
                logging.info(f"[TASK] Processing Signal #{signal_id}: {display_name}")
                logging.info(f"{'='*80}")
                logging.info(f"   Action: {signal_data.get('action')}")
                logging.info(f"   Entry Price: Rs.{signal_data.get('entry_price')}")
                logging.info(f"   Stop Loss: Rs.{signal_data.get('stop_loss')}")
                
                # Validate required fields
                required_fields = ['symbol', 'strike', 'option_type', 'action', 'entry_price', 'stop_loss']
                missing_fields = [field for field in required_fields if field not in signal_data]
                
                if missing_fields:
                    logging.error(f"[ERROR] Signal {signal_id} missing required fields: {missing_fields}")
                    self._mark_signal_processed(signal_id)
                    continue
                
                # Get trading symbol
                tradingsymbol = self._get_trading_symbol(signal_data)
                if not tradingsymbol:
                    logging.error(f"[ERROR] Cannot find instrument for signal {signal_id}")
                    self._mark_signal_processed(signal_id)
                    continue
                
                # Place entry order
                logging.info("[SEND] Placing ENTRY order...")
                entry_order_id = self._place_entry_order(signal_data, tradingsymbol)
                
                if entry_order_id:
                    # Save to database with SL flag = TO_BE_PLACED
                    self._save_order_to_db(signal_id, entry_order_id, signal_data, tradingsymbol)
                    
                    logging.info(f"[OK] Entry order placed: {entry_order_id}")
                    logging.info(f"[WAIT] SL flag: TO_BE_PLACED (monitor will place SL when entry executes)")
                    
                    # Mark signal as processed
                    self._mark_signal_processed(signal_id)
                else:
                    logging.error(f"[ERROR] Failed to place entry order for signal {signal_id}")
                    self._mark_signal_processed(signal_id)
                
                # Small delay between orders
                time.sleep(1)
                
            except json.JSONDecodeError as e:
                logging.error(f"[ERROR] JSON parsing error for signal {signal_id}: {e}")
                self._mark_signal_processed(signal_id)
            except Exception as e:
                logging.error(f"[ERROR] Error processing signal {signal_id}: {e}")
                import traceback
                logging.error(traceback.format_exc())
                self._mark_signal_processed(signal_id)
        
        print("="*80)
        logging.info("[OK] Finished processing all pending signals\n")
    
    def run_continuous(self, check_interval=30):
        """
        Continuously check for new signals and process them
        
        Args:
            check_interval: Seconds between checks (default: 30)
        """
        logging.info(f"[START] Starting continuous order placement (checking every {check_interval}s)")
        logging.info("Press Ctrl+C to stop\n")
        
        try:
            cycle = 0
            while True:
                cycle += 1
                
                # Check market hours - different for equity vs commodities
                current_time = datetime.now()
                hour = current_time.hour
                minute = current_time.minute
                
                # Equity market: 9:15 AM to 3:30 PM
                equity_hours = (
                    (hour == 9 and minute >= 15) or 
                    (10 <= hour < 15) or 
                    (hour == 15 and minute <= 30)
                )
                
                # Commodity market (MCX): 9:00 AM to 11:55 PM (23:55)
                commodity_hours = (
                    (hour == 9 and minute >= 0) or
                    (10 <= hour < 23) or
                    (hour == 23 and minute <= 55)
                )
                
                # We're in trading hours if either market is open
                in_trading_hours = equity_hours or commodity_hours
                
                if not in_trading_hours:
                    if cycle % 10 == 1:
                        logging.info("[TIME] All markets closed (Equity: 9:15-15:30, MCX: 9:00-23:55)")
                    time.sleep(300)  # Sleep 5 minutes
                    continue
                
                # Log which markets are open
                if cycle % 20 == 1:
                    markets_open = []
                    if equity_hours:
                        markets_open.append("Equity")
                    if commodity_hours:
                        markets_open.append("MCX")
                    logging.info(f"[TIME] Markets open: {', '.join(markets_open)}")
                
                # Process any pending signals
                logging.info(f"[CYCLE] Cycle #{cycle} - {current_time.strftime('%H:%M:%S')}")
                self.process_pending_signals()
                
                # Wait before next check
                time.sleep(check_interval)
                
        except KeyboardInterrupt:
            logging.info("\n[STOP] Stopping continuous order placement")
        finally:
            self.conn.close()
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()


def main():
    parser = argparse.ArgumentParser(description='Order Placer - Process trading signals from database')
    parser.add_argument('--continuous', action='store_true', 
                       help='Run continuously, checking for new signals')
    parser.add_argument('--test', action='store_true',
                       help='Test mode - log orders without placing them')
    parser.add_argument('--check-interval', type=int, default=30,
                       help='Seconds between checks in continuous mode (default: 30)')
    parser.add_argument('--api-key', type=str,
                       help='Kite API key (optional - reads from kite_config.json)')
    parser.add_argument('--access-token', type=str,
                       help='Kite access token (optional - reads from kite_config.json)')
    
    args = parser.parse_args()
    
    # Initialize order placer (will auto-load from kite_config.json)
    order_placer = OrderPlacerDB(
        api_key=args.api_key,
        access_token=args.access_token,
        test_mode=args.test
    )
    
    print("\n" + "="*80)
    print("ORDER PLACER - ENTRY ORDERS")
    print("="*80)
    print(f"Mode: {'TEST MODE' if order_placer.test_mode else 'LIVE MODE'}")
    print(f"Database: {order_placer.db_path}")
    print(f"Continuous: {args.continuous}")
    print(f"Credentials: {'From kite_config.json' if not args.api_key else 'From command line'}")
    print("="*80 + "\n")
    
    if args.continuous:
        order_placer.run_continuous(check_interval=args.check_interval)
    else:
        # Process pending signals once
        order_placer.process_pending_signals()
        order_placer.close()


if __name__ == "__main__":
    main()