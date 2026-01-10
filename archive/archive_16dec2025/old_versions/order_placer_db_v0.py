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
            instruments = self.kite.instruments("NFO")
            
            symbol_name = signal_data['symbol']
            strike = signal_data['strike']
            option_type = signal_data['option_type']
            
            # Calculate target expiry (next Thursday for NIFTY, Wednesday for BANKNIFTY)
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
            
            # Find matching instrument
            for ins in instruments:
                if (ins['name'] == symbol_name and
                    ins['expiry'] == target_expiry and
                    ins['strike'] == strike and
                    ins['instrument_type'] == option_type):
                    logging.info(f"[OK] Found instrument: {ins['tradingsymbol']}")
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
            # Calculate entry price (signal price + 1 tick)
            entry_price = float(signal_data['entry_price']) + 0.05
            
            # Round to tick size (0.05 for options)
            tick_size = 0.05
            entry_price = round(entry_price / tick_size) * tick_size
            
            # Get quantity (default based on symbol)
            quantity = signal_data.get('quantity')
            if not quantity:
                if 'BANK' in signal_data['symbol']:
                    quantity = 15  # BANKNIFTY lot size
                else:
                    quantity = 50  # NIFTY lot size
            
            if self.test_mode:
                logging.info(f"[TEST] TEST MODE - Would place entry order:")
                logging.info(f"   Symbol: {tradingsymbol}")
                logging.info(f"   Action: {signal_data['action']}")
                logging.info(f"   Quantity: {quantity}")
                logging.info(f"   Price: ₹{entry_price}")
                return f"TEST_ENTRY_{int(time.time())}"
            
            if not self.kite:
                logging.error("[ERROR] KiteConnect not initialized - cannot place order")
                return None
            
            # Place order using working Kite code format
            order_id = self.kite.place_order(
                variety=self.kite.VARIETY_REGULAR,
                exchange=self.kite.EXCHANGE_NFO,
                tradingsymbol=tradingsymbol,
                transaction_type=signal_data['action'].upper(),  # BUY or SELL
                quantity=int(quantity),
                product=self.kite.PRODUCT_MIS,  # Intraday
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
                logging.info(f"   Entry Price: ₹{signal_data.get('entry_price')}")
                logging.info(f"   Stop Loss: ₹{signal_data.get('stop_loss')}")
                
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
                
                # Check if market is open (9:30 AM to 3:30 PM)
                current_time = datetime.now()
                hour = current_time.hour
                minute = current_time.minute
                
                in_trading_hours = (
                    (hour == 9 and minute >= 30) or 
                    (10 <= hour < 15) or 
                    (hour == 15 and minute <= 30)
                )
                
                if not in_trading_hours:
                    if cycle % 10 == 1:
                        logging.info("[TIME] Outside trading hours - sleeping")
                    time.sleep(300)  # Sleep 5 minutes
                    continue
                
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