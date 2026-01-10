"""
order_placer_db_production.py - Production order placer with instrument lookup

FEATURES:
1. Symbol mapping (GOLD → GOLDM)
2. Reads tick_size, lot_size from valid_instruments.csv
3. Validates instruments before placing orders
4. Proper tick size rounding per instrument
"""

import sqlite3
import json
import logging
import time
import argparse
import pandas as pd
from datetime import datetime
from kiteconnect import KiteConnect

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - ORDER_PLACER - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('order_placer.log'),
        logging.StreamHandler()
    ]
)

# Symbol mapping - maps parsed symbols to actual trading symbols
SYMBOL_MAPPING = {
    'GOLD': 'GOLDM',      # GOLD → GOLDM
    'NIFTY': 'NIFTY',     # No change
    'BANKNIFTY': 'BANKNIFTY',
    'SENSEX': 'SENSEX',
    'SILVER': 'SILVER',
    'CRUDEOIL': 'CRUDEOIL',
    'NATURALGAS': 'NATURALGAS',
    'ZINC': 'ZINC',
    'COPPER': 'COPPER',
}


class InstrumentLookup:
    """Lookup instruments from valid_instruments.csv"""
    
    def __init__(self, csv_path='valid_instruments.csv'):
        """Load instruments from CSV"""
        try:
            self.df = pd.read_csv(csv_path)
            logging.info(f"[OK] Loaded {len(self.df)} instruments from {csv_path}")
            
            # Show available columns
            logging.info(f"[INFO] Available columns: {list(self.df.columns)}")
            
            # Map symbol names if needed
            if 'GOLD' in self.df['symbol'].values:
                self.df.loc[self.df['symbol'] == 'GOLD', 'symbol'] = 'GOLDM'
                logging.info("[INFO] Mapped GOLD → GOLDM in instrument data")
            
        except FileNotFoundError:
            logging.error(f"[ERROR] {csv_path} not found!")
            self.df = pd.DataFrame()
        except Exception as e:
            logging.error(f"[ERROR] Failed to load instruments: {e}")
            self.df = pd.DataFrame()
    
    def find_instrument(self, symbol, strike, option_type, expiry_date):
        """
        Find instrument in CSV
        
        Args:
            symbol: Trading symbol (e.g., GOLDM, NIFTY)
            strike: Strike price
            option_type: CE or PE
            expiry_date: Date in YYYY-MM-DD format
            
        Returns:
            dict with instrument details or None
        """
        try:
            # Apply symbol mapping
            mapped_symbol = SYMBOL_MAPPING.get(symbol, symbol)
            
            # Convert expiry to same format as CSV
            if 'T' in str(expiry_date):
                expiry_date = expiry_date.split('T')[0]
            
            # Search in dataframe
            mask = (
                (self.df['symbol'] == mapped_symbol) &
                (self.df['strike'] == float(strike)) &
                (self.df['option_type'] == option_type) &
                (self.df['expiry_date'] == expiry_date)
            )
            
            results = self.df[mask]
            
            if len(results) == 0:
                logging.warning(f"[CSV] Instrument not found: {mapped_symbol} {strike} {option_type} exp={expiry_date}")
                return None
            
            if len(results) > 1:
                logging.warning(f"[CSV] Multiple instruments found, using first")
            
            row = results.iloc[0]
            
            instrument = {
                'symbol': row['symbol'],
                'strike': row['strike'],
                'option_type': row['option_type'],
                'expiry_date': row['expiry_date'],
                'tick_size': row['tick_size'],
                'lot_size': int(row['lot_size']),
                'exchange': row['exchange'],
                'instrument_type': row.get('instrument_type', 'OPT')
            }
            
            logging.info(f"[CSV] Found: {instrument['symbol']} | Tick: Rs.{instrument['tick_size']} | Lot: {instrument['lot_size']} | Exchange: {instrument['exchange']}")
            
            return instrument
            
        except Exception as e:
            logging.error(f"[ERROR] Error finding instrument: {e}")
            return None
    
    def get_tradingsymbol(self, instrument):
        """Build trading symbol from instrument data"""
        try:
            symbol = instrument['symbol']
            expiry = instrument['expiry_date']  # YYYY-MM-DD
            strike = int(instrument['strike'])
            option_type = instrument['option_type']
            
            # Parse expiry date
            exp_date = datetime.strptime(expiry, '%Y-%m-%d')
            
            # Format: GOLDM26JAN138000CE
            exp_str = exp_date.strftime('%y%b').upper()  # 26JAN
            
            tradingsymbol = f"{symbol}{exp_str}{strike}{option_type}"
            
            return tradingsymbol
            
        except Exception as e:
            logging.error(f"[ERROR] Error building tradingsymbol: {e}")
            return None


class OrderPlacerProduction:
    """Production order placer with instrument validation"""
    
    def __init__(self, kite=None, test_mode=False):
        self.kite = kite
        self.test_mode = test_mode
        self.db_path = 'trading.db'
        self.instruments = InstrumentLookup('valid_instruments.csv')
        
        if test_mode:
            logging.info("[TEST] Running in TEST MODE - no real orders will be placed")
    
    def process_pending_signals(self):
        """Process all unprocessed signals"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get pending signals
        cursor.execute("""
            SELECT * FROM signals 
            WHERE processed = 0
            ORDER BY timestamp ASC
        """)
        
        signals = cursor.fetchall()
        
        if not signals:
            logging.info("[INFO] No pending signals")
            conn.close()
            return
        
        logging.info(f"[INFO] Found {len(signals)} pending signals")
        
        for signal in signals:
            try:
                signal_id = signal['id']
                signal_data = json.loads(signal['parsed_data'])
                
                logging.info("")
                logging.info("="*80)
                logging.info(f"[TASK] Processing Signal #{signal_id}: {signal_data.get('symbol')} {signal_data.get('strike')} {signal_data.get('option_type')}")
                logging.info("="*80)
                
                # Validate required fields
                required = ['symbol', 'strike', 'option_type', 'action', 'entry_price', 'stop_loss', 'expiry_date', 'quantity']
                missing = [f for f in required if f not in signal_data or signal_data[f] is None]
                
                if missing:
                    logging.error(f"[ERROR] Signal {signal_id} missing required fields: {missing}")
                    self._mark_processed(conn, signal_id)
                    continue
                
                # Log signal details
                logging.info(f"   Action: {signal_data.get('action')}")
                logging.info(f"   Entry Price: Rs.{signal_data.get('entry_price')}")
                logging.info(f"   Stop Loss: Rs.{signal_data.get('stop_loss')}")
                
                # Find instrument
                instrument = self.instruments.find_instrument(
                    signal_data['symbol'],
                    signal_data['strike'],
                    signal_data['option_type'],
                    signal_data['expiry_date']
                )
                
                if not instrument:
                    logging.error(f"[ERROR] Cannot find instrument for signal {signal_id}")
                    self._mark_processed(conn, signal_id)
                    continue
                
                # Build tradingsymbol
                tradingsymbol = self.instruments.get_tradingsymbol(instrument)
                if not tradingsymbol:
                    logging.error(f"[ERROR] Cannot build tradingsymbol for signal {signal_id}")
                    self._mark_processed(conn, signal_id)
                    continue
                
                logging.info(f"[SYMBOL] Trading as: {tradingsymbol}")
                
                # Place entry order
                entry_order_id = self._place_entry_order(signal_data, instrument, tradingsymbol)
                
                if entry_order_id:
                    # Save to orders table
                    self._save_order_to_db(signal_id, entry_order_id, signal_data, instrument, tradingsymbol)
                    logging.info(f"[OK] Entry order placed: {entry_order_id}")
                    logging.info(f"[WAIT] SL flag: TO_BE_PLACED (monitor will place SL when entry executes)")
                else:
                    logging.error(f"[ERROR] Failed to place entry order for signal {signal_id}")
                
                # Mark as processed
                self._mark_processed(conn, signal_id)
                
                # Rate limiting
                time.sleep(1)
                
            except Exception as e:
                logging.error(f"[ERROR] Error processing signal {signal_id}: {e}")
                self._mark_processed(conn, signal_id)
        
        conn.close()
        logging.info("="*80)
        logging.info("[OK] Finished processing all pending signals")
    
    def _place_entry_order(self, signal_data, instrument, tradingsymbol):
        """Place entry order with proper tick size rounding and retry logic"""
        try:
            # Get tick size from instrument
            tick_size = float(instrument['tick_size'])
            quantity = int(instrument['lot_size'])
            exchange = instrument['exchange']
            
            # Calculate entry price (signal price + 1 tick)
            entry_price = float(signal_data['entry_price']) + tick_size
            
            # Round to tick size
            entry_price = round(entry_price / tick_size) * tick_size
            
            logging.info(f"[SEND] Placing ENTRY order...")
            logging.info(f"[TICK] Tick size: Rs.{tick_size} | Price: Rs.{entry_price}")
            logging.info(f"[LOT] Quantity: {quantity}")
            
            if self.test_mode:
                logging.info(f"[TEST] TEST MODE - Would place entry order:")
                logging.info(f"   Symbol: {tradingsymbol}")
                logging.info(f"   Exchange: {exchange}")
                logging.info(f"   Action: {signal_data['action']}")
                logging.info(f"   Quantity: {quantity}")
                logging.info(f"   Price: Rs.{entry_price}")
                return f"TEST_ENTRY_{int(time.time())}"
            
            if not self.kite:
                logging.error("[ERROR] KiteConnect not initialized - cannot place order")
                return None
            
            # Determine product type based on exchange
            if exchange == 'MCX':
                product_type = self.kite.PRODUCT_NRML  # MCX requires NRML
                logging.info(f"[MCX] Using NRML product type")
            else:
                product_type = self.kite.PRODUCT_MIS  # NFO/BFO can use MIS
                logging.info(f"[{exchange}] Using MIS product type")
            
            # Retry logic for API connection errors
            max_retries = 3
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    # Place order
                    order_id = self.kite.place_order(
                        variety=self.kite.VARIETY_REGULAR,
                        exchange=exchange,
                        tradingsymbol=tradingsymbol,
                        transaction_type=signal_data['action'].upper(),
                        quantity=quantity,
                        product=product_type,
                        order_type=self.kite.ORDER_TYPE_LIMIT,
                        price=entry_price,
                        validity=self.kite.VALIDITY_DAY
                    )
                    
                    logging.info(f"[OK] Entry order placed successfully. Order ID: {order_id}")
                    return order_id
                    
                except Exception as e:
                    error_msg = str(e)
                    
                    # Check if it's a connection error that we should retry
                    if 'Connection aborted' in error_msg or 'RemoteDisconnected' in error_msg or 'timeout' in error_msg.lower():
                        if attempt < max_retries - 1:
                            logging.warning(f"[RETRY] Connection error, retrying in {retry_delay}s... (Attempt {attempt + 1}/{max_retries})")
                            time.sleep(retry_delay)
                            retry_delay *= 2  # Exponential backoff
                            continue
                        else:
                            logging.error(f"[ERROR] Max retries reached. Connection error: {error_msg}")
                            return None
                    else:
                        # Other errors (invalid params, etc.) - don't retry
                        logging.error(f"[ERROR] Order rejected: {error_msg}")
                        return None
            
            return None
            
        except Exception as e:
            logging.error(f"[ERROR] Error placing entry order: {e}")
            return None
    
    def _save_order_to_db(self, signal_id, entry_order_id, signal_data, instrument, tradingsymbol):
        """Save order to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Calculate trigger price (stop loss - 1 tick for safety)
            tick_size = float(instrument['tick_size'])
            stop_loss = float(signal_data['stop_loss'])
            trigger_price = stop_loss - tick_size
            trigger_price = round(trigger_price / tick_size) * tick_size
            
            cursor.execute("""
                INSERT INTO orders 
                (signal_id, entry_order_id, tradingsymbol, action, quantity, 
                 entry_price, stop_loss, trigger_price, entry_status, sl_flag,
                 entry_placed_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 'TO_BE_PLACED', ?, ?, ?)
            """, (
                signal_id,
                str(entry_order_id),
                tradingsymbol,
                signal_data['action'],
                int(instrument['lot_size']),
                float(signal_data['entry_price']),
                stop_loss,
                trigger_price,
                datetime.now().isoformat(),
                datetime.now().isoformat(),
                datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
            
            logging.info(f"[SAVE] Order saved to database - SL flag: TO_BE_PLACED")
            
        except Exception as e:
            logging.error(f"[ERROR] Error saving order to database: {e}")
    
    def _mark_processed(self, conn, signal_id):
        """Mark signal as processed"""
        try:
            cursor = conn.cursor()
            cursor.execute("UPDATE signals SET processed = 1 WHERE id = ?", (signal_id,))
            conn.commit()
            logging.info(f"[OK] Signal {signal_id} marked as processed")
        except Exception as e:
            logging.error(f"[ERROR] Error marking signal as processed: {e}")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Order Placer with Instrument Lookup')
    parser.add_argument('--continuous', action='store_true', help='Run continuously')
    parser.add_argument('--interval', type=int, default=30, help='Check interval in seconds')
    parser.add_argument('--test', action='store_true', help='Test mode (no real orders)')
    args = parser.parse_args()
    
    # Initialize KiteConnect
    kite = None
    if not args.test:
        try:
            with open('kite_config.json', 'r') as f:
                config = json.load(f)
            
            kite = KiteConnect(api_key=config['api_key'])
            kite.set_access_token(config['access_token'])
            
            # Verify connection
            profile = kite.profile()
            logging.info(f"[OK] Connected to Zerodha as {profile['user_name']}")
            
        except Exception as e:
            logging.error(f"[ERROR] Failed to initialize Kite: {e}")
            return
    
    # Initialize order placer
    placer = OrderPlacerProduction(kite=kite, test_mode=args.test)
    
    if args.continuous:
        logging.info("[START] Starting continuous order placement")
        logging.info(f"[INTERVAL] Checking every {args.interval} seconds")
        logging.info("Press Ctrl+C to stop")
        
        cycle = 0
        try:
            while True:
                cycle += 1
                logging.info(f"\n[CYCLE] Cycle #{cycle}")
                placer.process_pending_signals()
                time.sleep(args.interval)
        except KeyboardInterrupt:
            logging.info("\n[STOP] Stopping order placer")
    else:
        placer.process_pending_signals()


if __name__ == "__main__":
    main()
