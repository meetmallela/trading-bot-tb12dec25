"""
sl_monitor_with_trailing.py - SL Monitor with MCX Hours + Re-Entry Prevention

Features:
- MCX hours support (9 AM - 11:55 PM)
- NSE hours support (9:15 AM - 3:30 PM)
- Trailing SL with 30-second grace period
- Re-entry prevention (anti-revenge trading)
- No averaging down protection
"""

import sqlite3
import json
import logging
import time
import argparse
import pytz
from datetime import datetime
from kiteconnect import KiteConnect

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - SL_MONITOR - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sl_monitor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def is_market_open():
    """
    Check if market is open for trading
    NSE/NFO: 9:15 AM to 3:30 PM IST (Mon-Fri)
    MCX: 9:00 AM to 11:55 PM IST (Mon-Fri)
    
    Returns True if either NSE or MCX is open
    This allows monitoring positions across all exchanges
    """
    try:
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        
        # Check if weekend
        if now.weekday() >= 5:  # Saturday=5, Sunday=6
            return False
        
        # NSE/NFO hours: 9:15 AM to 3:30 PM IST
        nse_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        nse_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
        
        # MCX hours: 9:00 AM to 11:55 PM IST
        mcx_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
        mcx_close = now.replace(hour=23, minute=55, second=0, microsecond=0)
        
        # Return True if either market is open
        nse_is_open = nse_open <= now <= nse_close
        mcx_is_open = mcx_open <= now <= mcx_close
        
        return nse_is_open or mcx_is_open
    except:
        return True  # Safe default

class EnhancedSLMonitor:
    def __init__(self, check_interval=30, test_mode=False):
        self.check_interval = check_interval
        self.test_mode = test_mode
        self.db_path = 'trading.db'
        
        # Load Kite config
        with open('kite_config.json', 'r') as f:
            self.config = json.load(f)
        self.kite = KiteConnect(api_key=self.config['api_key'])
        self.kite.set_access_token(self.config['access_token'])
        
        # Track which positions already have SL orders
        self.protected_positions = set()
        
        # Track initial SL prices to detect trailing
        self.initial_sl_prices = {}
        
        # Track when SL was first placed (for 30-second transition rule)
        self.sl_placement_time = {}  # {tradingsymbol: timestamp}
        
        # Track SL exits to prevent re-entry (revenge trading)
        self.sl_exits_today = {}  # {tradingsymbol: date}
        
        # Load sl_exits from previous session if exists
        self._load_sl_exits()
        
        logging.info(f"[INIT] SL Monitor started (Mode: {'TEST' if test_mode else 'LIVE'})")
        logging.info(f"[INIT] Market hours: NSE 9:15-15:30, MCX 9:00-23:55")
    
    def _load_sl_exits(self):
        """Load SL exits from file to persist across restarts"""
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
                        logging.info(f"[INIT] Loaded {len(self.sl_exits_today)} SL exits from today")
                        for symbol in self.sl_exits_today:
                            logging.info(f"  - {symbol} (blocked from re-entry)")
        except Exception as e:
            logging.warning(f"[INIT] Could not load sl_exits: {e}")
    
    def _save_sl_exits(self):
        """Save SL exits to file"""
        try:
            with open('sl_exits.json', 'w') as f:
                json.dump(self.sl_exits_today, f, indent=2)
        except Exception as e:
            logging.warning(f"[WARN] Could not save sl_exits: {e}")
    
    def record_sl_exit(self, tradingsymbol):
        """Record that an instrument hit SL - block re-entry for rest of day"""
        from datetime import date
        
        today = date.today().isoformat()
        self.sl_exits_today[tradingsymbol] = today
        self._save_sl_exits()
        
        logging.warning(f"[BLACKLIST] {tradingsymbol} hit SL - NO RE-ENTRY allowed today")
    
    def is_blocked_from_reentry(self, tradingsymbol):
        """Check if instrument is blocked from re-entry due to SL exit"""
        from datetime import date
        
        if tradingsymbol in self.sl_exits_today:
            exit_date = self.sl_exits_today[tradingsymbol]
            today = date.today().isoformat()
            
            if exit_date == today:
                return True
        
        return False
    
    def get_signal_for_position(self, tradingsymbol):
        """Get original signal from database"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT parsed_data 
                FROM signals 
                WHERE json_extract(parsed_data, '$.tradingsymbol') = ?
                ORDER BY id DESC LIMIT 1
            """, (tradingsymbol,))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return json.loads(row['parsed_data'])
            return None
            
        except Exception as e:
            logging.error(f"[ERROR] Failed to get signal: {e}")
            return None
    
    def calculate_trailing_sl(self, position, current_price, current_sl_trigger, signal_data):
        """
        Calculate trailing SL based on price movement
        
        Returns:
            (new_sl_price, sl_type)
            sl_type can be: INITIAL, TRAILING, TRANSITION, NO_CHANGE
        """
        tradingsymbol = position['tradingsymbol']
        quantity = position['quantity']
        avg_price = abs(position['average_price'])
        pnl = position['pnl']
        
        # Determine if LONG or SHORT
        is_long = quantity > 0
        
        # Get stop loss from signal if available
        signal_sl = None
        if signal_data and signal_data.get('stop_loss'):
            signal_sl = float(signal_data['stop_loss'])
        
        # Calculate percentage from entry
        if is_long:
            pnl_percent = ((current_price - avg_price) / avg_price) * 100
        else:
            pnl_percent = ((avg_price - current_price) / avg_price) * 100
        
        # Store initial SL if not already stored
        if tradingsymbol not in self.initial_sl_prices:
            if signal_sl:
                self.initial_sl_prices[tradingsymbol] = signal_sl
            else:
                # Calculate 5% SL from entry
                if is_long:
                    self.initial_sl_prices[tradingsymbol] = avg_price * 0.95
                else:
                    self.initial_sl_prices[tradingsymbol] = avg_price * 1.05
        
        initial_sl = self.initial_sl_prices[tradingsymbol]
        
        # ========================================
        # TRAILING SL LOGIC - Based on CURRENT LTP
        # ========================================
        
        # Calculate what SL should be (5% below current LTP for LONG)
        if is_long:
            calculated_sl = current_price * 0.95  # 5% below current price
        else:
            calculated_sl = current_price * 1.05  # 5% above current price
        
        # Round to tick size
        tick_size = 0.05
        calculated_sl = round(calculated_sl / tick_size) * tick_size
        
        # SL should NEVER be worse than initial SL
        if is_long:
            # For LONG: new SL should be >= initial SL
            if calculated_sl < initial_sl:
                calculated_sl = initial_sl
        else:
            # For SHORT: new SL should be <= initial SL
            if calculated_sl > initial_sl:
                calculated_sl = initial_sl
        
        # Check if we need to trail (new SL better than current)
        if is_long:
            should_trail = calculated_sl > current_sl_trigger
        else:
            should_trail = calculated_sl < current_sl_trigger
        
        if should_trail:
            # Check if we're in grace period (first 30 seconds after SL placement)
            if tradingsymbol in self.sl_placement_time:
                time_since_placement = time.time() - self.sl_placement_time[tradingsymbol]
                
                if time_since_placement <= 30:
                    # Within 30 seconds - this is TRANSITION, not trailing yet
                    logging.info(f"   [TRANSITION] Within 30s grace period | LTP: {current_price:.2f} | New SL: {calculated_sl:.2f}")
                    return (calculated_sl, "TRANSITION")
            
            # After 30 seconds - trail it!
            logging.info(f"   [TRAILING] LTP: {current_price:.2f} (PnL: {pnl_percent:+.1f}%) | New SL: {calculated_sl:.2f} (5% below LTP)")
            return (calculated_sl, "TRAILING")
        
        # Below 3% profit - keep initial SL
        return (current_sl_trigger, "NO_CHANGE")
    
    def place_sl_order(self, position, signal_data):
        """Place stop-loss order for a position"""
        try:
            tradingsymbol = position['tradingsymbol']
            quantity = abs(position['quantity'])  # Always positive for order quantity
            avg_price = abs(position['average_price'])
            exchange = position['exchange']
            
            # Determine transaction type (opposite of position)
            if position['quantity'] > 0:
                transaction_type = self.kite.TRANSACTION_TYPE_SELL
            else:
                transaction_type = self.kite.TRANSACTION_TYPE_BUY
            
            # Get stop loss from signal if available
            if signal_data and signal_data.get('stop_loss'):
                sl_price = float(signal_data['stop_loss'])
                logging.info(f"   [SL SOURCE] Using signal SL: {sl_price}")
            else:
                # Calculate 5% SL below entry
                sl_price = avg_price * 0.95
                logging.info(f"   [SL FALLBACK] Using 5% SL: {sl_price} (entry: {avg_price})")
            
            # Round to tick size
            tick_size = 0.05
            sl_price = round(sl_price / tick_size) * tick_size
            
            # Set limit price slightly worse than trigger
            if transaction_type == self.kite.TRANSACTION_TYPE_SELL:
                limit_price = sl_price - 1
            else:
                limit_price = sl_price + 1
            
            if not self.test_mode:
                order_id = self.kite.place_order(
                    variety=self.kite.VARIETY_REGULAR,
                    exchange=exchange,
                    tradingsymbol=tradingsymbol,
                    transaction_type=transaction_type,
                    quantity=quantity,
                    product=self.kite.PRODUCT_MIS,
                    order_type=self.kite.ORDER_TYPE_SL,
                    trigger_price=sl_price,
                    price=limit_price
                )
                
                # Record placement time
                self.sl_placement_time[tradingsymbol] = time.time()
                
                logging.info(f"   [OK] SL placed at {sl_price} | Order ID: {order_id}")
                return order_id
            else:
                logging.info(f"   [TEST] Would place SL at {sl_price}")
                return f"TEST_{tradingsymbol}"
                
        except Exception as e:
            logging.error(f"   [ERROR] Failed to place SL: {e}")
            return None
    
    def get_existing_sl_order(self, tradingsymbol):
        """Find existing SL order for this position"""
        try:
            orders = self.kite.orders()
            for order in orders:
                if (order['tradingsymbol'] == tradingsymbol and 
                    order['order_type'] in ['SL', 'SL-M'] and
                    order['status'] in ['TRIGGER PENDING', 'OPEN']):
                    return order
            return None
        except Exception as e:
            logging.error(f"[ERROR] Failed to get orders: {e}")
            return None

    def modify_sl_order(self, order_id, tradingsymbol, new_sl_price, quantity, transaction_type, exchange):
        """Modify existing SL order with new trigger price"""
        try:
            # Set limit price slightly worse than trigger
            if transaction_type == self.kite.TRANSACTION_TYPE_SELL:
                price = new_sl_price - 1
            else:
                price = new_sl_price + 1
            
            if not self.test_mode:
                self.kite.modify_order(
                    variety=self.kite.VARIETY_REGULAR,
                    order_id=order_id,
                    trigger_price=new_sl_price,
                    price=price
                )
                logging.info(f"[OK] SL MODIFIED - Order ID: {order_id} | New SL: {new_sl_price}")
                return True
            else:
                logging.info(f"[TEST] Would modify SL to {new_sl_price}")
                return True
                
        except Exception as e:
            logging.error(f"[ERROR] Failed to modify SL: {e}")
            return False

    def monitor_open_positions(self):
        """Main monitoring logic - checks positions and manages SLs"""
        try:
            # Check if market is open
            if not is_market_open():
                ist = pytz.timezone('Asia/Kolkata')
                now = datetime.now(ist)
                logging.info(f"[PAUSED] Market closed at {now.strftime('%H:%M:%S')} IST")
                logging.info(f"[INFO] Market hours: NSE 9:15-15:30, MCX 9:00-23:55")
                return
            
            # Get positions
            positions = self.kite.positions()['net']
            
            # Filter open positions
            open_positions = [p for p in positions if p['quantity'] != 0]
            
            if not open_positions:
                logging.info("[STATUS] No open positions to monitor.")
                return
            
            # ========================================
            # Visual separator for new monitoring cycle
            # ========================================
            logging.info("")
            logging.info("#" * 80)
            logging.info(f"### MONITORING CYCLE - {len(open_positions)} OPEN POSITIONS ###")
            logging.info("#" * 80)
            logging.info("")
            
            # Check each position
            for pos in open_positions:
                tradingsymbol = pos['tradingsymbol']
                quantity = pos['quantity']
                pnl = pos['pnl']
                ltp = pos['last_price']
                avg_price = abs(pos['average_price'])
                
                # ========================================
                # Visual separator between positions
                # ========================================
                logging.info("")
                logging.info("=" * 80)
                logging.info(f"[POSITION] {tradingsymbol}")
                logging.info(f"   Qty: {quantity} | Avg: {avg_price:.2f} | LTP: {ltp:.2f} | PnL: {pnl:.2f}")
                
                # Find original signal
                signal_data = self.get_signal_for_position(tradingsymbol)
                
                if signal_data:
                    logging.info(f"   [OK] Signal found in database")
                else:
                    logging.info(f"   [WARN] No signal found - using fallback logic")
                
                # Check if SL order already exists
                existing_sl = self.get_existing_sl_order(tradingsymbol)
                
                if existing_sl:
                    # SL exists - check if we need to trail it
                    current_sl_trigger = existing_sl['trigger_price']
                    
                    # Calculate what the SL should be now
                    new_sl, sl_type = self.calculate_trailing_sl(pos, ltp, current_sl_trigger, signal_data)
                    
                    # Handle SL updates for TRAILING or TRANSITION
                    if (sl_type in ["TRAILING", "TRANSITION"]) and new_sl > current_sl_trigger:
                        # Update the SL
                        success = self.modify_sl_order(
                            existing_sl['order_id'],
                            tradingsymbol,
                            new_sl,
                            abs(quantity),
                            existing_sl['transaction_type'],
                            pos['exchange']
                        )
                        
                        if success:
                            logging.info(f"   [✓] {sl_type} SL updated: {current_sl_trigger:.2f} → {new_sl:.2f}")
                    else:
                        logging.info(f"   [CURRENT SL] {current_sl_trigger:.2f} ({sl_type})")
                else:
                    # No SL exists - place one
                    if tradingsymbol not in self.protected_positions:
                        logging.info(f"   [NEW] No SL found - placing initial SL")
                        order_id = self.place_sl_order(pos, signal_data)
                        
                        if order_id:
                            self.protected_positions.add(tradingsymbol)
                            logging.info(f"   [OK] Position now protected")
                
                # Close position separator
                logging.info("=" * 80)
            
            # ========================================
            # Check for closed positions (SL exits)
            # ========================================
            current_symbols = set([p['tradingsymbol'] for p in open_positions])
            
            for protected_symbol in list(self.protected_positions):
                if protected_symbol not in current_symbols:
                    # Position was closed - likely SL triggered!
                    logging.info("")
                    logging.info("!" * 80)
                    logging.warning(f"[CLOSED] {protected_symbol} position closed - likely SL exit")
                    self.record_sl_exit(protected_symbol)
                    self.protected_positions.remove(protected_symbol)
                    logging.info("!" * 80)
            
            # ========================================
            # End of monitoring cycle
            # ========================================
            logging.info("")
            logging.info("#" * 80)
            logging.info("### MONITORING CYCLE COMPLETE ###")
            logging.info("#" * 80)
            logging.info("")
            
            # End of monitoring cycle separator
            logging.info("")
            logging.info("="*60)
            logging.info("[CYCLE COMPLETE]")
            logging.info("="*60)
                    
        except Exception as e:
            logging.error(f"[ERROR] Monitoring failed: {e}")
            import traceback
            traceback.print_exc()


    def run(self):
        """Main monitoring loop"""
        logging.info("="*60)
        logging.info("[START] SL Monitor Active (WITH TRAILING SL)")
        logging.info(f"Check Interval: {self.check_interval}s")
        logging.info("="*60)
        
        while True:
            try:
                self.monitor_open_positions()
            except KeyboardInterrupt:
                logging.info("\n[STOP] Shutting down...")
                break
            except Exception as e:
                logging.error(f"[ERROR] Loop error: {e}")
            
            time.sleep(self.check_interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--interval', type=int, default=30, help='Check interval (seconds)')
    parser.add_argument('--test', action='store_true', help='Test mode (no real orders)')
    args = parser.parse_args()
    
    monitor = EnhancedSLMonitor(check_interval=args.interval, test_mode=args.test)
    monitor.run()
