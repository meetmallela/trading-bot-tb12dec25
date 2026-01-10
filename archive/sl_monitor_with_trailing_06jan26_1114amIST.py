"""
sl_monitor_enhanced.py - WITH TRAILING SL
Monitors open positions, places SL orders, and trails SL as price moves favorably
"""

import sqlite3
import time
import logging
import json
import os
import sys
import io
from datetime import datetime
from kiteconnect import KiteConnect
import argparse
import pytz  # For IST timezone

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - SL_MONITOR - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sl_monitor.log', encoding='utf-8'),
        logging.StreamHandler(
            io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
            if sys.platform == 'win32' else sys.stdout
        )
    ]
)

def is_market_open():
    """
    Check if market is open for regular orders
    NSE/NFO: 9:15 AM to 3:30 PM IST (Mon-Fri)
    """
    try:
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        
        # Check if weekend
        if now.weekday() >= 5:  # Saturday=5, Sunday=6
            return False
        
        # Market hours: 9:15 AM to 3:30 PM IST
        market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
        
        return market_open <= now <= market_close
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
        
        logging.info(f"[INIT] SL Monitor started (Mode: {'TEST' if test_mode else 'LIVE'})")

    def get_signal_for_position(self, tradingsymbol):
        """Find the original signal that created this position"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Search for signal with matching symbol
            cursor.execute("""
                SELECT * FROM signals 
                WHERE parsed_data LIKE ? 
                AND processed = 1
                ORDER BY timestamp DESC
                LIMIT 1
            """, (f'%{tradingsymbol}%',))
            
            signal = cursor.fetchone()
            conn.close()
            
            if signal:
                return json.loads(signal['parsed_data'])
            return None
            
        except Exception as e:
            logging.error(f"[ERROR] Database lookup failed: {e}")
            return None

    def calculate_fallback_sl(self, position):
        """Calculate SL at 95% of average buy price, rounded down to nearest rupee"""
        avg_price = abs(position['average_price'])
        sl_price = avg_price * 0.95
        sl_price = int(sl_price)  # Round down to nearest rupee
        return sl_price

    def calculate_trailing_sl(self, position, current_ltp, current_sl_trigger, signal_data=None):
        """
        Calculate trailing SL with RECURSIVE/CONTINUOUS trailing:
        
        MANUAL ORDERS (no signal):
        - Initial SL at 95% of buy price
        - Every time price moves 5% above CURRENT SL, trail SL up by 5%
        - This creates a continuous trailing effect
        
        Example:
        Buy @ 2.69 -> Initial SL @ 2.55 (95%)
        LTP hits 2.82 (105% of 2.69) -> Trail SL to 2.79 (104% of 2.69)
        LTP hits 2.93 (105% of 2.79) -> Trail SL to 2.92 (105% of 2.79)
        LTP hits 3.07 (105% of 2.92) -> Trail SL to 3.06 (105% of 2.92)
        And so on...
        
        SIGNAL-BASED ORDERS:
        - Use original signal's stop_loss (can add trailing logic later)
        """
        avg_price = abs(position['average_price'])
        quantity = position['quantity']
        
        # For LONG positions (quantity > 0)
        if quantity > 0:
            # Check if we have a signal
            if signal_data and signal_data.get('stop_loss'):
                # CUSTOM LOGIC: First SL vs Trailing SL
                tradingsymbol = position.get('tradingsymbol')
                
                # Check if this is the FIRST time we're setting SL
                if tradingsymbol not in self.sl_placement_time:
                    # FIRST TIME: Use signal's SL as-is
                    signal_sl = signal_data.get('stop_loss')
                    logging.info(f"[FIRST SL] Using signal SL: {signal_sl} (will transition to 95% after 30s)")
                    
                    # Record the time we placed this first SL
                    import time
                    self.sl_placement_time[tradingsymbol] = time.time()
                    
                    return signal_sl, "SIGNAL"
                
                else:
                    # NOT FIRST TIME: Check if 30 seconds have passed
                    import time
                    time_elapsed = time.time() - self.sl_placement_time[tradingsymbol]
                    
                    if time_elapsed < 30:
                        # Still within 30 seconds - keep signal SL
                        signal_sl = signal_data.get('stop_loss')
                        logging.info(f"[HOLD] Within 30s ({int(time_elapsed)}s) - keeping signal SL: {signal_sl}")
                        return signal_sl, "SIGNAL"
                    
                    else:
                        # 30+ seconds passed - START TRAILING LOGIC
                        
                        # If no current SL trigger (shouldn't happen, but safety check)
                        if current_sl_trigger is None or current_sl_trigger == 0:
                            # Move to 95% of entry
                            new_sl = int(avg_price * 0.95)
                            logging.info(f"[TRANSITION] 30s passed - moving SL to 95% of entry: {avg_price} * 0.95 = {new_sl}")
                            return new_sl, "INITIAL_TRAIL"
                        
                        # Calculate threshold: 105% of entry price
                        threshold_price = avg_price * 1.05
                        
                        if current_ltp >= threshold_price:
                            # Price moved up 5% - trail SL to 95% of current LTP
                            new_sl = int(current_ltp * 0.95)  # Round down
                            
                            # Safety check: new SL must be higher than current SL
                            if new_sl > current_sl_trigger:
                                logging.info(f"[TRAIL] LTP {current_ltp} >= threshold {threshold_price:.2f} - moving SL to 95% of LTP: {new_sl}")
                                return new_sl, "TRAILING"
                            else:
                                # New SL not higher, keep current
                                return current_sl_trigger, "HOLD"
                        else:
                            # Price hasn't reached 105% threshold yet
                            # Keep current SL
                            return current_sl_trigger, "HOLD"
            
            else:
                # NO SIGNAL DATA - Manual order: Use SAME logic as signal orders!
                
                # If no current SL trigger (first time), use 95% of buy
                if current_sl_trigger is None or current_sl_trigger == 0:
                    new_sl = int(avg_price * 0.95)
                    logging.info(f"[MANUAL] Initial SL: 95% of entry {avg_price} = {new_sl}")
                    return new_sl, "INITIAL"
                
                # Calculate threshold: 105% of ENTRY price (NOT current SL!)
                threshold_price = avg_price * 1.05
                
                if current_ltp >= threshold_price:
                    # Trail SL to 95% of current LTP
                    new_sl = int(current_ltp * 0.95)  # 95% of current LTP, rounded down
                    
                    # Safety check: new SL must be higher than current SL
                    if new_sl > current_sl_trigger:
                        logging.info(f"[MANUAL TRAIL] LTP {current_ltp} >= threshold {threshold_price:.2f} - moving SL to {new_sl}")
                        return new_sl, "TRAILING"
                    else:
                        return current_sl_trigger, "HOLD"
                else:
                    # Not yet time to trail, keep current SL
                    return current_sl_trigger, "HOLD"
        
        # For SHORT positions (quantity < 0) - inverse logic
        else:
            if signal_data and signal_data.get('stop_loss'):
                return signal_data.get('stop_loss'), "SIGNAL"
            else:
                if current_sl_trigger is None or current_sl_trigger == 0:
                    new_sl = int(avg_price * 1.05)
                    return new_sl, "INITIAL"
                
                threshold_price = current_sl_trigger * 0.95
                
                if current_ltp <= threshold_price:
                    new_sl = int(current_ltp * 1.05)
                    if new_sl < current_sl_trigger:
                        return new_sl, "TRAILING"
                    else:
                        return current_sl_trigger, "HOLD"
                else:
                    return current_sl_trigger, "HOLD"

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
                logging.info(f"[TEST MODE] Would modify SL to {new_sl_price}")
                return True
                
        except Exception as e:
            logging.error(f"[ERROR] Failed to modify SL: {e}")
            return False

    def place_sl_order(self, position, signal_data=None):
        """Place SL order for a position"""
        try:
            tradingsymbol = position['tradingsymbol']
            quantity = abs(position['quantity'])
            
            # Determine transaction type (opposite of position)
            transaction_type = self.kite.TRANSACTION_TYPE_SELL if position['quantity'] > 0 else self.kite.TRANSACTION_TYPE_BUY
            
            # Get SL price from signal OR calculate fallback
            if signal_data and signal_data.get('stop_loss'):
                sl_price = signal_data.get('stop_loss')
                logging.info(f"[SL SOURCE] From signal database")
            else:
                sl_price = self.calculate_fallback_sl(position)
                avg_price = abs(position['average_price'])
                logging.warning(f"[SL SOURCE] FALLBACK - No signal found")
                logging.info(f"   Buy Price: {avg_price} | SL at 95%: {sl_price}")
            
            # Store initial SL for tracking
            self.initial_sl_prices[tradingsymbol] = sl_price
            
            # Determine exchange
            exchange = position['exchange']
            
            logging.info(f"[PLACING SL] {tradingsymbol}")
            logging.info(f"   Quantity: {quantity} | SL Price: {sl_price}")
            
            if not self.test_mode:
                # Place SL order (Stop Loss Limit order)
                # SL order requires both trigger_price and price
                # Set price slightly worse than trigger to ensure execution
                if transaction_type == self.kite.TRANSACTION_TYPE_SELL:
                    price = sl_price - 1  # For selling, limit price below trigger
                else:
                    price = sl_price + 1  # For buying, limit price above trigger
                
                order_id = self.kite.place_order(
                    variety=self.kite.VARIETY_REGULAR,
                    exchange=exchange,
                    tradingsymbol=tradingsymbol,
                    transaction_type=transaction_type,
                    quantity=quantity,
                    product=self.kite.PRODUCT_MIS,  # FIXED: Changed to MIS to match entry orders
                    order_type=self.kite.ORDER_TYPE_SL,
                    trigger_price=sl_price,
                    price=price
                )
                logging.info(f"[OK] SL PLACED - Order ID: {order_id}")
                return order_id
            else:
                logging.info(f"[TEST MODE] Would place SL at {sl_price}")
                return "TEST_ORDER"
                
        except Exception as e:
            logging.error(f"[ERROR] Failed to place SL: {e}")
            return None

    def monitor_open_positions(self):
        """Monitor positions and place/trail SL orders if needed"""
        
        # CHECK MARKET HOURS FIRST
        if not is_market_open():
            ist = pytz.timezone('Asia/Kolkata')
            current_time = datetime.now(ist).strftime('%H:%M:%S')
            logging.info(f"[PAUSED] Market closed at {current_time} IST")
            logging.info(f"[INFO] Market hours: 9:15 AM - 3:30 PM IST (Mon-Fri)")
            return
        
        try:
            # Get all open positions
            positions = self.kite.positions()['net']
            open_positions = [p for p in positions if p['quantity'] != 0]
            
            if not open_positions:
                logging.info("[STATUS] No open positions to monitor.")
                return
            
            # Visual separator for new monitoring cycle
            logging.info("")
            logging.info("")
            logging.info("#" * 80)
            logging.info(f"[STATUS] Monitoring {len(open_positions)} open positions")
            
            # Check each position
            for pos in open_positions:
                tradingsymbol = pos['tradingsymbol']
                quantity = pos['quantity']
                pnl = pos['pnl']
                ltp = pos['last_price']
                avg_price = abs(pos['average_price'])
                
                logging.info(f"")
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
                    
                    if sl_type == "TRAILING" and new_sl > current_sl_trigger:
                        # Trail the SL upward
                        profit_from_buy = ((ltp - avg_price) / avg_price) * 100
                        logging.info(f"   [TRAILING] Price moved! LTP: {ltp:.2f} (+{profit_from_buy:.1f}% from buy)")
                        logging.info(f"   [TRAILING] Current SL: {current_sl_trigger} -> New SL: {new_sl}")
                        
                        success = self.modify_sl_order(
                            order_id=existing_sl['order_id'],
                            tradingsymbol=tradingsymbol,
                            new_sl_price=new_sl,
                            quantity=abs(quantity),
                            transaction_type=self.kite.TRANSACTION_TYPE_SELL if quantity > 0 else self.kite.TRANSACTION_TYPE_BUY,
                            exchange=pos['exchange']
                        )
                        
                        if success:
                            self.initial_sl_prices[tradingsymbol] = new_sl
                    else:
                        if sl_type == "HOLD":
                            # Calculate correct threshold (105% of entry price)
                            threshold = avg_price * 1.05
                            logging.info(f"   [OK] SL at {current_sl_trigger} | Need LTP >= {threshold:.2f} to trail (105% of entry {avg_price:.2f})")
                        else:
                            logging.info(f"   [OK] SL at {current_sl_trigger} | Type: {sl_type}")
                    
                    self.protected_positions.add(tradingsymbol)
                    
                else:
                    # No SL exists - place one
                    logging.info(f"   [WARN] UNPROTECTED - Placing SL order...")
                    order_id = self.place_sl_order(pos, signal_data)
                    
                    if order_id:
                        self.protected_positions.add(tradingsymbol)
                        logging.info(f"   [OK] Position now protected")
                    
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
    
    monitor = EnhancedSLMonitor(
        check_interval=args.interval,
        test_mode=args.test
    )
    monitor.run()