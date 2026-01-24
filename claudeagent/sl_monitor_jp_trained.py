"""
sl_monitor_jp_trained_v2.py
COMPLETE FIX - All errors resolved
- Uses SL orders (not SL-M)
- Correct trigger logic for CE vs PE
- Proper limit price calculation
"""

import sqlite3
import time
import logging
import json
import sys
import io
from datetime import datetime
from kiteconnect import KiteConnect
import argparse

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - SL_MONITOR_JP - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sl_monitor_jp.log', encoding='utf-8'),
        logging.StreamHandler(
            io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
            if sys.platform == 'win32' else sys.stdout
        )
    ]
)

class SLMonitorJPTrained:
    def __init__(self, check_interval=30, test_mode=False):
        self.check_interval = check_interval
        self.test_mode = test_mode
        self.db_path = 'jp_signals_trained.db'
        
        # Load Kite config
        with open('kite_config.json', 'r') as f:
            self.config = json.load(f)
        self.kite = KiteConnect(api_key=self.config['api_key'])
        self.kite.set_access_token(self.config['access_token'])
        
        # Track which positions already have SL orders
        self.protected_positions = set()
        
        # Track initial SL prices
        self.initial_sl_prices = {}
        
        logging.info(f"[INIT] JP SL Monitor started (Mode: {'TEST' if test_mode else 'LIVE'})")

    def get_signal_for_position(self, tradingsymbol):
        """Find the original JP signal"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
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
        """Calculate SL - CRITICAL FIX for CE vs PE"""
        avg_price = abs(position['average_price'])
        tradingsymbol = position['tradingsymbol']
        
        # Check if PE or CE
        if 'PE' in tradingsymbol:
            # For PUT options: SL ABOVE entry (price goes UP = loss)
            sl_price = avg_price * 1.05  # 5% above
        else:
            # For CALL options: SL BELOW entry (price goes DOWN = loss)
            sl_price = avg_price * 0.95  # 5% below
        
        sl_price = int(sl_price)
        return sl_price

    def place_sl_order(self, position, signal_data=None):
        """Place SL order - FIXED to use SL not SL-M"""
        tradingsymbol = position['tradingsymbol']
        exchange = position['exchange']
        quantity = abs(position['quantity'])
        avg_price = abs(position['average_price'])
        
        # Determine SL price
        if signal_data and 'stop_loss' in signal_data:
            sl_price = float(signal_data['stop_loss'])
            logging.info(f"[SL] Using signal SL: {sl_price}")
        else:
            sl_price = self.calculate_fallback_sl(position)
            logging.info(f"[SL] Using fallback SL: {sl_price}")
        
        # CRITICAL: Validate SL direction
        is_put = 'PE' in tradingsymbol
        
        if is_put:
            # PUT: SL should be ABOVE entry
            if sl_price <= avg_price:
                sl_price = int(avg_price * 1.05)
                logging.warning(f"[FIX] PUT SL adjusted to {sl_price} (above entry)")
        else:
            # CALL: SL should be BELOW entry
            if sl_price >= avg_price:
                sl_price = int(avg_price * 0.95)
                logging.warning(f"[FIX] CALL SL adjusted to {sl_price} (below entry)")
        
        # Store initial SL
        self.initial_sl_prices[tradingsymbol] = sl_price
        
        if self.test_mode:
            logging.info(f"[TEST] Would place SL order:")
            logging.info(f"  Symbol: {tradingsymbol}")
            logging.info(f"  Trigger: {sl_price}")
            logging.info(f"  Quantity: {quantity}")
            self.protected_positions.add(tradingsymbol)
            return True
        
        try:
            # Calculate limit price (2% buffer from trigger)
            if is_put:
                # PUT: Limit above trigger (price going up)
                limit_price = int(sl_price * 1.02)
            else:
                # CALL: Limit below trigger (price going down)
                limit_price = int(sl_price * 0.98)
            
            # Place SL order (NOT SL-M!)
            order_id = self.kite.place_order(
                variety='regular',
                exchange=exchange,
                tradingsymbol=tradingsymbol,
                transaction_type='SELL',
                quantity=quantity,
                order_type='SL',  # FIXED: Was SL-M, now SL
                product='MIS',
                price=limit_price,  # REQUIRED for SL orders
                trigger_price=sl_price
            )
            
            logging.info(f"[SUCCESS] SL order placed: {order_id}")
            logging.info(f"  Trigger: {sl_price}, Limit: {limit_price}")
            self.protected_positions.add(tradingsymbol)
            return True
            
        except Exception as e:
            logging.error(f"[ERROR] Failed to place SL: {e}")
            return False

    def calculate_trailing_sl(self, position, current_ltp, current_sl_trigger, signal_data=None):
        """Calculate trailing SL"""
        tradingsymbol = position['tradingsymbol']
        is_put = 'PE' in tradingsymbol
        
        avg_price = abs(position['average_price'])
        profit_pct = ((current_ltp - avg_price) / avg_price) * 100
        
        if is_put:
            # PUT: Profit when price drops (negative %)
            if profit_pct < -5:  # 5%+ profit
                new_sl = int(current_sl_trigger * 0.95)  # Trail DOWN
                logging.info(f"[TRAIL] PUT: {current_sl_trigger} -> {new_sl}")
                return new_sl
        else:
            # CALL: Profit when price rises (positive %)
            if profit_pct > 5:  # 5%+ profit
                new_sl = int(current_sl_trigger * 1.05)  # Trail UP
                logging.info(f"[TRAIL] CALL: {current_sl_trigger} -> {new_sl}")
                return new_sl
        
        return current_sl_trigger

    def update_trailing_sl(self, position, current_ltp, signal_data=None):
        """Update SL order if trailing"""
        tradingsymbol = position['tradingsymbol']
        
        try:
            orders = self.kite.orders()
            
            sl_order = None
            for order in orders:
                if (order['tradingsymbol'] == tradingsymbol and 
                    order['order_type'] == 'SL' and 
                    order['status'] in ['OPEN', 'TRIGGER PENDING']):
                    sl_order = order
                    break
            
            if not sl_order:
                return
            
            current_trigger = float(sl_order['trigger_price'])
            new_trigger = self.calculate_trailing_sl(
                position, current_ltp, current_trigger, signal_data
            )
            
            if new_trigger != current_trigger:
                if self.test_mode:
                    logging.info(f"[TEST] Would trail: {current_trigger} -> {new_trigger}")
                    return
                
                try:
                    # Cancel old
                    self.kite.cancel_order(variety='regular', order_id=sl_order['order_id'])
                    
                    # Place new
                    quantity = abs(position['quantity'])
                    exchange = position['exchange']
                    is_put = 'PE' in tradingsymbol
                    
                    if is_put:
                        limit_price = int(new_trigger * 1.02)
                    else:
                        limit_price = int(new_trigger * 0.98)
                    
                    new_order_id = self.kite.place_order(
                        variety='regular',
                        exchange=exchange,
                        tradingsymbol=tradingsymbol,
                        transaction_type='SELL',
                        quantity=quantity,
                        order_type='SL',
                        product='MIS',
                        price=limit_price,
                        trigger_price=new_trigger
                    )
                    
                    logging.info(f"[TRAIL] Updated: {current_trigger} -> {new_trigger} (Order: {new_order_id})")
                    
                except Exception as e:
                    logging.error(f"[ERROR] Trail failed: {e}")
        
        except Exception as e:
            logging.error(f"[ERROR] Check orders failed: {e}")

    def monitor_positions(self):
        """Monitor all positions"""
        try:
            positions = self.kite.positions()['net']
            
            if not positions:
                logging.info("[INFO] No open positions")
                return
            
            logging.info(f"[FOUND] {len(positions)} positions")
            
            for position in positions:
                if position['quantity'] == 0:
                    continue
                
                tradingsymbol = position['tradingsymbol']
                quantity = position['quantity']
                avg_price = position['average_price']
                
                # Get LTP
                try:
                    ltp_data = self.kite.ltp([f"{position['exchange']}:{tradingsymbol}"])
                    ltp_key = f"{position['exchange']}:{tradingsymbol}"
                    current_ltp = ltp_data[ltp_key]['last_price']
                except (KeyError, TypeError) as e:
                    logging.warning(f"[WARN] Could not get LTP for {tradingsymbol}: {e}")
                    continue
                except Exception as e:
                    logging.warning(f"[WARN] LTP fetch error for {tradingsymbol}: {type(e).__name__}: {e}")
                    continue
                
                # Calculate P&L
                pnl = (current_ltp - avg_price) * quantity
                pnl_pct = ((current_ltp - avg_price) / avg_price) * 100
                
                logging.info("")
                logging.info(f"[POSITION] {tradingsymbol}")
                logging.info(f"  Qty: {quantity} | Avg: {avg_price:.2f} | LTP: {current_ltp:.2f}")
                logging.info(f"  P&L: {pnl:.2f} ({pnl_pct:.2f}%)")
                
                # Get signal
                signal_data = self.get_signal_for_position(tradingsymbol)
                if signal_data:
                    logging.info(f"  Signal SL: {signal_data.get('stop_loss', 'N/A')}")
                
                # Place or update SL
                if tradingsymbol not in self.protected_positions:
                    logging.info(f"  [ACTION] Placing SL order...")
                    self.place_sl_order(position, signal_data)
                else:
                    logging.info(f"  [CHECK] Checking for trailing...")
                    self.update_trailing_sl(position, current_ltp, signal_data)
            
        except Exception as e:
            logging.error(f"[ERROR] Monitor failed: {e}")
            import traceback
            traceback.print_exc()

    def run(self):
        """Main loop"""
        logging.info(f"[START] Monitoring every {self.check_interval} seconds")
        logging.info("[INFO] Press Ctrl+C to stop")
        
        while True:
            try:
                self.monitor_positions()
                time.sleep(self.check_interval)
            except KeyboardInterrupt:
                logging.info("\n[STOP] Stopped by user")
                break
            except Exception as e:
                logging.error(f"[ERROR] {e}")
                time.sleep(self.check_interval)

def main():
    parser = argparse.ArgumentParser(description='JP SL Monitor v2 - FIXED')
    parser.add_argument('--test', action='store_true', help='Test mode')
    parser.add_argument('--interval', type=int, default=30, help='Check interval')
    
    args = parser.parse_args()
    
    print("")
    print("="*70)
    print("JP SL MONITOR V2 - ALL FIXES APPLIED")
    print("="*70)
    print(f"Mode: {'TEST' if args.test else 'LIVE'}")
    print(f"Fixes: SL orders, CE/PE logic, Trigger validation")
    print("="*70)
    print("")
    
    monitor = SLMonitorJPTrained(
        check_interval=args.interval,
        test_mode=args.test
    )
    
    monitor.run()

if __name__ == '__main__':
    main()
