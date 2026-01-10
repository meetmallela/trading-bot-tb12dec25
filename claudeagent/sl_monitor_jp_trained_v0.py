"""
sl_monitor_jp_trained.py
SL Monitor for JP channel trained signals
Monitors positions, places SL orders, trails SL as price moves
Reads from: jp_signals_trained.db
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
        self.db_path = 'jp_signals_trained.db'  # JP database
        
        # Load Kite config
        with open('kite_config.json', 'r') as f:
            self.config = json.load(f)
        self.kite = KiteConnect(api_key=self.config['api_key'])
        self.kite.set_access_token(self.config['access_token'])
        
        # Track which positions already have SL orders
        self.protected_positions = set()
        
        # Track initial SL prices to detect trailing
        self.initial_sl_prices = {}
        
        logging.info(f"[INIT] JP SL Monitor started (Mode: {'TEST' if test_mode else 'LIVE'})")

    def get_signal_for_position(self, tradingsymbol):
        """Find the original JP signal that created this position"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Search for signal with matching tradingsymbol
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
        """Calculate SL at 95% of average buy price for CE, 105% for PE"""
        avg_price = abs(position['average_price'])
        tradingsymbol = position['tradingsymbol']
        
        # Check if PE or CE
        if 'PE' in tradingsymbol:
            # For PUT options, SL should be above entry (105%)
            sl_price = avg_price * 1.05
        else:
            # For CALL options, SL should be below entry (95%)
            sl_price = avg_price * 0.95
        
        sl_price = int(sl_price)  # Round to nearest rupee
        return sl_price

    def calculate_trailing_sl(self, position, current_ltp, current_sl_trigger, signal_data=None):
        """
        Calculate trailing SL with RECURSIVE/CONTINUOUS trailing:
        
        For CALL (CE):
        - Trail SL up when price moves favorably
        - Every 5% gain -> move SL up by 5%
        
        For PUT (PE):
        - Trail SL down when price moves favorably
        - Every 5% gain -> move SL down by 5%
        """
        tradingsymbol = position['tradingsymbol']
        is_put = 'PE' in tradingsymbol
        
        # Get signal data if available
        if signal_data and 'stop_loss' in signal_data:
            signal_sl = float(signal_data['stop_loss'])
        else:
            signal_sl = None
        
        # Calculate profit percentage
        avg_price = abs(position['average_price'])
        profit_pct = ((current_ltp - avg_price) / avg_price) * 100
        
        # Determine if we should trail
        if is_put:
            # For PUT: trail when price goes DOWN (negative profit_pct is good)
            if profit_pct < -5:  # 5% profit on PUT (price dropped)
                # Trail SL down
                new_sl = current_sl_trigger * 0.95
                logging.info(f"[TRAIL] PUT trailing: {current_sl_trigger} -> {new_sl:.2f}")
                return int(new_sl)
        else:
            # For CALL: trail when price goes UP
            if profit_pct > 5:  # 5% profit on CALL
                # Trail SL up
                new_sl = current_sl_trigger * 1.05
                logging.info(f"[TRAIL] CALL trailing: {current_sl_trigger} -> {new_sl:.2f}")
                return int(new_sl)
        
        # No trailing needed
        return current_sl_trigger

    def place_sl_order(self, position, signal_data=None):
        """Place SL order for a position"""
        tradingsymbol = position['tradingsymbol']
        exchange = position['exchange']
        quantity = abs(position['quantity'])
        
        # Determine SL price
        if signal_data and 'stop_loss' in signal_data:
            sl_price = float(signal_data['stop_loss'])
            logging.info(f"[SL] Using signal SL: {sl_price}")
        else:
            sl_price = self.calculate_fallback_sl(position)
            logging.info(f"[SL] Using fallback SL: {sl_price}")
        
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
            # Place SL order
            order_id = self.kite.place_order(
                variety='regular',
                exchange=exchange,
                tradingsymbol=tradingsymbol,
                transaction_type='SELL',
                quantity=quantity,
                order_type='SL-M',
                product='MIS',
                trigger_price=sl_price
            )
            
            logging.info(f"[SUCCESS] SL order placed: {order_id}")
            self.protected_positions.add(tradingsymbol)
            return True
            
        except Exception as e:
            logging.error(f"[ERROR] Failed to place SL: {e}")
            return False

    def update_trailing_sl(self, position, current_ltp, signal_data=None):
        """Update SL order if trailing condition is met"""
        tradingsymbol = position['tradingsymbol']
        
        # Get current SL orders
        try:
            orders = self.kite.orders()
            
            # Find existing SL order for this symbol
            sl_order = None
            for order in orders:
                if (order['tradingsymbol'] == tradingsymbol and 
                    order['order_type'] == 'SL-M' and 
                    order['status'] in ['OPEN', 'TRIGGER PENDING']):
                    sl_order = order
                    break
            
            if not sl_order:
                logging.info(f"[INFO] No active SL order found for {tradingsymbol}")
                return
            
            current_trigger = float(sl_order['trigger_price'])
            
            # Calculate new trailing SL
            new_trigger = self.calculate_trailing_sl(
                position, 
                current_ltp, 
                current_trigger, 
                signal_data
            )
            
            # Update if needed
            if new_trigger != current_trigger:
                if self.test_mode:
                    logging.info(f"[TEST] Would update SL: {current_trigger} -> {new_trigger}")
                    return
                
                try:
                    # Cancel old order
                    self.kite.cancel_order(variety='regular', order_id=sl_order['order_id'])
                    
                    # Place new order
                    quantity = abs(position['quantity'])
                    exchange = position['exchange']
                    
                    new_order_id = self.kite.place_order(
                        variety='regular',
                        exchange=exchange,
                        tradingsymbol=tradingsymbol,
                        transaction_type='SELL',
                        quantity=quantity,
                        order_type='SL-M',
                        product='MIS',
                        trigger_price=new_trigger
                    )
                    
                    logging.info(f"[TRAIL] SL updated: {current_trigger} -> {new_trigger} (Order: {new_order_id})")
                    
                except Exception as e:
                    logging.error(f"[ERROR] Failed to update trailing SL: {e}")
        
        except Exception as e:
            logging.error(f"[ERROR] Failed to check orders: {e}")

    def monitor_positions(self):
        """Monitor all positions and place/update SL orders"""
        try:
            # Get all positions
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
                except:
                    logging.warning(f"[WARN] Could not get LTP for {tradingsymbol}")
                    continue
                
                # Calculate P&L
                pnl = (current_ltp - avg_price) * quantity
                pnl_pct = ((current_ltp - avg_price) / avg_price) * 100
                
                logging.info("")
                logging.info(f"[POSITION] {tradingsymbol}")
                logging.info(f"  Qty: {quantity} | Avg: {avg_price:.2f} | LTP: {current_ltp:.2f}")
                logging.info(f"  P&L: {pnl:.2f} ({pnl_pct:.2f}%)")
                
                # Get signal data from JP database
                signal_data = self.get_signal_for_position(tradingsymbol)
                if signal_data:
                    logging.info(f"  Signal SL: {signal_data.get('stop_loss', 'N/A')}")
                
                # Place SL if not already protected
                if tradingsymbol not in self.protected_positions:
                    logging.info(f"  [ACTION] Placing SL order...")
                    self.place_sl_order(position, signal_data)
                else:
                    # Check for trailing
                    logging.info(f"  [CHECK] Checking for trailing...")
                    self.update_trailing_sl(position, current_ltp, signal_data)
            
        except Exception as e:
            logging.error(f"[ERROR] Monitor failed: {e}")
            import traceback
            traceback.print_exc()

    def run(self):
        """Main monitoring loop"""
        logging.info(f"[START] Monitoring positions every {self.check_interval} seconds")
        logging.info("[INFO] Press Ctrl+C to stop")
        
        while True:
            try:
                self.monitor_positions()
                time.sleep(self.check_interval)
            except KeyboardInterrupt:
                logging.info("\n[STOP] Monitoring stopped by user")
                break
            except Exception as e:
                logging.error(f"[ERROR] {e}")
                time.sleep(self.check_interval)

def main():
    parser = argparse.ArgumentParser(description='JP SL Monitor - Trained Agent Signals')
    parser.add_argument('--test', action='store_true', help='Test mode (no real orders)')
    parser.add_argument('--interval', type=int, default=30, help='Check interval in seconds')
    
    args = parser.parse_args()
    
    print("")
    print("="*70)
    print("JP SL MONITOR - TRAINED AGENT")
    print("="*70)
    print(f"Mode: {'TEST' if args.test else 'LIVE'}")
    print(f"Database: jp_signals_trained.db")
    print(f"Interval: {args.interval} seconds")
    print("="*70)
    print("")
    
    monitor = SLMonitorJPTrained(
        check_interval=args.interval,
        test_mode=args.test
    )
    
    monitor.run()

if __name__ == '__main__':
    main()
