"""
sl_monitor_enhanced.py - COMPLETE VERSION
Monitors open positions and places SL orders automatically
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
                    product=self.kite.PRODUCT_NRML,
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
        """Monitor positions and place SL orders if needed"""
        try:
            # Get all open positions
            positions = self.kite.positions()['net']
            open_positions = [p for p in positions if p['quantity'] != 0]
            
            if not open_positions:
                logging.info("[STATUS] No open positions to monitor.")
                return
            
            logging.info(f"[STATUS] Monitoring {len(open_positions)} open positions")
            
            # Check each position
            for pos in open_positions:
                tradingsymbol = pos['tradingsymbol']
                quantity = pos['quantity']
                pnl = pos['pnl']
                
                logging.info(f"")
                logging.info(f"[POSITION] {tradingsymbol}")
                logging.info(f"   Qty: {quantity} | PnL: {pnl:.2f}")
                
                # Check if already protected
                if tradingsymbol in self.protected_positions:
                    logging.info(f"   [OK] Already protected with SL")
                    continue
                
                # Find original signal
                signal_data = self.get_signal_for_position(tradingsymbol)
                
                if signal_data:
                    logging.info(f"   [OK] Signal found in database")
                else:
                    logging.info(f"   [WARN] No signal found - will use fallback SL (95% of buy price)")
                
                # Check if SL order already exists in orderbook
                orders = self.kite.orders()
                sl_exists = any(
                    o['tradingsymbol'] == tradingsymbol and 
                    o['order_type'] in ['SL', 'SL-M'] and
                    o['status'] in ['TRIGGER PENDING', 'OPEN']
                    for o in orders
                )
                
                if sl_exists:
                    logging.info(f"   [OK] SL order already exists")
                    self.protected_positions.add(tradingsymbol)
                    continue
                
                # Place SL order (with signal data if available, fallback otherwise)
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
        logging.info("[START] SL Monitor Active")
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
