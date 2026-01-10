"""
sl_monitor_enhanced.py - Enhanced Stop Loss Monitoring Service

Features:
1. Monitors orders placed by order_placer (existing)
2. Fetches all open positions from Kite (NEW)
3. Auto-places SL for unprotected positions (NEW)

Protects BOTH:
- Orders from order_placer (tracked in database)
- Manual orders (fetched from Kite positions)
"""

import sqlite3
import time
import logging
import json
import os
from datetime import datetime
from kiteconnect import KiteConnect
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - SL_MONITOR - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sl_monitor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


def load_kite_config():
    """Load Kite credentials from kite_config.json"""
    config_file = 'kite_config.json'
    
    if not os.path.exists(config_file):
        logging.error(f"[ERROR] Config file not found: {config_file}")
        return None
    
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        if not config.get('api_key') or not config.get('access_token'):
            logging.error("[ERROR] Invalid config file")
            return None
        
        logging.info(f"[OK] Loaded Kite config for user: {config.get('user_name', 'Unknown')}")
        return config
        
    except Exception as e:
        logging.error(f"[ERROR] Error loading config: {e}")
        return None


class EnhancedSLMonitor:
    """Enhanced SL Monitor - protects both bot orders and manual positions"""
    
    def __init__(self, api_key=None, access_token=None, check_interval=30):
        """
        Initialize Enhanced SL Monitor
        
        Args:
            api_key: Zerodha API key (optional - will load from config)
            access_token: Zerodha access token (optional - will load from config)
            check_interval: Seconds between checks (default: 30)
        """
        self.check_interval = check_interval
        self.db_path = 'trading.db'
        
        # Load credentials from config if not provided
        if not api_key or not access_token:
            config = load_kite_config()
            if not config:
                raise Exception("No Kite credentials available")
            api_key = config['api_key']
            access_token = config['access_token']
            logging.info(f"[NOTE] Using credentials from kite_config.json")
        
        # Initialize Kite Connect
        self.kite = KiteConnect(api_key=api_key)
        self.kite.set_access_token(access_token)
        
        # Test connection
        try:
            profile = self.kite.profile()
            logging.info(f"[OK] Connected to Kite: {profile['user_name']}")
        except Exception as e:
            logging.error(f"[ERROR] Kite connection failed: {e}")
            raise
        
        # Connect to database
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        logging.info(f"[OK] Connected to database: {self.db_path}")
        
        # Create manual_positions table if not exists
        self._create_manual_positions_table()
    
    def _create_manual_positions_table(self):
        """Create table to track manual positions"""
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS manual_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tradingsymbol TEXT,
                quantity INTEGER,
                avg_price REAL,
                last_price REAL,
                pnl REAL,
                sl_order_id TEXT,
                sl_placed_at TEXT,
                detected_at TEXT,
                updated_at TEXT,
                UNIQUE(tradingsymbol)
            )
        """)
        self.conn.commit()
        logging.info("[OK] Manual positions table ready")
    
    # ============================================================================
    # PART 1: Monitor Bot Orders (Existing Functionality)
    # ============================================================================
    
    def check_order_status(self, order_id):
        """Check order status from Kite API"""
        try:
            orders = self.kite.orders()
            
            for order in orders:
                if order['order_id'] == order_id:
                    return order['status']
            
            logging.warning(f"[WARNING] Order {order_id} not found")
            return 'UNKNOWN'
            
        except Exception as e:
            logging.error(f"[ERROR] Error checking order status: {e}")
            return 'ERROR'
    
    def place_sl_order_from_db(self, order_record):
        """Place SL order based on database record with LTP validation"""
        try:
            tradingsymbol = order_record['tradingsymbol']
            quantity = order_record['quantity']
            action = order_record['action']
            stop_loss = order_record['stop_loss']
            trigger_price = order_record['trigger_price']
            
            # Get current LTP to validate SL
            try:
                quote = self.kite.ltp([f"NFO:{tradingsymbol}"])
                ltp = quote[f"NFO:{tradingsymbol}"]['last_price']
            except:
                try:
                    quote = self.kite.ltp([f"MCX:{tradingsymbol}"])
                    ltp = quote[f"MCX:{tradingsymbol}"]['last_price']
                except:
                    ltp = None
            
            # SL transaction is opposite of entry
            sl_transaction = 'SELL' if action.upper() == 'BUY' else 'BUY'
            
            # Validate SL against current price
            if ltp:
                if sl_transaction == 'SELL' and trigger_price >= ltp:
                    logging.warning(f"[⚠️ SL SKIP] LTIM: LTP={ltp}, SL trigger={trigger_price}")
                    logging.warning(f"[INFO] Stop loss already breached! Price dropped from entry to {ltp}")
                    logging.warning(f"[INFO] Marking as FAILED - position needs manual exit")
                    # Mark as failed since SL is already hit
                    self._update_sl_failed(order_record['entry_order_id'], "SL_ALREADY_BREACHED")
                    return None
                elif sl_transaction == 'BUY' and trigger_price <= ltp:
                    logging.warning(f"[⚠️ SL SKIP] Stop loss trigger {trigger_price} already breached (LTP: {ltp})")
                    logging.warning(f"[INFO] Marking as FAILED - position needs manual exit")
                    self._update_sl_failed(order_record['entry_order_id'], "SL_ALREADY_BREACHED")
                    return None
            
            # Determine exchange
            commodity_prefixes = ['GOLD', 'SILVER', 'NATURALGAS', 'CRUDEOIL', 'COPPER', 'ZINC', 'LEAD', 'NICKEL']
            is_commodity = any(tradingsymbol.startswith(prefix) for prefix in commodity_prefixes)
            
            exchange = self.kite.EXCHANGE_MCX if is_commodity else self.kite.EXCHANGE_NFO
            product = self.kite.PRODUCT_NRML if is_commodity else self.kite.PRODUCT_MIS
            
            logging.info(f"[BOT ORDER] Placing SL: {tradingsymbol} {sl_transaction} {quantity}")
            logging.info(f"   LTP: ₹{ltp:.2f} | SL Trigger: ₹{trigger_price:.2f}")
            
            sl_order_id = self.kite.place_order(
                variety=self.kite.VARIETY_REGULAR,
                exchange=exchange,
                tradingsymbol=tradingsymbol,
                transaction_type=sl_transaction,
                quantity=int(quantity),
                product=product,
                order_type=self.kite.ORDER_TYPE_SL,
                price=float(stop_loss),
                trigger_price=float(trigger_price),
                validity=self.kite.VALIDITY_DAY
            )
            
            logging.info(f"[OK] SL order placed: {sl_order_id}")
            return sl_order_id
            
        except Exception as e:
            logging.error(f"[ERROR] Error placing SL order: {e}")
            return None
    
    def monitor_bot_orders(self):
        """Monitor orders placed by order_placer"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT * FROM orders 
                WHERE sl_flag = 'TO_BE_PLACED'
                ORDER BY created_at ASC
            """)
            
            pending_orders = cursor.fetchall()
            
            if not pending_orders:
                return
            
            logging.info(f"[BOT ORDERS] Monitoring {len(pending_orders)} pending SL orders")
            
            for order in pending_orders:
                entry_order_id = order['entry_order_id']
                entry_status = order['entry_status']
                
                # Check current entry status
                current_status = self.check_order_status(entry_order_id)
                
                if current_status == 'COMPLETE':
                    if entry_status != 'COMPLETE':
                        self._update_entry_status(entry_order_id, 'COMPLETE')
                    
                    # Place SL order
                    logging.info(f"[OK] Entry order {entry_order_id} is COMPLETE!")
                    sl_order_id = self.place_sl_order_from_db(order)
                    
                    if sl_order_id:
                        self._update_sl_placed(entry_order_id, sl_order_id)
                    else:
                        logging.error(f"[ERROR] Failed to place SL for entry {entry_order_id}")
                
                elif current_status in ['REJECTED', 'CANCELLED']:
                    logging.warning(f"[SKIP] Entry order {entry_order_id} is {current_status}")
                    self._update_sl_skipped(entry_order_id, current_status)
                
                elif current_status != entry_status:
                    self._update_entry_status(entry_order_id, current_status)
            
        except Exception as e:
            logging.error(f"[ERROR] Error monitoring bot orders: {e}")
    
    # ============================================================================
    # PART 2: Monitor Open Positions (NEW - for manual orders)
    # ============================================================================
    
    def get_open_positions(self):
        """Fetch all open positions from Kite"""
        try:
            positions = self.kite.positions()
            
            # Get net positions (day + overnight combined)
            net_positions = positions.get('net', [])
            
            # Filter only open positions (quantity != 0)
            open_positions = [p for p in net_positions if p['quantity'] != 0]
            
            logging.info(f"[POSITIONS] Found {len(open_positions)} open positions")
            return open_positions
            
        except Exception as e:
            logging.error(f"[ERROR] Error fetching positions: {e}")
            return []
    
    def has_sl_order(self, tradingsymbol, position_qty):
        """
        Check if position has a matching SL order
        
        Args:
            tradingsymbol: Trading symbol
            position_qty: Position quantity (can be positive or negative)
            
        Returns:
            bool: True if SL order exists, False otherwise
        """
        try:
            orders = self.kite.orders()
            
            # Determine expected SL direction
            # If position is BUY (positive qty), SL should be SELL
            # If position is SELL (negative qty), SL should be BUY
            expected_sl_type = 'SELL' if position_qty > 0 else 'BUY'
            
            for order in orders:
                if (order['tradingsymbol'] == tradingsymbol and
                    order['transaction_type'] == expected_sl_type and
                    order['order_type'] in ['SL', 'SL-M'] and
                    order['status'] not in ['CANCELLED', 'REJECTED', 'COMPLETE']):
                    
                    logging.debug(f"[OK] SL order exists for {tradingsymbol}: {order['order_id']}")
                    return True
            
            return False
            
        except Exception as e:
            logging.error(f"[ERROR] Error checking SL orders: {e}")
            return False
    
    def place_sl_for_position(self, position):
        """
        Place SL order for an unprotected position
        
        Args:
            position: Position dict from Kite
            
        Returns:
            SL order ID if successful, None otherwise
        """
        try:
            tradingsymbol = position['tradingsymbol']
            quantity = abs(position['quantity'])  # Use absolute value
            avg_price = position['average_price']
            last_price = position['last_price']
            
            # Determine if BUY or SELL position
            is_buy_position = position['quantity'] > 0
            
            # Calculate SL price (2% below for BUY, 2% above for SELL)
            if is_buy_position:
                sl_price = avg_price * 0.98  # 2% below
                trigger_price = avg_price * 0.98
                sl_transaction = 'SELL'
            else:
                sl_price = avg_price * 1.02  # 2% above
                trigger_price = avg_price * 1.02
                sl_transaction = 'BUY'
            
            # Determine tick size based on instrument type
            tick_size = 0.05  # Default for options
            
            # Check if it's a futures contract (has FUT in name)
            if 'FUT' in tradingsymbol:
                tick_size = 0.50  # Futures typically have 0.50 tick size
            
            # Check if commodity
            commodity_prefixes = ['GOLD', 'SILVER', 'NATURALGAS', 'CRUDEOIL', 'COPPER', 'ZINC']
            is_commodity = any(tradingsymbol.startswith(prefix) for prefix in commodity_prefixes)
            
            if is_commodity:
                tick_size = 0.50  # MCX commodities use 0.50
            
            # Round to tick size
            sl_price = round(sl_price / tick_size) * tick_size
            trigger_price = round(trigger_price / tick_size) * tick_size
            
            # Determine exchange and product
            exchange = self.kite.EXCHANGE_MCX if is_commodity else self.kite.EXCHANGE_NFO
            product = self.kite.PRODUCT_NRML if is_commodity else self.kite.PRODUCT_MIS
            
            logging.info(f"[MANUAL POSITION] Placing SL for {tradingsymbol}")
            logging.info(f"   Position: {position['quantity']} @ ₹{avg_price:.2f}")
            logging.info(f"   SL: {sl_transaction} {quantity} @ ₹{sl_price:.2f} (Trigger: ₹{trigger_price:.2f})")
            
            sl_order_id = self.kite.place_order(
                variety=self.kite.VARIETY_REGULAR,
                exchange=exchange,
                tradingsymbol=tradingsymbol,
                transaction_type=sl_transaction,
                quantity=int(quantity),
                product=product,
                order_type=self.kite.ORDER_TYPE_SL,
                price=float(sl_price),
                trigger_price=float(trigger_price),
                validity=self.kite.VALIDITY_DAY
            )
            
            logging.info(f"[OK] SL placed for manual position: {sl_order_id}")
            
            # Save to manual_positions table
            self._save_manual_position(position, sl_order_id)
            
            return sl_order_id
            
        except Exception as e:
            logging.error(f"[ERROR] Error placing SL for position: {e}")
            return None
    
    def monitor_open_positions(self):
        """Monitor all open positions and place SL if missing"""
        try:
            positions = self.get_open_positions()
            
            if not positions:
                return
            
            unprotected_count = 0
            
            for position in positions:
                tradingsymbol = position['tradingsymbol']
                quantity = position['quantity']
                pnl = position['pnl']
                
                # Skip if already has SL
                if self.has_sl_order(tradingsymbol, quantity):
                    logging.debug(f"[OK] {tradingsymbol} already protected")
                    continue
                
                # Check if we already placed SL for this position
                if self._has_manual_sl_record(tradingsymbol):
                    logging.debug(f"[OK] {tradingsymbol} SL already placed by bot")
                    continue
                
                # Unprotected position found!
                unprotected_count += 1
                logging.warning(f"[⚠️ UNPROTECTED] {tradingsymbol} | Qty: {quantity} | P&L: ₹{pnl:.2f}")
                
                # Place SL order
                sl_order_id = self.place_sl_for_position(position)
                
                if sl_order_id:
                    logging.info(f"[✓ PROTECTED] {tradingsymbol} now has SL order")
                else:
                    logging.error(f"[✗ FAILED] Could not place SL for {tradingsymbol}")
                
                time.sleep(1)  # Small delay between orders
            
            if unprotected_count > 0:
                logging.info(f"[SUMMARY] Protected {unprotected_count} unprotected positions")
            
        except Exception as e:
            logging.error(f"[ERROR] Error monitoring positions: {e}")
    
    # ============================================================================
    # Database Helper Methods
    # ============================================================================
    
    def _update_entry_status(self, entry_order_id, status):
        """Update entry order status"""
        try:
            cursor = self.conn.cursor()
            now = datetime.now().isoformat()
            
            if status == 'COMPLETE':
                cursor.execute("""
                    UPDATE orders 
                    SET entry_status = ?, entry_filled_at = ?, updated_at = ?
                    WHERE entry_order_id = ?
                """, (status, now, now, entry_order_id))
            else:
                cursor.execute("""
                    UPDATE orders 
                    SET entry_status = ?, updated_at = ?
                    WHERE entry_order_id = ?
                """, (status, now, entry_order_id))
            
            self.conn.commit()
            
        except Exception as e:
            logging.error(f"[ERROR] Error updating entry status: {e}")
    
    def _update_sl_placed(self, entry_order_id, sl_order_id):
        """Update SL order details"""
        try:
            cursor = self.conn.cursor()
            now = datetime.now().isoformat()
            
            cursor.execute("""
                UPDATE orders 
                SET sl_order_id = ?, sl_flag = 'ORDER_PLACED', sl_placed_at = ?, updated_at = ?
                WHERE entry_order_id = ?
            """, (sl_order_id, now, now, entry_order_id))
            
            self.conn.commit()
            
        except Exception as e:
            logging.error(f"[ERROR] Error updating SL status: {e}")
    
    def _update_sl_skipped(self, entry_order_id, reason):
        """Mark SL as skipped"""
        try:
            cursor = self.conn.cursor()
            now = datetime.now().isoformat()
            
            cursor.execute("""
                UPDATE orders 
                SET sl_flag = ?, updated_at = ?
                WHERE entry_order_id = ?
            """, (f'SKIPPED_{reason}', now, entry_order_id))
            
            self.conn.commit()
            
        except Exception as e:
            logging.error(f"[ERROR] Error marking SL as skipped: {e}")
    
    def _update_sl_failed(self, entry_order_id, reason):
        """Mark SL as failed (e.g., already breached)"""
        try:
            cursor = self.conn.cursor()
            now = datetime.now().isoformat()
            
            cursor.execute("""
                UPDATE orders 
                SET sl_flag = ?, updated_at = ?
                WHERE entry_order_id = ?
            """, (f'FAILED_{reason}', now, entry_order_id))
            
            self.conn.commit()
            logging.info(f"[DB] Marked SL as {reason} for order {entry_order_id}")
            
        except Exception as e:
            logging.error(f"[ERROR] Error marking SL as failed: {e}")
    
    def _save_manual_position(self, position, sl_order_id):
        """Save manual position to database"""
        try:
            cursor = self.conn.cursor()
            now = datetime.now().isoformat()
            
            cursor.execute("""
                INSERT OR REPLACE INTO manual_positions 
                (tradingsymbol, quantity, avg_price, last_price, pnl, sl_order_id, sl_placed_at, detected_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                position['tradingsymbol'],
                position['quantity'],
                position['average_price'],
                position['last_price'],
                position['pnl'],
                sl_order_id,
                now,
                now,
                now
            ))
            
            self.conn.commit()
            
        except Exception as e:
            logging.error(f"[ERROR] Error saving manual position: {e}")
    
    def _has_manual_sl_record(self, tradingsymbol):
        """Check if manual SL was already placed"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT sl_order_id FROM manual_positions 
                WHERE tradingsymbol = ? AND sl_order_id IS NOT NULL
            """, (tradingsymbol,))
            
            return cursor.fetchone() is not None
            
        except Exception as e:
            logging.error(f"[ERROR] Error checking manual SL record: {e}")
            return False
    
    # ============================================================================
    # Main Monitoring Loop
    # ============================================================================
    
    def run(self):
        """
        Main monitoring loop
        Monitors both bot orders and manual positions
        """
        logging.info("="*80)
        logging.info("ENHANCED SL MONITOR - STARTED")
        logging.info("="*80)
        logging.info(f"Check interval: {self.check_interval} seconds")
        logging.info("Monitoring:")
        logging.info("  1. Bot orders (from order_placer)")
        logging.info("  2. Manual positions (from Kite)")
        logging.info("="*80)
        
        cycle = 0
        
        try:
            while True:
                cycle += 1
                logging.info(f"\n[CYCLE {cycle}] {datetime.now().strftime('%H:%M:%S')}")
                
                # Part 1: Monitor bot orders
                self.monitor_bot_orders()
                
                # Part 2: Monitor manual positions
                self.monitor_open_positions()
                
                logging.info(f"[WAIT] Sleeping {self.check_interval}s...")
                time.sleep(self.check_interval)
                
        except KeyboardInterrupt:
            logging.info("\n[STOP] Stopping SL monitor...")
        except Exception as e:
            logging.error(f"\n[ERROR] Fatal error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.conn.close()
            logging.info("[DONE] SL monitor stopped")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Enhanced SL Monitor')
    parser.add_argument('--interval', type=int, default=30, help='Check interval in seconds')
    args = parser.parse_args()
    
    try:
        monitor = EnhancedSLMonitor(check_interval=args.interval)
        monitor.run()
    except Exception as e:
        logging.error(f"[FATAL] Failed to start monitor: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
