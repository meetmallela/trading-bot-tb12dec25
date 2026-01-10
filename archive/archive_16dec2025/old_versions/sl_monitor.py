"""
sl_monitor.py - Stop Loss Monitoring Service
Monitors orders with sl_flag='TO_BE_PLACED'
Checks if entry order is executed using Kite API
Places SL order when entry is COMPLETE
"""

import sqlite3
import time
import logging
import json
import os
from datetime import datetime
from kiteconnect import KiteConnect
import argparse

# Configure logging (Windows-compatible - no emojis)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - SL_MONITOR - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sl_monitor.log', encoding='utf-8'),
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
        logging.error(f"[ERROR] Config file not found: {config_file}")
        logging.error("   Run: python auth_with_token_save.py to generate it")
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


class SLMonitor:
    def __init__(self, api_key=None, access_token=None, check_interval=30):
        """
        Initialize SL Monitor
        Automatically loads credentials from kite_config.json if not provided
        
        Args:
            api_key: Zerodha API key (optional - will load from config)
            access_token: Zerodha access token (optional - will load from config)
            check_interval: Seconds between checks (default: 30)
        """
        self.check_interval = check_interval
        self.db_path = 'trading.db'
        
        # Load credentials from config file if not provided
        if not api_key or not access_token:
            config = load_kite_config()
            if not config:
                raise Exception("No Kite credentials available. Run: python auth_with_token_save.py")
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
    
    def check_order_status(self, order_id):
        """
        Check order status from Kite API
        
        Args:
            order_id: Kite order ID
            
        Returns:
            Status string: COMPLETE, PENDING, REJECTED, CANCELLED, etc.
        """
        try:
            # Fetch all orders
            orders = self.kite.orders()
            
            # Find our order
            for order in orders:
                if order['order_id'] == order_id:
                    status = order['status']
                    filled_qty = order.get('filled_quantity', 0)
                    
                    logging.debug(f"Order {order_id}: Status={status}, Filled={filled_qty}")
                    return status
            
            logging.warning(f"[WARNING]  Order {order_id} not found in order list")
            return 'UNKNOWN'
            
        except Exception as e:
            logging.error(f"[ERROR] Error checking order status: {e}")
            return 'ERROR'
    
    def place_sl_order(self, order_record):
        """
        Place SL order based on order record from database
        Uses the working Kite code format
        Supports both NFO (equity) and MCX (commodities)
        
        Args:
            order_record: Database row with order details
            
        Returns:
            SL order ID if successful, None otherwise
        """
        try:
            tradingsymbol = order_record['tradingsymbol']
            quantity = order_record['quantity']
            action = order_record['action']
            stop_loss = order_record['stop_loss']
            trigger_price = order_record['trigger_price']
            
            # Determine exchange based on tradingsymbol
            # MCX commodities: GOLD, SILVER, NATURALGAS, CRUDEOIL, etc.
            commodity_prefixes = ['GOLD', 'SILVER', 'NATURALGAS', 'CRUDEOIL', 'COPPER', 'ZINC', 'LEAD', 'NICKEL']
            is_commodity = any(tradingsymbol.startswith(prefix) for prefix in commodity_prefixes)
            
            if is_commodity:
                exchange = self.kite.EXCHANGE_MCX
                logging.info(f"[MCX] Detected commodity instrument")
            else:
                exchange = self.kite.EXCHANGE_NFO
            
            # SL transaction is opposite of entry
            sl_transaction = 'SELL' if action.upper() == 'BUY' else 'BUY'
            
            logging.info(f"[SEND] Placing SL order:")
            logging.info(f"   Symbol: {tradingsymbol}")
            logging.info(f"   Exchange: {exchange}")
            logging.info(f"   Action: {sl_transaction}")
            logging.info(f"   Quantity: {quantity}")
            logging.info(f"   Trigger: ₹{trigger_price}")
            logging.info(f"   Price: ₹{stop_loss}")
            
            # Place SL order using working Kite code format
            sl_order_id = self.kite.place_order(
                variety=self.kite.VARIETY_REGULAR,
                exchange=exchange,  # NFO or MCX based on instrument
                tradingsymbol=tradingsymbol,
                transaction_type=sl_transaction,
                quantity=int(quantity),
                product=self.kite.PRODUCT_MIS,
                order_type=self.kite.ORDER_TYPE_SL,  # Stop-loss LIMIT order
                price=float(stop_loss),              # Limit price after trigger
                trigger_price=float(trigger_price),  # Order triggers at this price
                validity=self.kite.VALIDITY_DAY
            )
            
            logging.info(f"[OK] SL order placed successfully. Order ID: {sl_order_id}")
            return sl_order_id
            
        except Exception as e:
            logging.error(f"[ERROR] Error placing SL order: {e}")
            return None
    
    def update_entry_status(self, entry_order_id, status):
        """Update entry order status in database"""
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
            logging.debug(f"Entry order {entry_order_id} status updated to {status}")
            
        except Exception as e:
            logging.error(f"[ERROR] Error updating entry status: {e}")
    
    def update_sl_placed(self, entry_order_id, sl_order_id):
        """Update SL order details in database"""
        try:
            cursor = self.conn.cursor()
            now = datetime.now().isoformat()
            
            cursor.execute("""
                UPDATE orders 
                SET sl_order_id = ?, sl_flag = 'ORDER_PLACED', sl_placed_at = ?, updated_at = ?
                WHERE entry_order_id = ?
            """, (sl_order_id, now, now, entry_order_id))
            
            self.conn.commit()
            logging.info(f"[SAVE] SL flag updated to 'ORDER_PLACED' for entry {entry_order_id}")
            
        except Exception as e:
            logging.error(f"[ERROR] Error updating SL status: {e}")
    
    def update_sl_skipped(self, entry_order_id, reason):
        """Mark SL as skipped (entry rejected/cancelled)"""
        try:
            cursor = self.conn.cursor()
            now = datetime.now().isoformat()
            
            cursor.execute("""
                UPDATE orders 
                SET sl_flag = ?, updated_at = ?
                WHERE entry_order_id = ?
            """, (f'SKIPPED_{reason}', now, entry_order_id))
            
            self.conn.commit()
            logging.info(f"[SKIP]  SL skipped for entry {entry_order_id}: {reason}")
            
        except Exception as e:
            logging.error(f"[ERROR] Error updating SL skip status: {e}")
    
    def get_pending_sl_orders(self):
        """
        Get orders where sl_flag = 'TO_BE_PLACED'
        These are entry orders waiting for SL placement
        
        Returns:
            List of database rows
        """
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                SELECT * FROM orders
                WHERE sl_flag = 'TO_BE_PLACED'
                ORDER BY created_at ASC
            """)
            
            return cursor.fetchall()
            
        except Exception as e:
            logging.error(f"[ERROR] Error fetching pending SL orders: {e}")
            return []
    
    def process_pending_sl_orders(self):
        """Process all orders with pending SL placement"""
        
        pending = self.get_pending_sl_orders()
        
        if not pending:
            logging.debug("No pending SL orders")
            return
        
        logging.info(f"\n[INFO] Found {len(pending)} orders with pending SL placement")
        print("="*80)
        
        for order in pending:
            entry_order_id = order['entry_order_id']
            tradingsymbol = order['tradingsymbol']
            
            logging.info(f"\n[CHECK] Checking entry order: {entry_order_id} ({tradingsymbol})")
            
            # Check entry order status
            status = self.check_order_status(entry_order_id)
            
            if status == 'COMPLETE':
                logging.info(f"[OK] Entry order {entry_order_id} is COMPLETE!")
                
                # Update database status
                self.update_entry_status(entry_order_id, 'COMPLETE')
                
                # Place SL order
                sl_order_id = self.place_sl_order(order)
                
                if sl_order_id:
                    # Update database with SL order ID and flag
                    self.update_sl_placed(entry_order_id, sl_order_id)
                    logging.info(f"[OK] SL order {sl_order_id} placed for entry {entry_order_id}")
                else:
                    logging.error(f"[ERROR] Failed to place SL for entry {entry_order_id}")
                    # Don't update flag - will retry on next cycle
            
            elif status in ['REJECTED', 'CANCELLED']:
                logging.warning(f"[WARNING]  Entry order {entry_order_id} is {status}")
                
                # Update database status and skip SL
                self.update_entry_status(entry_order_id, status)
                self.update_sl_skipped(entry_order_id, status)
            
            elif status == 'PENDING' or status == 'OPEN':
                logging.info(f"[WAIT] Entry order {entry_order_id} still {status}, waiting...")
                # Update status in database
                self.update_entry_status(entry_order_id, status)
            
            else:
                logging.warning(f"[WARNING]  Entry order {entry_order_id} has unknown status: {status}")
            
            # Small delay between checks
            time.sleep(1)
        
        print("="*80 + "\n")
    
    def run(self):
        """Run continuous monitoring"""
        logging.info(f"\n{'='*80}")
        logging.info("SL MONITORING SERVICE - STARTED")
        logging.info(f"{'='*80}")
        logging.info(f"Check interval: {self.check_interval} seconds")
        logging.info(f"Database: {self.db_path}")
        logging.info("Press Ctrl+C to stop")
        logging.info(f"{'='*80}\n")
        
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
                        logging.info(f"[TIME] All markets closed (Equity: 9:15-15:30, MCX: 9:00-23:55)")
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
                
                # Process pending SL orders
                logging.info(f"[CYCLE] Cycle #{cycle} - {current_time.strftime('%H:%M:%S')}")
                self.process_pending_sl_orders()
                
                # Wait before next check
                time.sleep(self.check_interval)
                
        except KeyboardInterrupt:
            logging.info("\n[STOP] SL Monitoring Service stopped")
        finally:
            self.conn.close()
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()


def main():
    parser = argparse.ArgumentParser(description='SL Monitor - Monitor and place stop-loss orders')
    parser.add_argument('--api-key', type=str,
                       help='Kite API key (optional - reads from kite_config.json)')
    parser.add_argument('--access-token', type=str,
                       help='Kite access token (optional - reads from kite_config.json)')
    parser.add_argument('--check-interval', type=int, default=30,
                       help='Seconds between checks (default: 30)')
    
    args = parser.parse_args()
    
    print("\n" + "="*80)
    print("SL MONITORING SERVICE")
    print("="*80)
    print(f"Check Interval: {args.check_interval}s")
    print(f"Credentials: {'From kite_config.json' if not args.api_key else 'From command line'}")
    print("="*80 + "\n")
    
    # Initialize and run monitor (will auto-load from kite_config.json)
    try:
        monitor = SLMonitor(
            api_key=args.api_key,
            access_token=args.access_token,
            check_interval=args.check_interval
        )
        
        monitor.run()
        
    except Exception as e:
        logging.error(f"[ERROR] Failed to start SL Monitor: {e}")
        print("\n" + "="*80)
        print("[WARNING]  SETUP REQUIRED")
        print("="*80)
        print("Please run: python auth_with_token_save.py")
        print("This will generate kite_config.json with your access token")
        print("="*80 + "\n")


if __name__ == "__main__":
    main()
