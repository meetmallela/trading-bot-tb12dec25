"""
order_placer_premium.py
Order placer specifically for premium Claude agent signals
"""

import sqlite3
import json
import logging
import time
import argparse
from datetime import datetime
from kiteconnect import KiteConnect

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class PremiumOrderPlacer:
    """Order placer for premium channel signals"""
    
    def __init__(self, kite, test_mode=False):
        self.kite = kite
        self.test_mode = test_mode
        self.db_path = 'premium_signals.db'
        
        logging.info(f"[INIT] Premium Order Placer ({'TEST' if test_mode else 'LIVE'} mode)")
    
    def process_pending_signals(self):
        """Process all pending signals from database"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get unprocessed signals
        cursor.execute("SELECT * FROM signals WHERE processed = 0 AND parser_type = 'CLAUDE_AGENT'")
        signals = cursor.fetchall()
        
        if not signals:
            logging.info("[STATUS] No pending signals")
        else:
            logging.info(f"[STATUS] Processing {len(signals)} pending signal(s)")
        
        for sig in signals:
            try:
                logging.info("")
                logging.info("="*60)
                logging.info(f"[SIGNAL {sig['id']}] {sig['channel_name']}")
                
                # Parse signal data
                data = json.loads(sig['parsed_data'])
                
                # Show signal details
                logging.info(f"   Symbol: {data.get('symbol')} {data.get('strike')} {data.get('option_type')}")
                logging.info(f"   Action: {data.get('action')} | Entry: {data.get('entry_price')} | SL: {data.get('stop_loss')}")
                logging.info(f"   Tradingsymbol: {data.get('tradingsymbol')}")
                logging.info(f"   Exchange: {data.get('exchange')}")
                logging.info(f"   Quantity: {data.get('quantity')}")
                
                # Validate required fields
                required = ['tradingsymbol', 'exchange', 'action', 'quantity']
                missing = [f for f in required if not data.get(f)]
                
                if missing:
                    logging.error(f"[ERROR] Missing fields: {missing}")
                    cursor.execute("UPDATE signals SET processed = -1 WHERE id = ?", (sig['id'],))
                    conn.commit()
                    continue
                
                # Place order
                if not self.test_mode:
                    logging.info("[EXECUTE] Placing order...")
                    
                    try:
                        order_id = self.kite.place_order(
                            variety=self.kite.VARIETY_REGULAR,
                            exchange=data['exchange'],
                            tradingsymbol=data['tradingsymbol'],
                            transaction_type=self.kite.TRANSACTION_TYPE_BUY if data['action'] == 'BUY' else self.kite.TRANSACTION_TYPE_SELL,
                            quantity=int(data['quantity']),
                            product=self.kite.PRODUCT_NRML,
                            order_type=self.kite.ORDER_TYPE_MARKET
                        )
                        logging.info(f"[SUCCESS] Order ID: {order_id}")
                        
                        # Mark as processed
                        cursor.execute("UPDATE signals SET processed = 1 WHERE id = ?", (sig['id'],))
                        conn.commit()
                        
                    except Exception as e:
                        logging.error(f"[FAILED] Order placement error: {e}")
                        # Leave unprocessed for retry
                        continue
                        
                else:
                    # Test mode
                    logging.info("[TEST MODE] Order would be placed:")
                    logging.info(f"   {data['action']} {data['quantity']} {data['tradingsymbol']} @ {data['exchange']}")
                    
                    # Mark as processed in test mode
                    cursor.execute("UPDATE signals SET processed = 1 WHERE id = ?", (sig['id'],))
                    conn.commit()
                
            except Exception as e:
                logging.error(f"[ERROR] Signal {sig['id']}: {e}")
                import traceback
                traceback.print_exc()
        
        conn.close()

def initialize_kite(config):
    """Initialize Kite Connect"""
    try:
        kite = KiteConnect(api_key=config['api_key'])
        kite.set_access_token(config['access_token'])
        
        # Test connection
        profile = kite.profile()
        logging.info(f"[OK] Connected to Kite as {profile['user_name']}")
        
        return kite
    except Exception as e:
        logging.error(f"[ERROR] Kite connection failed: {e}")
        exit(1)

def main():
    parser = argparse.ArgumentParser(description='Premium Channel Order Placer')
    parser.add_argument('--test', action='store_true', help='Test mode (no real orders)')
    parser.add_argument('--continuous', action='store_true', help='Run continuously')
    parser.add_argument('--interval', type=int, default=5, help='Check interval (seconds)')
    args = parser.parse_args()
    
    # Load Kite config
    try:
        with open('kite_config.json', 'r') as f:
            config = json.load(f)
    except Exception as e:
        logging.error(f"[ERROR] Failed to load kite_config.json: {e}")
        return
    
    # Initialize Kite
    kite = initialize_kite(config)
    
    # Initialize order placer
    placer = PremiumOrderPlacer(kite, test_mode=args.test)
    
    logging.info("")
    logging.info("="*60)
    logging.info(f"[START] Premium Order Placer Active")
    logging.info(f"Mode: {'TEST' if args.test else 'LIVE'}")
    logging.info(f"Database: premium_signals.db")
    if args.continuous:
        logging.info(f"Interval: {args.interval} seconds")
    logging.info("="*60)
    logging.info("")
    
    # Main loop
    try:
        while True:
            placer.process_pending_signals()
            
            if not args.continuous:
                break
            
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        logging.info("\n[STOP] Shutting down...")

if __name__ == '__main__':
    main()
