"""
order_placer_db_enhanced.py
Entry order placement with:
- MCX expiry auto-resolution
- Symbol normalization
- Expiry validation before DB insert
"""

import sqlite3
import json
import time
import logging
import os
from datetime import datetime, timedelta
from kiteconnect import KiteConnect
import argparse
import csv

from find_instrument_from_cache import InstrumentFinder

# ==================== LOGGING ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - ORDER_PLACER - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('order_placer.log'),
        logging.StreamHandler()
    ]
)

# ==================== UTILITIES ====================

def normalize_symbol(symbol):
    """Normalize common commodity aliases"""
    if not symbol:
        return symbol

    symbol = symbol.upper().strip()

    alias_map = {
        'GAS': 'NATURALGAS',
        'NG': 'NATURALGAS',
        'SILVERMIC': 'SILVERM',
    }

    return alias_map.get(symbol, symbol)


def load_kite_config():
    config_file = 'kite_config.json'
    if not os.path.exists(config_file):
        logging.error("[ERROR] kite_config.json not found")
        return None

    with open(config_file, 'r') as f:
        cfg = json.load(f)

    if not cfg.get('api_key') or not cfg.get('access_token'):
        logging.error("[ERROR] Invalid kite_config.json")
        return None

    return cfg


# ==================== ORDER PLACER ====================

class OrderPlacerDB:
    def __init__(self, api_key=None, access_token=None, test_mode=False):
        self.test_mode = test_mode
        self.db_path = 'trading.db'
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

        if not api_key or not access_token:
            cfg = load_kite_config()
            if cfg:
                api_key = cfg['api_key']
                access_token = cfg['access_token']

        if not test_mode:
            self.kite = KiteConnect(api_key=api_key)
            self.kite.set_access_token(access_token)
            logging.info("[OK] KiteConnect initialized")
        else:
            self.kite = None
            logging.warning("[TEST MODE] Orders will not be placed")

        self.instrument_finder = InstrumentFinder('instruments_cache.csv')

        self._create_orders_table()

    def _create_orders_table(self):
        cur = self.conn.cursor()
        cur.execute("""
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
                created_at TEXT,
                updated_at TEXT
            )
        """)
        self.conn.commit()

    # ==================== CORE LOGIC ====================

    def _resolve_mcx_expiry(self, symbol, strike, option_type, expiry_date):
        """Ensure MCX expiry exists, auto-resolve from cache if missing"""

        if expiry_date:
            return expiry_date

        logging.warning(f"[EXPIRY] Missing expiry for {symbol}, resolving from cache")

        inst = self.instrument_finder.find_with_auto_expiry(
            symbol, strike, option_type
        )

        if inst:
            resolved = inst.get('expiry_date')
            logging.info(f"[EXPIRY FIX] {symbol} → {resolved}")
            return resolved

        return None

    def _validate_signal(self, signal_data):
        """Validate & normalize signal before order placement"""

        # Normalize symbol
        symbol = normalize_symbol(signal_data.get('symbol'))
        signal_data['symbol'] = symbol

        required = ['symbol', 'strike', 'option_type', 'action', 'entry_price', 'stop_loss']
        missing = [f for f in required if not signal_data.get(f)]

        if missing:
            return False, f"Missing required fields: {missing}"

        mcx_symbols = [
            'GOLD', 'GOLDM', 'SILVER', 'SILVERM',
            'CRUDEOIL', 'NATURALGAS', 'COPPER', 'ZINC', 'LEAD', 'NICKEL'
        ]

        if symbol in mcx_symbols:
            expiry = self._resolve_mcx_expiry(
                symbol,
                signal_data['strike'],
                signal_data['option_type'],
                signal_data.get('expiry_date')
            )

            if not expiry:
                return False, f"Unable to resolve expiry for MCX symbol {symbol}"

            signal_data['expiry_date'] = expiry

        return True, None

    # ==================== SYMBOL RESOLUTION ====================

    def _get_trading_symbol(self, signal_data):
        symbol = signal_data['symbol']
        strike = signal_data['strike']
        opt_type = signal_data['option_type']
        expiry = signal_data.get('expiry_date')

        inst = self.instrument_finder.find_instrument(
            symbol, strike, opt_type, expiry
        )

        if inst:
            return inst['symbol']

        logging.error(f"[ERROR] Instrument not found: {symbol} {strike} {opt_type} {expiry}")
        return None

    # ==================== MAIN FLOW ====================

    def process_pending_signals(self):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT id, parsed_data FROM signals
            WHERE processed = 0
            ORDER BY timestamp ASC
        """)

        rows = cur.fetchall()
        if not rows:
            logging.info("[DONE] No pending signals")
            return

        for row in rows:
            signal_id = row['id']
            signal_data = json.loads(row['parsed_data'])

            logging.info(f"[TASK] Processing Signal #{signal_id}")

            valid, error = self._validate_signal(signal_data)
            if not valid:
                logging.error(f"[REJECT] Signal {signal_id}: {error}")
                self._mark_processed(signal_id)
                continue

            tradingsymbol = self._get_trading_symbol(signal_data)
            if not tradingsymbol:
                self._mark_processed(signal_id)
                continue

            logging.info(f"[OK] Ready to place order: {tradingsymbol}")

            if not self.test_mode:
                self._place_entry_order(signal_id, signal_data, tradingsymbol)

            self._mark_processed(signal_id)

            time.sleep(1)

    def _place_entry_order(self, signal_id, signal_data, tradingsymbol):
        symbol = signal_data['symbol']
        action = signal_data['action']
        price = float(signal_data['entry_price']) + 0.05
        sl = float(signal_data['stop_loss'])

        mcx_symbols = [
            'GOLD', 'GOLDM', 'SILVER', 'SILVERM',
            'CRUDEOIL', 'NATURALGAS', 'COPPER'
        ]

        exchange = (
            self.kite.EXCHANGE_MCX
            if symbol in mcx_symbols
            else self.kite.EXCHANGE_NFO
        )

        qty = 1
        if 'NIFTY' in symbol:
            qty = 25
        elif 'BANKNIFTY' in symbol:
            qty = 15

        order_id = self.kite.place_order(
            variety=self.kite.VARIETY_REGULAR,
            exchange=exchange,
            tradingsymbol=tradingsymbol,
            transaction_type=action.upper(),
            quantity=qty,
            product=self.kite.PRODUCT_MIS,
            order_type=self.kite.ORDER_TYPE_LIMIT,
            price=round(price / 0.05) * 0.05,
            validity=self.kite.VALIDITY_DAY
        )

        logging.info(f"[OK] Entry order placed: {order_id}")

    def _mark_processed(self, signal_id):
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE signals SET processed = 1 WHERE id = ?",
            (signal_id,)
        )
        self.conn.commit()
    def run_continuous(self, interval=20):
        logging.info("[START] Order placer running continuously")
        try:
            while True:
                self.process_pending_signals()
                time.sleep(interval)
        except KeyboardInterrupt:
            logging.info("[STOP] Order placer stopped")


# ==================== CLI ====================

"""def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true')
    args = parser.parse_args()

    #placer = OrderPlacerDB(test_mode=args.test)
    placer.process_pending_signals()"""

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true')
    parser.add_argument('--continuous', action='store_true')
    args = parser.parse_args()

    placer = OrderPlacerDB(test_mode=args.test)

    if args.continuous:
        placer.run_continuous()
    else:
        placer.process_pending_signals()


if __name__ == "__main__":
    main()
