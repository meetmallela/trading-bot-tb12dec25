"""
sl_monitor_with_trailing_ATR.py - Professional Grade SL Monitor with ATR

Features:
- MCX hours support (9 AM - 11:55 PM)
- NSE hours support (9:15 AM - 3:30 PM)
- ATR-based initial SL (wider stops on volatile stocks)
- Fixed 5% minimum (safety floor)
- 3% trailing after +5% profit
- Breakeven at +3% profit
- Re-entry prevention
- Works for ALL positions

PRO Features (merged from v3_PRO):
- Telegram notifications for critical alerts
- State reconciliation (detect unprotected positions)
- Forced auto-exit at end of session
- Exponential backoff on errors
- Graceful shutdown handling
"""

import json
import logging
import time
import argparse
import pytz
import os
import signal
import sys
import requests
import pandas as pd
from datetime import datetime, date, timedelta
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

# ========================================
# TELEGRAM NOTIFICATIONS (PRO Feature)
# ========================================
TELEGRAM_TOKEN = None
TELEGRAM_CHAT_ID = None

def load_telegram_config():
    """Load Telegram credentials from kite_config.json or environment"""
    global TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
    try:
        # Try kite_config.json first
        if os.path.exists('kite_config.json'):
            with open('kite_config.json', 'r') as f:
                config = json.load(f)
                TELEGRAM_TOKEN = config.get('telegram_token') or config.get('TELEGRAM_TOKEN')
                TELEGRAM_CHAT_ID = config.get('telegram_chat_id') or config.get('TELEGRAM_CHAT_ID')

        # Fallback to environment variables
        if not TELEGRAM_TOKEN:
            TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
        if not TELEGRAM_CHAT_ID:
            TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

        if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
            logging.info("[INIT] Telegram notifications enabled")
            return True
        else:
            logging.warning("[INIT] Telegram not configured - alerts will be log-only")
            return False
    except Exception as e:
        logging.warning(f"[INIT] Could not load Telegram config: {e}")
        return False

def send_telegram(message):
    """Send real-time mobile alerts via Telegram bot"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'text': message}, timeout=5)
        return True
    except Exception as e:
        logging.error(f"[TELEGRAM] Failed to send: {e}")
        return False

# Load Telegram config at startup
load_telegram_config()

# ========================================
# GRACEFUL SHUTDOWN (PRO Feature)
# ========================================
_shutdown_requested = False

def request_shutdown(signum=None, frame=None):
    """Handle shutdown signals gracefully"""
    global _shutdown_requested
    if _shutdown_requested:
        logging.warning("[SHUTDOWN] Force quit...")
        sys.exit(1)
    _shutdown_requested = True
    sig_name = signal.Signals(signum).name if signum else "UNKNOWN"
    logging.info(f"\n[SHUTDOWN] Received {sig_name}, shutting down gracefully...")
    send_telegram("🛑 SL Monitor shutting down gracefully")

# Register signal handlers
if sys.platform != 'win32':
    signal.signal(signal.SIGTERM, request_shutdown)
signal.signal(signal.SIGINT, request_shutdown)

def is_market_open():
    """
    Check if market is open for trading
    NSE/NFO: 9:15 AM to 3:30 PM IST (Mon-Fri)
    MCX: 9:00 AM to 11:55 PM IST (Mon-Fri)
    """
    try:
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        
        # Check if weekend
        if now.weekday() >= 5:  # Saturday=5, Sunday=6
            return False
        
        # NSE/NFO hours
        nse_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        nse_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
        
        # MCX hours
        mcx_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
        mcx_close = now.replace(hour=23, minute=55, second=0, microsecond=0)
        
        nse_is_open = nse_open <= now <= nse_close
        mcx_is_open = mcx_open <= now <= mcx_close

        return nse_is_open or mcx_is_open
    except (ValueError, TypeError, AttributeError) as e:
        logging.debug(f"Market hours check error: {e}")
        return True  # Safe default - assume market is open

class EnhancedSLMonitor:
    def __init__(self, check_interval=30, test_mode=False, use_atr=True, atr_multiplier=1.5):
        self.check_interval = check_interval
        self.test_mode = test_mode
        self.use_atr = use_atr  # Toggle ATR feature
        self.atr_multiplier = atr_multiplier  # Configurable ATR multiplier (1.5, 2.0, etc.)
        
        # Load Kite config
        with open('kite_config.json', 'r') as f:
            self.config = json.load(f)
        self.kite = KiteConnect(api_key=self.config['api_key'])
        self.kite.set_access_token(self.config['access_token'])
        
        # Load SL config (ATR settings, commodity settings, etc.)
        self._load_sl_config()
        
        self.protected_positions = set()
        self.initial_sl_prices = {}
        self.sl_placement_time = {}  # {tradingsymbol: timestamp}
        self.sl_exits_today = {}     # {tradingsymbol: date_iso}
        self.atr_cache = {}          # {tradingsymbol: atr_value}
        self.exited_today = set()    # Positions force-exited at EOD
        self.last_sync = 0           # Last state reconciliation time
        self.ist = pytz.timezone('Asia/Kolkata')

        # PRO Settings (merged from v3_PRO)
        self.pro_rules = {
            "equity_exit": "15:25",     # Force exit NSE/NFO positions
            "commodity_exit": "23:25",  # Force exit MCX positions
            "sync_interval": 300,       # State reconciliation every 5 minutes
            "commodities": ["CRUDEOIL", "NATURALGAS", "GOLD", "SILVER", "COPPER", "ZINC", "NICKEL", "LEAD", "ALUMINIUM"]
        }

        self._load_sl_exits()
        
        logging.info(f"[INIT] SL Monitor started (Mode: {'TEST' if test_mode else 'LIVE'})")
        logging.info(f"[INIT] Market hours: NSE 9:15-15:30, MCX 9:00-23:55")
        logging.info(f"[INIT] ATR Enhancement: {'ENABLED' if use_atr else 'DISABLED'}")
        if use_atr:
            logging.info(f"[INIT] ATR Multiplier: {self.atr_multiplier}x")
            logging.info(f"[INIT] Commodities SL: {self.sl_config['commodity_sl_pct']}%")
            logging.info(f"[INIT] Stocks/Indices SL: {self.sl_config['default_sl_pct']}%")
        logging.info(f"[INIT] Works for ALL positions (manual + automatic)")
        logging.info(f"[INIT] PRO Features: Telegram alerts, State reconciliation, Forced exit, Exponential backoff")
        logging.info(f"[INIT] Forced exit times: Equity {self.pro_rules['equity_exit']}, MCX {self.pro_rules['commodity_exit']}")

    def _load_sl_config(self):
        """Load SL configuration from file or use defaults"""
        default_config = {
            "default_sl_pct": 5.0,      # 5% for stocks/indices
            "commodity_sl_pct": 4.0,    # 4% for commodities (avoid circuit limits)
            "atr_multiplier": self.atr_multiplier,  # ATR multiplier (1.5x default)
            "atr_period": 14,            # ATR calculation period
            "atr_candle_interval": "5minute",  # Candle interval for ATR
            "atr_lookback_hours": 2,     # Hours of historical data
            "atr_cache_minutes": 5,      # Cache ATR for N minutes
            "commodities": [             # List of commodity keywords
                "CRUDE", "CRUDEOIL", "GOLD", "SILVER", "COPPER", 
                "ZINC", "NICKEL", "LEAD", "ALUMINIUM", "NATURALGAS"
            ]
        }
        
        try:
            if os.path.exists('sl_config.json'):
                with open('sl_config.json', 'r') as f:
                    user_config = json.load(f)
                    # Merge with defaults (user config overrides)
                    self.sl_config = {**default_config, **user_config}
                    # Update ATR multiplier from user config
                    if 'atr_multiplier' in user_config:
                        self.atr_multiplier = user_config['atr_multiplier']
                    logging.info(f"[INIT] Loaded custom SL config from sl_config.json")
            else:
                self.sl_config = default_config
                # Save default config for user to edit
                with open('sl_config.json', 'w') as f:
                    json.dump(default_config, f, indent=2)
                logging.info(f"[INIT] Created default sl_config.json - customize as needed!")
        except Exception as e:
            logging.warning(f"[INIT] Could not load sl_config.json: {e}, using defaults")
            self.sl_config = default_config

    def _load_sl_exits(self):
        """Load SL exits from file to persist across restarts"""
        try:
            if os.path.exists('sl_exits.json'):
                with open('sl_exits.json', 'r') as f:
                    data = json.load(f)
                    today = date.today().isoformat()
                    # Only keep today's exits
                    self.sl_exits_today = {k: v for k, v in data.items() if v == today}
                    
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
        """Record hit SL - block re-entry for rest of day"""
        today = date.today().isoformat()
        self.sl_exits_today[tradingsymbol] = today
        self._save_sl_exits()
        logging.warning(f"[BLACKLIST] {tradingsymbol} hit SL - NO RE-ENTRY allowed today")

    def is_blocked_from_reentry(self, tradingsymbol):
        """Check if instrument is blocked from re-entry"""
        if tradingsymbol in self.sl_exits_today:
            return self.sl_exits_today[tradingsymbol] == date.today().isoformat()
        return False

    # ========================================
    # PRO FEATURES: State Reconciliation & Forced Exit
    # ========================================

    def get_ist_now(self):
        """Get current time in IST"""
        return datetime.now(self.ist)

    def is_forced_exit_time(self, symbol):
        """Check if segment-specific square-off time has passed"""
        now = self.get_ist_now()
        is_mcx = any(c in symbol.upper() for c in self.pro_rules['commodities'])
        target = self.pro_rules['commodity_exit'] if is_mcx else self.pro_rules['equity_exit']
        h, m = map(int, target.split(':'))
        return now >= now.replace(hour=h, minute=m, second=0)

    def reconcile_state(self):
        """
        STATE RECONCILIATION: Cross-check Broker Orders vs Positions
        Finds positions without SL orders and places backup SLs
        """
        try:
            positions = self.kite.positions()['net']
            orders = self.kite.orders()

            # Find symbols with pending SL orders
            active_sl_symbols = [
                o['tradingsymbol'] for o in orders
                if o['status'] in ['TRIGGER PENDING', 'OPEN'] and o['order_type'] in ['SL', 'SL-M']
            ]

            unprotected_count = 0
            for pos in [p for p in positions if p['quantity'] != 0]:
                symbol = pos['tradingsymbol']
                if symbol not in active_sl_symbols:
                    unprotected_count += 1
                    logging.warning(f"[RECONCILE] {symbol} is UNPROTECTED. Placing backup SL.")
                    send_telegram(f"⚠️ Security Alert: {symbol} had no SL order. Placing one now.")
                    self.place_sl_order(pos)
                    self.protected_positions.add(symbol)

            if unprotected_count == 0:
                logging.info(f"[RECONCILE] All {len([p for p in positions if p['quantity'] != 0])} positions are protected ✓")

            self.last_sync = time.time()
        except Exception as e:
            logging.error(f"[SYNC ERROR] Could not reconcile state: {e}")

    def execute_square_off(self, pos):
        """
        Forced Market Exit: Cancel pending SLs and exit at market
        Used for mandatory end-of-session square-off
        """
        symbol = pos['tradingsymbol']
        try:
            # 1. Cancel all pending SLs for this symbol
            orders = self.kite.orders()
            for o in orders:
                if o['tradingsymbol'] == symbol and o['status'] == 'TRIGGER PENDING':
                    try:
                        self.kite.cancel_order(variety=self.kite.VARIETY_REGULAR, order_id=o['order_id'])
                        logging.info(f"[CANCEL] Cancelled SL order {o['order_id']} for {symbol}")
                    except Exception as cancel_err:
                        logging.warning(f"[CANCEL] Could not cancel {o['order_id']}: {cancel_err}")

            # 2. Market Exit
            product_type = pos.get('product', 'MIS')
            if product_type == 'NRML':
                kite_product = self.kite.PRODUCT_NRML
            elif product_type == 'MIS':
                kite_product = self.kite.PRODUCT_MIS
            else:
                kite_product = self.kite.PRODUCT_MIS

            if not self.test_mode:
                order_id = self.kite.place_order(
                    variety=self.kite.VARIETY_REGULAR,
                    exchange=pos['exchange'],
                    tradingsymbol=symbol,
                    transaction_type=self.kite.TRANSACTION_TYPE_SELL if pos['quantity'] > 0 else self.kite.TRANSACTION_TYPE_BUY,
                    quantity=abs(pos['quantity']),
                    product=kite_product,
                    order_type=self.kite.ORDER_TYPE_MARKET
                )
                logging.warning(f"[FORCED EXIT] {symbol} closed at market | Order: {order_id}")
                send_telegram(f"🛑 MANDATORY EXIT: Closed {symbol} at end-of-session")
            else:
                logging.info(f"[TEST] Would force exit {symbol} at market")

        except Exception as e:
            error_msg = f"❌ ERROR: Failed to force-close {symbol}: {e}"
            logging.error(error_msg)
            send_telegram(error_msg)

    def get_atr(self, instrument_token, tradingsymbol):
        """
        Calculate ATR using configurable settings
        Default: 5-minute candles over 2 hours, 14-period ATR
        """
        try:
            # Check cache first (configurable cache duration)
            cache_seconds = self.sl_config['atr_cache_minutes'] * 60
            if tradingsymbol in self.atr_cache:
                cached_time, cached_atr = self.atr_cache[tradingsymbol]
                if (time.time() - cached_time) < cache_seconds:
                    return cached_atr
            
            # Fetch historical data based on config
            to_date = datetime.now()
            from_date = to_date - timedelta(hours=self.sl_config['atr_lookback_hours'])
            
            data = self.kite.historical_data(
                instrument_token, 
                from_date, 
                to_date, 
                self.sl_config['atr_candle_interval']
            )
            
            atr_period = self.sl_config['atr_period']
            if len(data) < atr_period:
                logging.debug(f"[ATR] Insufficient data for {tradingsymbol} ({len(data)} candles, need {atr_period})")
                return None
            
            # Calculate True Range
            df = pd.DataFrame(data)
            df['h-l'] = df['high'] - df['low']
            df['h-pc'] = abs(df['high'] - df['close'].shift(1))
            df['l-pc'] = abs(df['low'] - df['close'].shift(1))
            df['tr'] = df[['h-l', 'h-pc', 'l-pc']].max(axis=1)
            
            # N-period ATR (configurable)
            atr = df['tr'].tail(atr_period).mean()
            
            # Cache it
            self.atr_cache[tradingsymbol] = (time.time(), atr)
            
            logging.debug(f"[ATR] {tradingsymbol} = {atr:.2f} ({atr_period}-period on {self.sl_config['atr_candle_interval']})")
            return atr
            
        except Exception as e:
            logging.debug(f"[ATR] Failed for {tradingsymbol}: {e}")
            return None

    def calculate_initial_sl(self, position):
        """
        Calculate initial SL using ATR + Fixed minimum
        Returns: (sl_price, method_used)
        
        Logic:
        1. Determine if commodity (uses different % SL)
        2. Calculate fixed % SL (baseline - 4% for commodities, 5% for others)
        3. If ATR enabled: Calculate ATR-based SL
        4. Use whichever is WIDER (more room to breathe)
        """
        tradingsymbol = position['tradingsymbol']
        avg_price = abs(position['average_price'])
        is_long = position['quantity'] > 0
        
        # Check if commodity
        is_commodity = any(keyword in tradingsymbol.upper() for keyword in self.sl_config['commodities'])
        
        # Fixed % SL (baseline)
        sl_pct = self.sl_config['commodity_sl_pct'] if is_commodity else self.sl_config['default_sl_pct']
        sl_factor = (100 - sl_pct) / 100 if is_long else (100 + sl_pct) / 100
        fixed_sl = avg_price * sl_factor
        
        # Try ATR if enabled
        if self.use_atr:
            try:
                atr = self.get_atr(position['instrument_token'], tradingsymbol)
                
                if atr and atr > 0:
                    # Calculate ATR-based SL (configurable multiplier)
                    atr_distance = atr * self.atr_multiplier
                    
                    if is_long:
                        atr_sl = avg_price - atr_distance
                    else:
                        atr_sl = avg_price + atr_distance
                    
                    # Use ATR if it's WIDER than fixed %
                    # (gives more room on volatile stocks)
                    if is_long:
                        if atr_sl < fixed_sl:  # ATR is wider (lower SL)
                            sl_pct_actual = ((avg_price - atr_sl) / avg_price) * 100
                            logging.info(f"   [ATR] Using ATR-based SL: {atr_sl:.2f} ({sl_pct_actual:.1f}% = {self.atr_multiplier}x ATR vs {sl_pct}% fixed)")
                            return (atr_sl, f"ATR_{sl_pct_actual:.1f}%")
                    else:
                        if atr_sl > fixed_sl:  # ATR is wider (higher SL)
                            sl_pct_actual = ((atr_sl - avg_price) / avg_price) * 100
                            logging.info(f"   [ATR] Using ATR-based SL: {atr_sl:.2f} ({sl_pct_actual:.1f}% = {self.atr_multiplier}x ATR vs {sl_pct}% fixed)")
                            return (atr_sl, f"ATR_{sl_pct_actual:.1f}%")
            except Exception as e:
                logging.debug(f"[ATR] Failed, falling back to {sl_pct}%: {e}")
        
        # Fallback to fixed % (4% for commodities, 5% for others)
        commodity_label = f"COMMODITY_{sl_pct}%" if is_commodity else f"FIXED_{sl_pct}%"
        return (fixed_sl, commodity_label)

    def calculate_trailing_sl(self, position, current_price, current_sl_trigger):
        """
        BALANCED SL SYSTEM:
        - Initial: ATR-based or 5% (whichever is wider)
        - Breakeven: At +3% profit
        - Trailing: 3% below LTP
        """
        tradingsymbol = position['tradingsymbol']
        quantity = position['quantity']
        avg_price = abs(position['average_price'])
        is_long = quantity > 0
        
        # Calculate percentage from entry
        if is_long:
            pnl_percent = ((current_price - avg_price) / avg_price) * 100
        else:
            pnl_percent = ((avg_price - current_price) / avg_price) * 100
        
        # Store initial SL (ATR or 5%)
        if tradingsymbol not in self.initial_sl_prices:
            initial_sl, method = self.calculate_initial_sl(position)
            self.initial_sl_prices[tradingsymbol] = initial_sl
            logging.info(f"   [INIT SL] {tradingsymbol}: ₹{initial_sl:.2f} ({method})")
        
        initial_sl = self.initial_sl_prices[tradingsymbol]
        
        # Rule 1: Below +3% profit -> Keep initial SL
        if pnl_percent < 3:
            return (initial_sl, "INITIAL")
        
        # Rule 2: At +3% to +5% profit -> Move to breakeven
        if 3 <= pnl_percent < 5:
            new_sl = round(avg_price / 0.05) * 0.05
            if (is_long and new_sl > current_sl_trigger) or (not is_long and new_sl < current_sl_trigger):
                return (new_sl, "BREAKEVEN")
            return (current_sl_trigger, "NO_CHANGE")
        
        # Rule 3: Above +5% profit -> Trail 3% below LTP
        if pnl_percent >= 5:
            calculated_sl = current_price * 0.97 if is_long else current_price * 1.03
            calculated_sl = round(calculated_sl / 0.05) * 0.05
            
            # Floor at entry price (breakeven)
            if (is_long and calculated_sl < avg_price) or (not is_long and calculated_sl > avg_price):
                calculated_sl = round(avg_price / 0.05) * 0.05
            
            should_trail = (is_long and calculated_sl > current_sl_trigger) or (not is_long and calculated_sl < current_sl_trigger)
            
            if should_trail:
                # 30-second transition grace period
                if tradingsymbol in self.sl_placement_time:
                    if (time.time() - self.sl_placement_time[tradingsymbol]) <= 30:
                        return (calculated_sl, "TRANSITION")
                return (calculated_sl, "TRAILING")
        
        return (current_sl_trigger, "NO_CHANGE")

    def place_sl_order(self, position):
        """Place stop-loss order (BROKER SL - not software!)"""
        try:
            tradingsymbol = position['tradingsymbol']
            quantity = abs(position['quantity'])
            avg_price = abs(position['average_price'])
            exchange = position['exchange']
            product_type = position.get('product', 'MIS')
            is_long = position['quantity'] > 0
            transaction_type = self.kite.TRANSACTION_TYPE_SELL if is_long else self.kite.TRANSACTION_TYPE_BUY
            
            # Calculate initial SL (ATR or 5%)
            sl_price, method = self.calculate_initial_sl(position)
            sl_price = round(sl_price / 0.05) * 0.05
            
            # Set limit price slightly worse than trigger for execution
            limit_price = round((sl_price - 1) if is_long else (sl_price + 1), 2)
            
            # Convert product type to Kite constant
            if product_type == 'NRML':
                kite_product = self.kite.PRODUCT_NRML
            elif product_type == 'MIS':
                kite_product = self.kite.PRODUCT_MIS
            else:
                kite_product = self.kite.PRODUCT_MIS  # Fallback
            
            logging.info(f"   [PRODUCT] {product_type} | Entry: ₹{avg_price:.2f} | SL: ₹{sl_price:.2f} ({method})")

            if not self.test_mode:
                order_id = self.kite.place_order(
                    variety=self.kite.VARIETY_REGULAR,
                    exchange=exchange,
                    tradingsymbol=tradingsymbol,
                    transaction_type=transaction_type,
                    quantity=quantity,
                    product=kite_product,
                    order_type=self.kite.ORDER_TYPE_SL,
                    trigger_price=sl_price,
                    price=limit_price
                )
                self.sl_placement_time[tradingsymbol] = time.time()
                logging.info(f"   [✓ PLACED] SL order | Order ID: {order_id}")
                send_telegram(f"🛡️ SL Placed: {tradingsymbol} @ ₹{sl_price:.2f} ({method})")
                return order_id
            else:
                logging.info(f"   [TEST] Would place SL at {sl_price:.2f}")
                return f"TEST_{tradingsymbol}"
        except Exception as e:
            logging.error(f"   [✗ FAILED] Could not place SL: {e}")
            return None

    def get_existing_sl_order(self, tradingsymbol):
        try:
            orders = self.kite.orders()
            return next((o for o in orders if o['tradingsymbol'] == tradingsymbol and o['order_type'] in ['SL', 'SL-M'] and o['status'] in ['TRIGGER PENDING', 'OPEN']), None)
        except Exception as e:
            logging.error(f"[ERROR] Failed to get orders: {e}")
            return None

    def modify_sl_order(self, order_id, tradingsymbol, new_sl_price, quantity, transaction_type, exchange, sl_type="TRAILING"):
        try:
            limit_price = round((new_sl_price - 1) if transaction_type == self.kite.TRANSACTION_TYPE_SELL else (new_sl_price + 1), 2)
            if not self.test_mode:
                self.kite.modify_order(
                    variety=self.kite.VARIETY_REGULAR,
                    order_id=order_id,
                    trigger_price=new_sl_price,
                    price=limit_price
                )
                self.sl_placement_time[tradingsymbol] = time.time()  # Reset grace period
                logging.info(f"   [✓ MODIFIED] Order ID: {order_id} | New SL: {new_sl_price:.2f}")
                # Telegram alert for significant SL changes (breakeven/trailing)
                if sl_type == "BREAKEVEN":
                    send_telegram(f"🟡 BREAKEVEN: {tradingsymbol} SL moved to ₹{new_sl_price:.2f}")
                elif sl_type == "TRAILING":
                    send_telegram(f"🟢 TRAILING: {tradingsymbol} SL trailed to ₹{new_sl_price:.2f}")
            else:
                logging.info(f"   [TEST] Would modify SL to {new_sl_price:.2f}")
            return True
        except Exception as e:
            logging.error(f"   [✗ FAILED] Could not modify SL: {e}")
            return False

    def monitor_open_positions(self):
        try:
            if not is_market_open():
                logging.info(f"[PAUSED] Market closed. Active: NSE 9:15-15:30, MCX 9:00-23:55")
                return
            
            positions = self.kite.positions()['net']
            open_positions = [p for p in positions if p['quantity'] != 0]
            
            if not open_positions:
                logging.info("[STATUS] No open positions.")
                return

            logging.info("---" + "#"*72 + "---")
            logging.info(f"--- MONITORING {len(open_positions)} POSITIONS ---")
            logging.info("---" + "#"*72 + "---")
            
            for pos in open_positions:
                symbol = pos['tradingsymbol']
                ltp = pos['last_price']
                pnl = pos['pnl']
                
                logging.info(f"[{symbol}] LTP: ₹{ltp:.2f} | PnL: ₹{pnl:.2f}")
                
                # Check for existing SL order
                existing_sl = self.get_existing_sl_order(symbol)
                
                if existing_sl:
                    # Calculate new trailing SL
                    new_sl, sl_type = self.calculate_trailing_sl(pos, ltp, existing_sl['trigger_price'])
                    self.log_status(pos, new_sl, sl_type)
                    
                    # Modify SL if needed
                    should_modify = False
                    if sl_type in ["TRAILING", "TRANSITION", "BREAKEVEN"]:
                        if pos['quantity'] > 0:  # LONG
                            should_modify = new_sl > existing_sl['trigger_price']
                        else:  # SHORT
                            should_modify = new_sl < existing_sl['trigger_price']
                    
                    if should_modify:
                        self.modify_sl_order(
                            existing_sl['order_id'],
                            symbol,
                            new_sl,
                            abs(pos['quantity']),
                            existing_sl['transaction_type'],
                            pos['exchange'],
                            sl_type=sl_type
                        )
                else:
                    # No SL exists - place one
                    if symbol not in self.protected_positions:
                        logging.info(f"   [NEW POSITION] No SL found - placing initial SL...")
                        if self.place_sl_order(pos):
                            self.protected_positions.add(symbol)

            # Check for exits (SL hit or manual close)
            current_symbols = {p['tradingsymbol'] for p in open_positions}
            for protected in list(self.protected_positions):
                if protected not in current_symbols:
                    logging.warning(f"[EXIT] {protected} position closed")
                    send_telegram(f"📤 Position Closed: {protected}")
                    self.record_sl_exit(protected)
                    self.protected_positions.remove(protected)
                    # Clean up tracking data
                    self.initial_sl_prices.pop(protected, None)
                    self.sl_placement_time.pop(protected, None)
                    self.atr_cache.pop(protected, None)
                    
        except Exception as e:
            logging.error(f"[ERROR] Monitor failed: {e}")

    def log_status(self, position, sl_price, sl_type):
        avg_price = abs(position['average_price'])
        ltp = position['last_price']
        pnl_pct = ((ltp - avg_price) / avg_price * 100) if position['quantity'] > 0 else ((avg_price - ltp) / avg_price * 100)
        
        status_map = {
            "INITIAL": "🔴 INITIAL",
            "BREAKEVEN": "🟡 BREAKEVEN (Risk-Free)",
            "TRAILING": f"🟢 TRAILING (3% from ₹{ltp:.2f})",
            "TRANSITION": "🟠 TRANSITION (Grace)",
            "NO_CHANGE": "⚪ NO CHANGE"
        }
        logging.info(f"   [SL] {status_map.get(sl_type, sl_type)} | PnL: {pnl_pct:+.1f}% | SL: ₹{sl_price:.2f}")

    def run(self):
        """Main monitoring loop with PRO features: exponential backoff, forced exit, reconciliation"""
        global _shutdown_requested
        retry_delay = 5

        # Startup notification
        send_telegram(f"🚀 SL Monitor Online (ATR: {'ON' if self.use_atr else 'OFF'} | Mode: {'TEST' if self.test_mode else 'LIVE'})")
        logging.info(f"[START] SL Monitor running with PRO features enabled")

        while not _shutdown_requested:
            try:
                # 1. Periodic State Reconciliation (every 5 minutes)
                if time.time() - self.last_sync > self.pro_rules['sync_interval']:
                    self.reconcile_state()

                # 2. Check for forced exit times
                positions = self.kite.positions()['net']
                open_positions = [p for p in positions if p['quantity'] != 0]

                for pos in open_positions:
                    symbol = pos['tradingsymbol']
                    # Mandatory Exit Check
                    if self.is_forced_exit_time(symbol) and symbol not in self.exited_today:
                        logging.warning(f"[FORCED EXIT] {symbol} - End of session square-off triggered")
                        self.execute_square_off(pos)
                        self.exited_today.add(symbol)

                # 3. Standard SL monitoring
                self.monitor_open_positions()

                retry_delay = 5  # Reset delay on success

            except KeyboardInterrupt:
                logging.info("\n[STOP] Shutting down SL monitor...")
                break
            except Exception as e:
                logging.error(f"[CRITICAL] Loop error: {e}. Retrying in {retry_delay}s...")
                send_telegram(f"⚠️ SL Monitor Error: {e}")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 300)  # Exponential backoff up to 5 mins
                continue

            time.sleep(self.check_interval)

        # Graceful shutdown
        logging.info("[SHUTDOWN] SL Monitor stopped")
        send_telegram("🛑 SL Monitor stopped")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='SL Monitor with ATR Enhancement')
    parser.add_argument('--interval', type=int, default=30, help='Check interval in seconds (default: 30)')
    parser.add_argument('--test', action='store_true', help='Test mode (no real orders)')
    parser.add_argument('--no-atr', action='store_true', help='Disable ATR (use fixed % only)')
    parser.add_argument('--atr-multiplier', type=float, default=1.5, 
                       help='ATR multiplier (default: 1.5). Try 2.0 for more room, 1.0 for tighter stops')
    args = parser.parse_args()
    
    EnhancedSLMonitor(
        check_interval=args.interval, 
        test_mode=args.test,
        use_atr=not args.no_atr,
        atr_multiplier=args.atr_multiplier
    ).run()
