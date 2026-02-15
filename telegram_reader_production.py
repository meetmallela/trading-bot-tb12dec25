"""
telegram_reader_production.py - ENHANCED VERSION
WITH FUTURES SUPPORT + EXPIRY DATE DISPLAY + TIMESTAMPED LOGS
+ MULTI-MESSAGE SIGNAL COMBINING + NOISE FILTERING

Features:
- Graceful shutdown on SIGTERM/SIGINT
- Rate limiting to prevent API bans
- Explicit timezone handling (IST)
- Multi-message signal combining for split signals (any channel)
- Noise filtering for non-trading messages
"""

import asyncio
import json
import logging
import sqlite3
import signal
from datetime import datetime
from telethon import TelegramClient, events
from telethon.tl.functions.channels import GetParticipantRequest
import sys
import io

try:
    import pytz
    IST = pytz.timezone('Asia/Kolkata')
except ImportError:
    IST = None
    logging.warning("[WARN] pytz not installed - using local timezone")

# Fix Windows console encoding issues
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Generate timestamped log filename
log_start_time = datetime.now()
log_filename = f"telegram_reader_production_{log_start_time.strftime('%d%b%y_%I%M%S_%p').upper()}.log"

# Load Telegram API credentials from config file
import os

def load_telegram_config():
    """Load Telegram credentials from config file or environment variables"""
    # First try environment variables (highest priority)
    api_id = os.environ.get('TELEGRAM_API_ID')
    api_hash = os.environ.get('TELEGRAM_API_HASH')
    phone = os.environ.get('TELEGRAM_PHONE')

    if api_id and api_hash and phone:
        return int(api_id), api_hash, phone

    # Fall back to config file
    config_paths = ['telegram_config.json', 'claudeagent/telegram_config.json']
    for config_path in config_paths:
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    config = json.load(f)
                    return (
                        config['api_id'],
                        config['api_hash'],
                        config.get('phone') or config.get('phone_number')
                    )
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error reading {config_path}: {e}")
                continue

    raise RuntimeError(
        "Telegram credentials not found. Please either:\n"
        "1. Set environment variables: TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE\n"
        "2. Create telegram_config.json with: api_id, api_hash, phone_number"
    )

TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE = load_telegram_config()

# Channels to monitor (INTEGER format!)
MONITORED_CHANNELS = [
    -1002498088029,  # RJ - STUDENT PRACTICE CALLS
    -1002770917134,  # MCX PREMIUM
    -1002842854743,  # VIP RJ Paid Education Purpose
    -1003089362819,  # Paid Premium group
    -1001903138387,  # COPY MY TRADES BANKNIFTY
    -1002380215256,  # PREMIUM_GROUP
   # -1002568252699,  # TARGET HIT CLUB
    -1002201480769,  # Trader ayushi
   # -1001294857397,  # Mcx Trading King Official Group
   # -1002431924245,  # MCX JACKPOT TRADING
   # -1001389090145,  # Stockpro Online
   # -1001456128948,  # Ashish Kyal Trading Gurukul
    -1003282204738,  # JP Paper trade - Dec-25
    -1001801974768,  # New Channel 1
    -1001200390337,  # New Channel 2
]

# Import the FUTURES-ENABLED parser
try:
    from signal_parser_with_futures import SignalParserWithFutures
    PARSER_TYPE = "futures_enabled"
    FUTURES_SUPPORT = True
except ImportError:
    # Fallback to old parser if new one not found
    try:
        from signal_parser_with_claude_fallback import SignalParserWithClaudeFallback as SignalParserWithFutures
        PARSER_TYPE = "claude_fallback"
        FUTURES_SUPPORT = False
        logging.warning("[WARNING] Using old parser - futures support disabled")
    except ImportError:
        print("ERROR: Parser not found!")
        exit(1)

# Import multi-message signal combiner
try:
    from multi_message_signal_combiner import MultiMessageSignalCombiner, ChannelSpecificRules
    MULTI_MESSAGE_SUPPORT = True
except ImportError:
    MULTI_MESSAGE_SUPPORT = False

# Configure logging with timestamped filename
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - TELEGRAM - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(
            io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
            if sys.platform == 'win32' else sys.stdout
        )
    ]
)

# Log the filename being used
logging.info(f"[LOG] Writing to: {log_filename}")

if not MULTI_MESSAGE_SUPPORT:
    logging.warning("[WARNING] multi_message_signal_combiner not found - multi-message combining disabled")

# Load Claude API key
try:
    with open('claude_api_key.txt', 'r') as f:
        claude_api_key = f.read().strip()
except FileNotFoundError:
    logging.warning("[WARNING] claude_api_key.txt not found - fallback to Claude API disabled")
    claude_api_key = None
except (IOError, OSError) as e:
    logging.warning(f"[WARNING] Could not read claude_api_key.txt: {e}")
    claude_api_key = None

# Initialize Telegram client
client = TelegramClient('trading_bot', TELEGRAM_API_ID, TELEGRAM_API_HASH)

# Initialize thread-safe database
from db_utils import ThreadSafeDB
db = ThreadSafeDB('trading.db')
db.init_signals_table(include_instrument_type=True)
logging.info("[OK] Thread-safe database initialized")

# Initialize parser with futures support
parser = SignalParserWithFutures(
    claude_api_key=claude_api_key,
    rules_file='parsing_rules_enhanced_v2.json'
)

# ========================================
# CRITICAL FIX: Patch parser for correct tradingsymbol format
# ========================================
from tradingsymbol_utils import get_correct_tradingsymbol

original_parse = parser.parse

def patched_parse(message, **kwargs):
    """Wrapper that fixes tradingsymbol format after parsing"""
    result = original_parse(message, **kwargs)

    if result and 'tradingsymbol' in result:
        # Regenerate tradingsymbol with correct format for OPTIONS
        if 'strike' in result and result.get('instrument_type', 'OPTIONS') == 'OPTIONS':
            try:
                correct_ts = get_correct_tradingsymbol(
                    symbol=result['symbol'],
                    strike=result['strike'],
                    option_type=result['option_type'],
                    expiry_date=result['expiry_date']
                )
                if correct_ts != result['tradingsymbol']:
                    logging.info(f"[FIX] Corrected: {result['tradingsymbol']} -> {correct_ts}")
                    result['tradingsymbol'] = correct_ts
            except Exception as e:
                logging.warning(f"[WARN] Could not fix tradingsymbol: {e}")

    return result

parser.parse = patched_parse
logging.info("[OK] Parser patched with correct tradingsymbol format (NIFTY/SENSEX weekly fix applied)")


if FUTURES_SUPPORT:
    logging.info(f"[OK] Using SignalParserWithFutures (OPTIONS + FUTURES support)")
else:
    logging.info(f"[OK] Using legacy parser (OPTIONS only)")

# ========================================
# MULTI-MESSAGE SIGNAL COMBINER SETUP
# ========================================
signal_combiner = None

if MULTI_MESSAGE_SUPPORT:
    signal_combiner = MultiMessageSignalCombiner(
        parser=parser,
        combination_window_seconds=30,
        max_messages_to_combine=5,
    )
    logging.info("[OK] Multi-message signal combiner initialized (window=30s, max=5)")

    # ---- Channel-specific rules ----

    # Active channels - single-message only (send complete signals)
    signal_combiner.add_channel_rules("-1002498088029", ChannelSpecificRules(
        channel_name="RJ - STUDENT PRACTICE CALLS",
        always_single_message=True,
    ))
    signal_combiner.add_channel_rules("-1002770917134", ChannelSpecificRules(
        channel_name="MCX PREMIUM",
        always_single_message=True,
    ))
    signal_combiner.add_channel_rules("-1002842854743", ChannelSpecificRules(
        channel_name="VIP RJ Paid Education Purpose",
        always_single_message=True,
    ))
    signal_combiner.add_channel_rules("-1003089362819", ChannelSpecificRules(
        channel_name="Paid Premium group",
        always_single_message=True,
    ))
    signal_combiner.add_channel_rules("-1001903138387", ChannelSpecificRules(
        channel_name="COPY MY TRADES BANKNIFTY",
        always_single_message=True,
    ))
    signal_combiner.add_channel_rules("-1002380215256", ChannelSpecificRules(
        channel_name="PREMIUM_GROUP",
        always_single_message=True,
    ))
    signal_combiner.add_channel_rules("-1002201480769", ChannelSpecificRules(
        channel_name="Trader ayushi",
        always_single_message=True,
    ))
    signal_combiner.add_channel_rules("-1001801974768", ChannelSpecificRules(
        channel_name="New Channel 1",
        always_single_message=True,
    ))

    # JP Paper trade - single-message only (has dedicated parser)
    signal_combiner.add_channel_rules("-1003282204738", ChannelSpecificRules(
        channel_name="JP Paper trade - Dec-25",
        always_single_message=True,
    ))

    # New Channel 2 - multi-message combining enabled (splits signals across messages)
    signal_combiner.add_channel_rules("-1001200390337", ChannelSpecificRules(
        channel_name="New Channel 2",
        combination_window_seconds=30,
        max_messages_to_combine=5,
    ))

    # Noisy channels (currently disabled, rules ready for when re-enabled)
    signal_combiner.add_channel_rules("-1001456128948", ChannelSpecificRules(
        channel_name="Ashish Kyal Trading Gurukul",
        always_single_message=True,
        noise_patterns=[
            r'(?i)^(dear\s+(students?|members?|traders?)|class\s+|session\s+|lecture)',
            r'(?i)(gurukul|workshop|webinar|seminar|course|enroll)',
        ],
        min_message_length=10,
    ))
    signal_combiner.add_channel_rules("-1001389090145", ChannelSpecificRules(
        channel_name="Stockpro Online",
        always_single_message=True,
        noise_patterns=[
            r'(?i)(visit\s+(our|www)|stockpro|free\s+trial|accuracy\s+\d+%)',
            r'(?i)^(follow\s+us|share\s+with|forward\s+this)',
        ],
        min_message_length=10,
    ))
    signal_combiner.add_channel_rules("-1002431924245", ChannelSpecificRules(
        channel_name="MCX JACKPOT TRADING",
        always_single_message=True,
        noise_patterns=[
            r'(?i)(jackpot|bumper\s+profit|100%\s+sure|guaranteed)',
            r'(?i)^(profit\s+booking|booked\s+profit|see\s+our\s+result)',
        ],
        min_message_length=10,
    ))
    signal_combiner.add_channel_rules("-1001294857397", ChannelSpecificRules(
        channel_name="Mcx Trading King Official Group",
        always_single_message=True,
        noise_patterns=[
            r'(?i)(trading\s+king|king\s+of\s+mcx|join\s+paid|premium\s+member)',
            r'(?i)^(profit\s+earned|today.s\s+result|check\s+screenshot)',
        ],
        min_message_length=10,
    ))

    logging.info("[OK] Channel rules configured for all channels")
else:
    logging.info("[INFO] Multi-message combining disabled (module not found)")

# Statistics
stats = {
    'total_messages': 0,
    'parsed_signals': 0,
    'stored_signals': 0,
    'parsing_failures': 0,
    'options_signals': 0,
    'futures_signals': 0,
    'noise_filtered': 0,
    'combined_signals': 0,
}

# ========================================
# RATE LIMITING
# ========================================
import time
from collections import deque

class RateLimiter:
    """Simple rate limiter to prevent API bans"""
    def __init__(self, max_calls=30, window_seconds=60):
        self.max_calls = max_calls
        self.window_seconds = window_seconds
        self.calls = deque()

    def acquire(self):
        """Wait if rate limit exceeded, return True when ready"""
        now = time.time()
        # Remove calls outside the window
        while self.calls and self.calls[0] < now - self.window_seconds:
            self.calls.popleft()

        if len(self.calls) >= self.max_calls:
            # Wait until oldest call expires
            sleep_time = self.calls[0] + self.window_seconds - now
            if sleep_time > 0:
                logging.warning(f"[RATE LIMIT] Waiting {sleep_time:.1f}s...")
                time.sleep(sleep_time)

        self.calls.append(time.time())
        return True

# Rate limiter: max 30 messages per minute
rate_limiter = RateLimiter(max_calls=30, window_seconds=60)

# ========================================
# GRACEFUL SHUTDOWN
# ========================================
shutdown_event = asyncio.Event() if hasattr(asyncio, 'Event') else None
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

    # Flush any pending multi-message buffers before shutting down
    if signal_combiner:
        logging.info("[SHUTDOWN] Flushing multi-message buffers...")
        signal_combiner.flush_all()

    print_stats()

    # Try to set asyncio event if available
    try:
        if shutdown_event and not shutdown_event.is_set():
            shutdown_event.set()
    except Exception:
        pass

# Register signal handlers
if sys.platform != 'win32':
    signal.signal(signal.SIGTERM, request_shutdown)
signal.signal(signal.SIGINT, request_shutdown)

# ========================================
# TIMEZONE UTILITIES
# ========================================
def get_ist_now():
    """Get current time in IST timezone"""
    if IST:
        return datetime.now(IST)
    return datetime.now()

def format_ist_timestamp(dt=None):
    """Format datetime as IST timestamp string"""
    if dt is None:
        dt = get_ist_now()
    if IST and dt.tzinfo is None:
        dt = IST.localize(dt)
    return dt.strftime('%Y-%m-%d %H:%M:%S IST')


def get_expiry_dates_from_csv():
    """
    Extract and display expiry dates for major indices from CSV
    Returns dict of {symbol: [expiry_dates]}
    """
    try:
        import pandas as pd
        from datetime import datetime

        # Try to load CSV or Parquet
        try:
            df = pd.read_parquet('valid_instruments.parquet')
            logging.info("[EXPIRY] Loaded valid_instruments.parquet")
        except (FileNotFoundError, IOError, Exception) as parquet_err:
            try:
                df = pd.read_csv('valid_instruments.csv')
                logging.info("[EXPIRY] Loaded valid_instruments.csv")
            except (FileNotFoundError, IOError, pd.errors.EmptyDataError) as csv_err:
                logging.warning(f"[EXPIRY] Could not load instruments file: {csv_err}")
                return None

        # Get current month
        current_month = datetime.now().month
        current_year = datetime.now().year

        # Symbols to check
        symbols_to_check = ['NIFTY', 'SENSEX', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']

        expiry_info = {}

        for symbol in symbols_to_check:
            # Find instruments for this symbol
            symbol_instruments = df[df['symbol'].str.contains(symbol, case=False, na=False)].copy()

            if len(symbol_instruments) > 0:
                # Convert expiry_date to datetime
                symbol_instruments['expiry_dt'] = pd.to_datetime(symbol_instruments['expiry_date'])

                # Filter for current month
                current_month_expiries = symbol_instruments[
                    (symbol_instruments['expiry_dt'].dt.month == current_month) &
                    (symbol_instruments['expiry_dt'].dt.year == current_year)
                ]

                # Get unique expiry dates
                unique_expiries = sorted(current_month_expiries['expiry_dt'].unique())

                if len(unique_expiries) > 0:
                    # Convert to string dates
                    expiry_dates = [dt.strftime('%Y-%m-%d (%A)') for dt in unique_expiries]
                    expiry_info[symbol] = expiry_dates

        return expiry_info

    except Exception as e:
        logging.error(f"[EXPIRY] Error extracting expiry dates: {e}")
        import traceback
        traceback.print_exc()
        return None


def display_expiry_info():
    """Display expiry dates for major indices"""
    logging.info("")
    logging.info("="*80)
    logging.info("CURRENT MONTH EXPIRY DATES")
    logging.info("="*80)

    expiry_info = get_expiry_dates_from_csv()

    if expiry_info:
        current_month_name = datetime.now().strftime('%B %Y')
        logging.info(f"Month: {current_month_name}")
        logging.info("")

        # Display NIFTY and SENSEX first
        for symbol in ['NIFTY', 'SENSEX']:
            if symbol in expiry_info:
                logging.info(f"{symbol}:")
                for expiry in expiry_info[symbol]:
                    logging.info(f"  > {expiry}")
                logging.info("")

        # Display BANKNIFTY, FINNIFTY, MIDCPNIFTY
        for symbol in ['BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']:
            if symbol in expiry_info:
                logging.info(f"{symbol}:")
                for expiry in expiry_info[symbol]:
                    logging.info(f"  > {expiry}")
                logging.info("")

        # Display any other symbols found
        other_symbols = [s for s in expiry_info.keys()
                        if s not in ['NIFTY', 'SENSEX', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']]
        for symbol in other_symbols:
            logging.info(f"{symbol}:")
            for expiry in expiry_info[symbol]:
                logging.info(f"  > {expiry}")
            logging.info("")
    else:
        logging.warning("Could not load expiry information from CSV/Parquet")

    logging.info("="*80)
    logging.info("")


def insert_signal(channel_id, channel_name, message_id, raw_text, parsed_data):
    """Insert signal into database (thread-safe with retry logic)"""
    try:
        # Get instrument type
        instrument_type = parsed_data.get('instrument_type', 'OPTIONS')

        # Use thread-safe insert with retry logic
        signal_id = db.insert_signal(
            channel_id=channel_id,
            channel_name=channel_name,
            message_id=message_id,
            raw_text=raw_text,
            parsed_data=parsed_data,
            instrument_type=instrument_type
        )

        if signal_id:
            stats['stored_signals'] += 1

            # Track by type
            if instrument_type == 'FUTURES':
                stats['futures_signals'] += 1
            else:
                stats['options_signals'] += 1

            logging.info(f"[STORED] Signal ID: {signal_id} | Type: {instrument_type}")
            return signal_id
        else:
            logging.info(f"[SKIP] Duplicate message (Channel: {channel_name}, Msg ID: {message_id})")
            return None

    except Exception as e:
        logging.error(f"[DB ERROR] {e}")
        return None


# ========================================
# SIGNAL PROCESSING HELPERS
# ========================================

def _log_and_store_signal(parsed_data, channel_id, channel_name, message_id,
                          raw_text, was_combined=False, source_ids=None):
    """Log a parsed signal and insert it into the database.

    Shared logic used by both the single-message path and the combiner callback.
    """
    stats['parsed_signals'] += 1
    if was_combined:
        stats['combined_signals'] += 1

    instrument_type = parsed_data.get('instrument_type', 'OPTIONS')

    # Validate required fields
    if instrument_type == 'FUTURES':
        required_fields = ['symbol', 'action', 'entry_price', 'stop_loss',
                         'expiry_date', 'quantity', 'instrument_type']
    else:
        required_fields = ['symbol', 'strike', 'option_type', 'action',
                         'entry_price', 'stop_loss', 'expiry_date', 'quantity']

    missing = [f for f in required_fields if f not in parsed_data or parsed_data[f] is None]

    if missing:
        logging.warning(f"[INCOMPLETE] Missing fields: {missing}")
        logging.warning(f"   Message: {raw_text[:100]}")
    else:
        logging.info(f"[COMPLETE] All required fields present")

    if was_combined and source_ids:
        logging.info(f"[COMBINED] Signal from {len(source_ids)} messages: {source_ids}")

    # Log based on type
    if instrument_type == 'FUTURES':
        logging.info(f"[PARSED FUTURES] {parsed_data.get('symbol')} "
                   f"{parsed_data.get('expiry_month', 'FUT')}")
        logging.info(f"   Action: {parsed_data.get('action')} | "
                   f"Entry: {parsed_data.get('entry_price')} | "
                   f"SL: {parsed_data.get('stop_loss')}")
        logging.info(f"   Expiry: {parsed_data.get('expiry_date')} | "
                   f"Qty: {parsed_data.get('quantity')}")
    else:
        logging.info(f"[PARSED OPTIONS] {parsed_data.get('symbol')} "
                   f"{parsed_data.get('strike')} {parsed_data.get('option_type')}")
        logging.info(f"   Action: {parsed_data.get('action')} | "
                   f"Entry: {parsed_data.get('entry_price')} | "
                   f"SL: {parsed_data.get('stop_loss')}")
        logging.info(f"   Expiry: {parsed_data.get('expiry_date')} | "
                   f"Qty: {parsed_data.get('quantity')}")

    insert_signal(channel_id, channel_name, message_id, raw_text, parsed_data)


# ========================================
# COMBINER FLUSH CALLBACK
# ========================================
# Cache channel names so the flush callback (which fires on a timer) can log them
_channel_name_cache = {}


def _combiner_flush_callback(channel_id, combine_result):
    """Called when the combiner's timer fires and produces a signal from buffered messages."""
    if combine_result.parsed_data:
        channel_name = _channel_name_cache.get(channel_id, channel_id)
        msg_ids = combine_result.source_message_ids
        logging.info("")
        logging.info("=" * 60)
        logging.info(f"[FLUSH-COMBINE] Delayed signal from channel {channel_name}")
        logging.info(f"[TIME] {format_ist_timestamp()}")
        logging.info("=" * 60)
        _log_and_store_signal(
            parsed_data=combine_result.parsed_data,
            channel_id=channel_id,
            channel_name=channel_name,
            message_id=msg_ids[-1] if msg_ids else 0,
            raw_text=combine_result.combined_text,
            was_combined=combine_result.was_combined,
            source_ids=msg_ids,
        )


# Register flush callback if combiner is available
if signal_combiner:
    signal_combiner.set_flush_callback(_combiner_flush_callback)


# ========================================
# MESSAGE HANDLER
# ========================================

async def handle_message(event):
    """Handle incoming Telegram messages with rate limiting and multi-message combining."""
    if _shutdown_requested:
        return

    try:
        message_text = event.message.message
        if not message_text:
            return

        # Apply rate limiting
        rate_limiter.acquire()

        channel = await event.get_chat()
        channel_id = str(event.chat_id)
        channel_name = channel.title if hasattr(channel, 'title') else channel_id
        message_id = event.message.id

        # Cache channel name for flush callback
        _channel_name_cache[channel_id] = channel_name

        stats['total_messages'] += 1

        # Log preview with IST timestamp
        logging.info("")
        logging.info("="*60)
        logging.info(f"[NEW] Message from: {channel_name} (ID: {channel_id})")
        logging.info(f"[TIME] {format_ist_timestamp()}")
        preview = message_text[:80].replace('\n', ' ')
        if len(message_text) > 80:
            preview += '...'
        logging.info(f"[PREVIEW] {preview}")
        logging.info("="*60)

        # ---- Multi-message combiner path ----
        if signal_combiner:
            result = await signal_combiner.process_message(
                channel_id=channel_id,
                message_text=message_text,
                message_id=message_id,
            )

            if result is None:
                # Message buffered, waiting for more
                logging.info(f"[BUFFERED] Waiting for follow-up messages...")
                return

            if result.was_noise:
                stats['noise_filtered'] += 1
                stats['parsing_failures'] += 1
                logging.info(f"[NOISE] Filtered non-trading message")
                return

            if result.parsed_data:
                _log_and_store_signal(
                    parsed_data=result.parsed_data,
                    channel_id=channel_id,
                    channel_name=channel_name,
                    message_id=message_id,
                    raw_text=result.combined_text,
                    was_combined=result.was_combined,
                    source_ids=result.source_message_ids,
                )
            else:
                stats['parsing_failures'] += 1
                logging.info(f"[SKIP] Not a trading signal")
            return

        # ---- Fallback: original single-message path (if combiner not available) ----
        parsed_data = parser.parse(message_text, channel_id=channel_id)

        if parsed_data:
            _log_and_store_signal(
                parsed_data=parsed_data,
                channel_id=channel_id,
                channel_name=channel_name,
                message_id=message_id,
                raw_text=message_text,
            )
        else:
            stats['parsing_failures'] += 1
            logging.info(f"[SKIP] Not a trading signal")

    except Exception as e:
        logging.error(f"[ERROR] Error handling message: {e}")
        import traceback
        traceback.print_exc()


async def main():
    """Main function"""
    await client.start(TELEGRAM_PHONE)

    # Get user info
    me = await client.get_me()
    logging.info(f"[OK] Connected to Telegram as {me.phone}")

    # Display expiry information BEFORE starting monitoring
    display_expiry_info()

    # Get channel entities and FORCE CATCH-UP for each
    channel_entities = []
    for channel_id in MONITORED_CHANNELS:
        try:
            entity = await client.get_entity(channel_id)
            channel_entities.append(entity)

            # IMPORTANT: Fetch recent messages to "wake up" the channel
            # This forces Telegram to send us new messages from this channel
            try:
                await client.get_messages(entity, limit=1)
                logging.info(f"[OK] Monitoring: {entity.title} (synced)")
            except Exception as sync_err:
                logging.info(f"[OK] Monitoring: {entity.title} (sync skipped: {type(sync_err).__name__})")

        except Exception as e:
            logging.error(f"[ERROR] Failed to get channel {channel_id}: {e}")

    logging.info("="*80)
    logging.info(f"[START] Monitoring {len(channel_entities)} channels")
    logging.info(f"[MODE] {PARSER_TYPE} - {'OPTIONS + FUTURES' if FUTURES_SUPPORT else 'OPTIONS only'}")
    if signal_combiner:
        logging.info(f"[COMBINE] Multi-message combining enabled (window=30s, max=5)")
    else:
        logging.info(f"[COMBINE] Multi-message combining disabled")
    logging.info(f"[LOG] Output: {log_filename}")
    logging.info("Press Ctrl+C to stop")
    logging.info("="*80)

    # Register event handler for ALL entities
    @client.on(events.NewMessage(chats=channel_entities))
    async def handler(event):
        await handle_message(event)

    # Run until disconnected
    await client.run_until_disconnected()


def print_stats():
    """Print statistics including combiner stats"""
    logging.info("")
    logging.info("="*80)
    logging.info("STATISTICS")
    logging.info("="*80)
    logging.info(f"Total Messages:     {stats['total_messages']}")
    logging.info(f"Noise Filtered:     {stats['noise_filtered']}")
    logging.info(f"Parsed Signals:     {stats['parsed_signals']}")
    logging.info(f"  - Options:        {stats['options_signals']}")
    logging.info(f"  - Futures:        {stats['futures_signals']}")
    logging.info(f"  - Combined:       {stats['combined_signals']}")
    logging.info(f"Stored Signals:     {stats['stored_signals']}")
    logging.info(f"Parse Failures:     {stats['parsing_failures']}")
    logging.info("="*80)

    # Print combiner-specific stats if available
    if signal_combiner:
        signal_combiner.log_stats()


if __name__ == '__main__':
    logging.info(f"[START] Telegram Reader starting at {format_ist_timestamp()}")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        if not _shutdown_requested:
            logging.info("\n[STOP] Shutting down...")
            print_stats()
    except Exception as e:
        logging.error(f"[FATAL] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        logging.info(f"[END] Telegram Reader stopped at {format_ist_timestamp()}")
    # Note: No db.close() needed - ThreadSafeDB uses connection-per-operation
