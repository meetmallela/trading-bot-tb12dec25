"""
telegram_reader_production.py - ENHANCED VERSION
WITH FUTURES SUPPORT + EXPIRY DATE DISPLAY + TIMESTAMPED LOGS
"""

import asyncio
import json
import logging
import sqlite3
from datetime import datetime
from telethon import TelegramClient, events
from telethon.tl.functions.channels import GetParticipantRequest
import sys
import io

# Fix Windows console encoding issues
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Generate timestamped log filename
log_start_time = datetime.now()
log_filename = log_start_time.strftime('telegram_reader_%d%b%Y_%H_%M_%S.log')

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
   # -1003282204738,  # JP Paper trade - Dec-25
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

# Load Claude API key
try:
    with open('claude_api_key.txt', 'r') as f:
        claude_api_key = f.read().strip()
except:
    logging.warning("[WARNING] claude_api_key.txt not found - fallback to Claude API disabled")
    claude_api_key = None

# Initialize Telegram client
client = TelegramClient('trading_bot', TELEGRAM_API_ID, TELEGRAM_API_HASH)

# Initialize database
db = sqlite3.connect('trading.db', check_same_thread=False)
db.execute('''
    CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_id TEXT,
        channel_name TEXT,
        message_id INTEGER,
        raw_text TEXT,
        parsed_data TEXT,
        timestamp TEXT,
        processed INTEGER DEFAULT 0,
        instrument_type TEXT DEFAULT 'OPTIONS',
        UNIQUE(channel_id, message_id)
    )
''')
db.commit()

# Initialize parser with futures support
parser = SignalParserWithFutures(
    claude_api_key=claude_api_key,
    rules_file='parsing_rules_enhanced_v2.json'
)

if FUTURES_SUPPORT:
    logging.info(f"[OK] Using SignalParserWithFutures (OPTIONS + FUTURES support)")
else:
    logging.info(f"[OK] Using legacy parser (OPTIONS only)")

# Statistics
stats = {
    'total_messages': 0,
    'parsed_signals': 0,
    'stored_signals': 0,
    'parsing_failures': 0,
    'options_signals': 0,
    'futures_signals': 0
}


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
        except:
            try:
                df = pd.read_csv('valid_instruments.csv')
                logging.info("[EXPIRY] Loaded valid_instruments.csv")
            except:
                logging.warning("[EXPIRY] Could not load instruments file")
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
    logging.info("📅 CURRENT MONTH EXPIRY DATES")
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
                    logging.info(f"  ✓ {expiry}")
                logging.info("")
        
        # Display BANKNIFTY, FINNIFTY, MIDCPNIFTY
        for symbol in ['BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']:
            if symbol in expiry_info:
                logging.info(f"{symbol}:")
                for expiry in expiry_info[symbol]:
                    logging.info(f"  ✓ {expiry}")
                logging.info("")
        
        # Display any other symbols found
        other_symbols = [s for s in expiry_info.keys() 
                        if s not in ['NIFTY', 'SENSEX', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']]
        for symbol in other_symbols:
            logging.info(f"{symbol}:")
            for expiry in expiry_info[symbol]:
                logging.info(f"  ✓ {expiry}")
            logging.info("")
    else:
        logging.warning("Could not load expiry information from CSV/Parquet")
    
    logging.info("="*80)
    logging.info("")


def insert_signal(channel_id, channel_name, message_id, raw_text, parsed_data):
    """Insert signal into database"""
    try:
        cursor = db.cursor()
        
        # Get instrument type
        instrument_type = parsed_data.get('instrument_type', 'OPTIONS')
        
        cursor.execute("""
            INSERT OR IGNORE INTO signals 
            (channel_id, channel_name, message_id, raw_text, parsed_data, timestamp, processed, instrument_type)
            VALUES (?, ?, ?, ?, ?, ?, 0, ?)
        """, (
            channel_id,
            channel_name,
            message_id,
            raw_text,
            json.dumps(parsed_data),
            datetime.now().isoformat(),
            instrument_type
        ))
        db.commit()
        
        if cursor.rowcount > 0:
            signal_id = cursor.lastrowid
            stats['stored_signals'] += 1
            
            # Track by type
            if instrument_type == 'FUTURES':
                stats['futures_signals'] += 1
            else:
                stats['options_signals'] += 1
            
            logging.info(f"[✓ STORED] Signal ID: {signal_id} | Type: {instrument_type}")
            return signal_id
        else:
            logging.info(f"[SKIP] Duplicate message (Channel: {channel_name}, Msg ID: {message_id})")
            return None
        
    except Exception as e:
        logging.error(f"[✗ DB ERROR] {e}")
        return None


async def handle_message(event):
    """Handle incoming Telegram messages"""
    try:
        message_text = event.message.message
        if not message_text:
            return
        
        channel = await event.get_chat()
        channel_id = str(event.chat_id)
        channel_name = channel.title if hasattr(channel, 'title') else str(channel_id)
        message_id = event.message.id
        
        stats['total_messages'] += 1
        
        # Log preview
        logging.info("")
        logging.info("="*60)
        logging.info(f"[NEW] Message from: {channel_name} (ID: {channel_id})")
        preview = message_text[:50].replace('\n', ' ') + '...' if len(message_text) > 50 else message_text
        logging.info(f"[PREVIEW] {preview}")
        logging.info("="*60)
        
        # Parse the message (handles both OPTIONS and FUTURES)
        parsed_data = parser.parse(message_text, channel_id=channel_id)
        
        if parsed_data:
            stats['parsed_signals'] += 1
            
            # Get instrument type
            instrument_type = parsed_data.get('instrument_type', 'OPTIONS')
            
            # Validate required fields based on type
            if instrument_type == 'FUTURES':
                required_fields = ['symbol', 'action', 'entry_price', 'stop_loss', 
                                 'expiry_date', 'quantity', 'instrument_type']
            else:
                required_fields = ['symbol', 'strike', 'option_type', 'action', 
                                 'entry_price', 'stop_loss', 'expiry_date', 'quantity']
            
            missing = [f for f in required_fields if f not in parsed_data or parsed_data[f] is None]
            
            if missing:
                logging.warning(f"[⚠️ INCOMPLETE] Missing fields: {missing}")
                logging.warning(f"   Message: {message_text[:100]}")
            else:
                logging.info(f"[✓ COMPLETE] All required fields present")
            
            # Log based on type
            if instrument_type == 'FUTURES':
                logging.info(f"[✓ PARSED FUTURES] {parsed_data.get('symbol')} "
                           f"{parsed_data.get('expiry_month', 'FUT')}")
                logging.info(f"   Action: {parsed_data.get('action')} | "
                           f"Entry: {parsed_data.get('entry_price')} | "
                           f"SL: {parsed_data.get('stop_loss')}")
                logging.info(f"   Expiry: {parsed_data.get('expiry_date')} | "
                           f"Qty: {parsed_data.get('quantity')}")
            else:
                logging.info(f"[✓ PARSED OPTIONS] {parsed_data.get('symbol')} "
                           f"{parsed_data.get('strike')} {parsed_data.get('option_type')}")
                logging.info(f"   Action: {parsed_data.get('action')} | "
                           f"Entry: {parsed_data.get('entry_price')} | "
                           f"SL: {parsed_data.get('stop_loss')}")
                logging.info(f"   Expiry: {parsed_data.get('expiry_date')} | "
                           f"Qty: {parsed_data.get('quantity')}")
            
            # Insert into database
            insert_signal(channel_id, channel_name, message_id, message_text, parsed_data)
        else:
            stats['parsing_failures'] += 1
            logging.info(f"[✗ SKIP] Not a trading signal")
            
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
            except:
                logging.info(f"[OK] Monitoring: {entity.title}")
                
        except Exception as e:
            logging.error(f"[ERROR] Failed to get channel {channel_id}: {e}")
    
    logging.info("="*80)
    logging.info(f"[START] Monitoring {len(channel_entities)} channels")
    logging.info(f"[MODE] {PARSER_TYPE} - {'OPTIONS + FUTURES' if FUTURES_SUPPORT else 'OPTIONS only'}")
    logging.info(f"[LOG] Output: {log_filename}")
    logging.info("Press Ctrl+C to stop")
    logging.info("="*80)
    
    # Register event handler for ALL entities
    @client.on(events.NewMessage(chats=channel_entities))
    async def handler(event):
        await handle_message(event)
    
    # CRITICAL FIX: Also create a catch-all handler for the problematic channel
    # This ensures we don't miss messages even if entity subscription fails
    @client.on(events.NewMessage(chats=[-1003282204738]))
    async def jp_handler(event):
        logging.info("[JP SPECIAL] Got message from JP Paper trade!")
        await handle_message(event)
    
    # Run until disconnected
    await client.run_until_disconnected()


def print_stats():
    """Print statistics"""
    logging.info("")
    logging.info("="*80)
    logging.info("STATISTICS")
    logging.info("="*80)
    logging.info(f"Total Messages:     {stats['total_messages']}")
    logging.info(f"Parsed Signals:     {stats['parsed_signals']}")
    logging.info(f"  - Options:        {stats['options_signals']}")
    logging.info(f"  - Futures:        {stats['futures_signals']}")
    logging.info(f"Stored Signals:     {stats['stored_signals']}")
    logging.info(f"Parse Failures:     {stats['parsing_failures']}")
    logging.info("="*80)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("\n[STOP] Shutting down...")
        print_stats()
    finally:
        db.close()
