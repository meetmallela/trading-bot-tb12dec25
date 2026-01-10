"""
telegram_reader_production.py - WITH FUTURES SUPPORT
Handles both OPTIONS and FUTURES signals
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

# Telegram API credentials
TELEGRAM_API_ID = 25677420
TELEGRAM_API_HASH = "3fe3d6d76fdffd005104a5df5db5ba6f"
TELEGRAM_PHONE = "+919833459174"

# Channels to monitor (INTEGER format!)
MONITORED_CHANNELS = [
    -1002498088029,  # RJ - STUDENT PRACTICE CALLS
    -1002770917134,  # MCX PREMIUM
    -1002842854743,  # VIP RJ Paid Education Purpose
    -1003089362819,  # Paid Premium group
    -1001903138387,  # COPY MY TRADES BANKNIFTY
    -1002380215256,  # PREMIUM_GROUP
    -1002568252699,  # TARGET HIT CLUB
    -1002201480769,  # Trader ayushi
    -1001294857397,  # Mcx Trading King Official Group
    -1002431924245,  # MCX JACKPOT TRADING
    -1001389090145,  # Stockpro Online
    -1001456128948,  # Ashish Kyal Trading Gurukul
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - TELEGRAM - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('telegram_reader.log', encoding='utf-8'),
        logging.StreamHandler(
            io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
            if sys.platform == 'win32' else sys.stdout
        )
    ]
)

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
