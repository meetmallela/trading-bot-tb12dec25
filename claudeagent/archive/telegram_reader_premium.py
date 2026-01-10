"""
telegram_reader_premium.py
Telegram reader with premium channel routing to Claude agent
"""

import asyncio
import json
import logging
import sqlite3
import sys
import io
from datetime import datetime
from telethon import TelegramClient, events

# Fix Windows encoding
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('telegram_premium.log', encoding='utf-8'),
        logging.StreamHandler(
            io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
            if sys.platform == 'win32' else sys.stdout
        )
    ]
)

logger = logging.getLogger('TELEGRAM')

# Load Telegram config
try:
    with open('telegram_config.json', 'r') as f:
        tg_config = json.load(f)
        TELEGRAM_API_ID = tg_config['api_id']
        TELEGRAM_API_HASH = tg_config['api_hash']
        TELEGRAM_PHONE = tg_config['phone']
except Exception as e:
    logger.error(f"Failed to load telegram_config.json: {e}")
    exit(1)

# Load Claude API key
try:
    with open('claude_api_key.txt', 'r') as f:
        claude_api_key = f.read().strip()
except:
    logger.error("claude_api_key.txt not found!")
    logger.info("Please create claude_api_key.txt with your Anthropic API key")
    exit(1)

# CHANNEL CONFIGURATION
#PREMIUM_CHANNEL_ID = -1003282204738,  # JP Paper trade - Dec-25 # Update with your premium channel ID
PREMIUM_CHANNEL_ID = -1002498088029 ,  #RJ channel 
MONITORED_CHANNELS = [PREMIUM_CHANNEL_ID]

# Initialize Telegram client
client = TelegramClient('premium_bot', TELEGRAM_API_ID, TELEGRAM_API_HASH)

# Initialize database
db = sqlite3.connect('premium_signals.db', check_same_thread=False)
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
        parser_type TEXT,
        UNIQUE(channel_id, message_id)
    )
''')
db.commit()

# Initialize Premium Agent
from premium_channel_agent import PremiumChannelAgent

try:
    premium_agent = PremiumChannelAgent(
        claude_api_key=claude_api_key,
        instruments_csv='../valid_instruments.csv'
    )
    logger.info("[OK] Premium Claude Agent initialized")
except Exception as e:
    logger.error(f"[ERROR] Failed to initialize premium agent: {e}")
    exit(1)

# Statistics
stats = {'total_messages': 0, 'parsed_signals': 0, 'stored_signals': 0, 'parsing_failures': 0}

def insert_signal(channel_id, channel_name, message_id, raw_text, parsed_data, parser_type):
    try:
        cursor = db.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO signals 
            (channel_id, channel_name, message_id, raw_text, parsed_data, timestamp, processed, parser_type)
            VALUES (?, ?, ?, ?, ?, ?, 0, ?)
        """, (channel_id, channel_name, message_id, raw_text, json.dumps(parsed_data), datetime.now().isoformat(), parser_type))
        db.commit()
        if cursor.rowcount > 0:
            stats['stored_signals'] += 1
            logger.info(f"[STORED] Signal ID: {cursor.lastrowid}")
            return cursor.lastrowid
    except Exception as e:
        logger.error(f"[DB ERROR] {e}")

async def handle_message(event):
    try:
        message_text = event.message.message
        if not message_text:
            return
        
        channel = await event.get_chat()
        channel_id = str(event.chat_id)
        channel_name = channel.title if hasattr(channel, 'title') else str(channel_id)
        message_id = event.message.id
        
        stats['total_messages'] += 1
        
        logger.info("")
        logger.info("="*80)
        logger.info(f"[NEW] Message from: {channel_name}")
        preview = message_text[:70].replace('\n', ' ') + '...' if len(message_text) > 70 else message_text
        logger.info(f"[PREVIEW] {preview}")
        logger.info("="*80)
        
        logger.info("[PREMIUM] Using Claude Agent...")
        parsed_data = premium_agent.parse_signal(message=message_text, channel_id=channel_id, channel_name=channel_name)
        
        if parsed_data:
            stats['parsed_signals'] += 1
            logger.info(f"[SUCCESS] {parsed_data.get('symbol')} {parsed_data.get('strike')} {parsed_data.get('option_type')}")
            logger.info(f"   Action: {parsed_data.get('action')} | Entry: {parsed_data.get('entry_price')} | SL: {parsed_data.get('stop_loss')}")
            logger.info(f"   Tradingsymbol: {parsed_data.get('tradingsymbol')} | Expiry: {parsed_data.get('expiry_date')}")
            insert_signal(channel_id, channel_name, message_id, message_text, parsed_data, "CLAUDE_AGENT")
        else:
            stats['parsing_failures'] += 1
            logger.info(f"[SKIP] Not a trading signal")
            
    except Exception as e:
        logger.error(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()

async def main():
    print("\n" + "="*80)
    print("PREMIUM CHANNEL AGENT - TELEGRAM READER")
    print("="*80)
    
    await client.start(TELEGRAM_PHONE)
    me = await client.get_me()
    logger.info(f"[OK] Connected as {me.phone}")
    
    channel_entities = []
    for channel_id in MONITORED_CHANNELS:
        try:
            entity = await client.get_entity(channel_id)
            channel_entities.append(entity)
            await client.get_messages(entity, limit=1)
            logger.info(f"[OK] Monitoring: {entity.title} (Claude Agent)")
        except Exception as e:
            logger.error(f"[ERROR] Failed to get channel {channel_id}: {e}")
    
    logger.info("")
    logger.info("="*80)
    logger.info(f"[START] Monitoring {len(channel_entities)} channel(s)")
    logger.info("Press Ctrl+C to stop")
    logger.info("="*80)
    
    @client.on(events.NewMessage(chats=channel_entities))
    async def handler(event):
        await handle_message(event)
    
    await client.run_until_disconnected()

def print_stats():
    logger.info("\n" + "="*80)
    logger.info("STATISTICS")
    logger.info("="*80)
    logger.info(f"Total Messages: {stats['total_messages']}")
    logger.info(f"Parsed Signals: {stats['parsed_signals']}")
    logger.info(f"Stored Signals: {stats['stored_signals']}")
    logger.info(f"Parse Failures: {stats['parsing_failures']}")
    logger.info("="*80)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n[STOP] Shutting down...")
        print_stats()
    finally:
        db.close()
