"""
fetch_historical_messages.py
Fetch last 30 days of messages from Telegram channel for training
"""

import asyncio
import json
import sqlite3
from datetime import datetime, timedelta
from telethon import TelegramClient
import sys
import io

# Fix Windows encoding
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Load Telegram config
try:
    with open('telegram_config.json', 'r') as f:
        config = json.load(f)
        API_ID = config['api_id']
        API_HASH = config['api_hash']
        PHONE = config.get('phone') or config.get('phone_number')
except Exception as e:
    print(f"[ERROR] Failed to load telegram_config.json: {e}")
    print("\nYour telegram_config.json should have:")
    print('{')
    print('  "api_id": 12345678,')
    print('  "api_hash": "your_hash",')
    print('  "phone": "+919xxxxxxxxx"')
    print('}')
    exit(1)

# Channel to fetch from
CHANNEL_ID = -1003282204738  # Update with your channel ID

# Database setup
db = sqlite3.connect('premium_signals.db')
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

print("\n" + "="*70)
print("HISTORICAL MESSAGE FETCHER")
print("="*70)
print(f"Channel ID: {CHANNEL_ID}")
print(f"Fetching last 30 days of messages...")
print("="*70 + "\n")

async def fetch_messages():
    """Fetch historical messages from Telegram channel"""
    
    # Initialize client
    client = TelegramClient('historical_fetch', API_ID, API_HASH)
    
    await client.start(PHONE)
    
    # Get channel entity
    try:
        channel = await client.get_entity(CHANNEL_ID)
        print(f"[OK] Connected to channel: {channel.title}")
    except Exception as e:
        print(f"[ERROR] Failed to get channel {CHANNEL_ID}: {e}")
        return
    
    # Calculate date 30 days ago (timezone aware)
    from datetime import timezone
    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
    
    print(f"[FETCH] Getting messages since: {thirty_days_ago.strftime('%Y-%m-%d')}")
    print()
    
    # Fetch messages
    messages_fetched = 0
    messages_stored = 0
    
    async for message in client.iter_messages(channel, limit=500):
        # Make comparison timezone-aware
        if message.date.replace(tzinfo=timezone.utc) < thirty_days_ago:
            break
        
        if not message.message:
            continue
        
        messages_fetched += 1
        
        # Store in database
        try:
            cursor = db.cursor()
            cursor.execute("""
                INSERT OR IGNORE INTO signals 
                (channel_id, channel_name, message_id, raw_text, timestamp, processed, parser_type)
                VALUES (?, ?, ?, ?, ?, 0, 'UNFETCHED')
            """, (
                str(message.chat_id),
                channel.title,
                message.id,
                message.message,
                message.date.isoformat()
            ))
            
            if cursor.rowcount > 0:
                messages_stored += 1
                print(f"[{messages_stored:3d}] {message.date.strftime('%Y-%m-%d %H:%M')} | {message.message[:50]}...")
            
            db.commit()
            
        except Exception as e:
            print(f"[ERROR] Failed to store message {message.id}: {e}")
    
    print()
    print("="*70)
    print("FETCH COMPLETE")
    print("="*70)
    print(f"Messages fetched: {messages_fetched}")
    print(f"Messages stored: {messages_stored}")
    print(f"Database: premium_signals.db")
    print("="*70)
    print()
    print("Next step: Run historical trainer")
    print("python historical_trainer.py --api-key YOUR_KEY --days 30")
    print()
    
    await client.disconnect()

if __name__ == '__main__':
    try:
        asyncio.run(fetch_messages())
    except KeyboardInterrupt:
        print("\n[STOP] Interrupted by user")
    finally:
        db.close()
