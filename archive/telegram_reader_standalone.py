"""
telegram_reader_standalone.py - Standalone version with all config inline
No external config files needed - everything is in this file
"""

import logging
import json
from telethon import TelegramClient, events
from datetime import datetime
import asyncio
import os
import sys

# Fix Windows console encoding issues with emojis
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Import your modules
from signal_parser_enhanced_v2 import EnhancedSignalParser
from signal_parser_with_claude_fallback import SignalParserWithClaudeFallback
from database import TradingDatabase

# ==================== CONFIGURATION ====================

# Telegram API credentials
TELEGRAM_API_ID = 25677420
TELEGRAM_API_HASH = "3fe3d6d76fdffd005104a5df5db5ba6f"
TELEGRAM_PHONE = "+919833459174"

# Channels to monitor
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
    -1003282204738,  # JP Paper trade - Nov-25
]

# ==================== SETUP LOGGING ====================

# Fix Windows console encoding
if sys.platform == 'win32':
    import io
    # Reconfigure logging handlers to use UTF-8
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

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

# ==================== MESSAGE LOGGER ====================

class MessageLogger:
    """Log all received messages to file"""
    
    def __init__(self, log_dir='telegram_messages'):
        self.log_dir = log_dir
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Create today's log file
        today = datetime.now().strftime('%Y-%m-%d')
        self.current_file = os.path.join(log_dir, f'messages_{today}.txt')
    
    def log_message(self, channel_id, channel_name, message_id, timestamp, content):
        """Log a message to the daily file"""
        try:
            with open(self.current_file, 'a', encoding='utf-8') as f:
                f.write("="*80 + "\n")
                f.write("MESSAGE RECEIVED\n")
                f.write(f"Channel ID: {channel_id}\n")
                f.write(f"Channel Name: {channel_name}\n")
                f.write(f"Message ID: {message_id}\n")
                f.write(f"Timestamp: {timestamp}\n")
                f.write("--- MESSAGE CONTENT ---\n")
                f.write(content + "\n")
                f.write("="*80 + "\n\n")
        except Exception as e:
            logging.error(f"[ERROR] Failed to log message: {e}")

# ==================== TELEGRAM READER ====================

class TelegramSignalReader:
    """Read messages from Telegram channels and parse trading signals"""
    
    def __init__(self, api_id, api_hash, phone, channels):
        self.client = TelegramClient('trading_session', api_id, api_hash)
        self.phone = phone
        self.channels = channels
        self.db = TradingDatabase()
        
        # Initialize enhanced parser
        self.parser = EnhancedSignalParser(
            rules_file='parsing_rules_enhanced_v2.json',
            instruments_cache='instruments_cache.csv'
        )
        
        self.message_logger = MessageLogger()
        
        # Statistics
        self.stats = {
            'total_messages': 0,
            'parsed_signals': 0,
            'stored_signals': 0,
            'duplicates': 0,
            'parsing_failures': 0
        }
    
    async def start(self):
        """Start the Telegram client and listen for messages"""
        
        await self.client.start(phone=self.phone)
        logging.info(f"[OK] Connected to Telegram as {self.phone}")
        
        # Get channel entities
        channel_entities = []
        for channel_id in self.channels:
            try:
                entity = await self.client.get_entity(channel_id)
                channel_entities.append(entity)
                logging.info(f"[OK] Monitoring: {entity.title}")
            except Exception as e:
                logging.error(f"[ERROR] Failed to get channel {channel_id}: {e}")
        
        logging.info("="*80)
        logging.info(f"[START] Monitoring {len(channel_entities)} channels")
        logging.info("Press Ctrl+C to stop")
        logging.info("="*80)
        
        # Listen for new messages
        @self.client.on(events.NewMessage(chats=channel_entities))
        async def handler(event):
            await self.handle_message(event)
        
        # Keep running
        await self.client.run_until_disconnected()
    
    async def handle_message(self, event):
        """Handle incoming message"""
        
        try:
            # Get message details
            message_text = event.message.message
            channel = await event.get_chat()
            channel_id = str(channel.id)
            channel_name = channel.title if hasattr(channel, 'title') else 'Unknown'
            message_id = event.message.id
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            self.stats['total_messages'] += 1
            
            # Log message to file
            self.message_logger.log_message(
                channel_id, channel_name, message_id, timestamp, message_text
            )
            
            # Log to console
            logging.info("")
            logging.info("="*60)
            logging.info(f"[NEW] Message from: {channel_name}")
            logging.info(f"[TIME] {timestamp}")
            preview = message_text[:50].replace('\n', ' ') + '...' if len(message_text) > 50 else message_text
            logging.info(f"[PREVIEW] {preview}")
            logging.info("="*60)
            
            # Parse the message
            parsed_signal = self.parser.parse(message_text, channel_id=channel_id)
            
            if parsed_signal:
                self.stats['parsed_signals'] += 1
                logging.info(f"[✓ PARSED] Signal extracted")
                
                # Store in database
                signal_id = self.db.insert_signal(
                    channel_id=channel_id,
                    channel_name=channel_name,
                    message_id=message_id,
                    message_text=message_text,
                    parsed_data=parsed_signal,
                    timestamp=timestamp
                )
                
                if signal_id:
                    self.stats['stored_signals'] += 1
                    logging.info(f"[✓ STORED] Signal ID: {signal_id}")
                else:
                    self.stats['duplicates'] += 1
                    logging.info(f"[SKIP] Duplicate signal")
            else:
                self.stats['parsing_failures'] += 1
                logging.info(f"[✗ SKIP] Could not parse signal")
        
        except Exception as e:
            logging.error(f"[ERROR] Error handling message: {e}")
            import traceback
            traceback.print_exc()
    
    def print_stats(self):
        """Print statistics"""
        logging.info("")
        logging.info("="*80)
        logging.info("STATISTICS")
        logging.info("="*80)
        logging.info(f"Total Messages:     {self.stats['total_messages']}")
        logging.info(f"Parsed Signals:     {self.stats['parsed_signals']}")
        logging.info(f"Stored Signals:     {self.stats['stored_signals']}")
        logging.info(f"Duplicates:         {self.stats['duplicates']}")
        logging.info(f"Parse Failures:     {self.stats['parsing_failures']}")
        logging.info("="*80)

# ==================== MAIN ====================

async def main():
    """Main entry point"""
    
    try:
        reader = TelegramSignalReader(
            api_id=TELEGRAM_API_ID,
            api_hash=TELEGRAM_API_HASH,
            phone=TELEGRAM_PHONE,
            channels=MONITORED_CHANNELS
        )
        
        await reader.start()
        
    except KeyboardInterrupt:
        logging.info("")
        logging.info("[STOP] Stopping Telegram reader...")
        reader.print_stats()
    
    except Exception as e:
        logging.error(f"[ERROR] Fatal error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
