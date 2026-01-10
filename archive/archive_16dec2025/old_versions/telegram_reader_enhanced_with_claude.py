"""
telegram_reader_enhanced.py - Read signals from Telegram channels with AI-powered parsing
Stores in SQLite database AND logs ALL messages to text file for troubleshooting

FEATURES:
✅ Hybrid signal parsing: Fast regex (80%) + Claude API fallback (20%)
✅ Comprehensive message logging to daily text files
✅ SQLite database storage with duplicate detection
✅ Real-time statistics and performance monitoring
✅ 95%+ parsing accuracy with minimal cost (~₹15-75/month)

REQUIREMENTS:
- anthropic library: pip install anthropic
- Claude API key from https://console.anthropic.com/settings/keys
"""

import logging
import json
from telethon import TelegramClient, events
from datetime import datetime
import asyncio
import os

# Import your modules
from config import API_KEY
from signal_parser_with_claude import SignalParserWithClaude
from database import TradingDatabase

# ⚠️ IMPORTANT: Set your Claude API key here
CLAUDE_API_KEY = "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"  # Replace with your actual key

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - TELEGRAM - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('telegram_reader.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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

# ==================== MESSAGE LOGGER ====================

class MessageLogger:
    """Logs ALL Telegram messages to text file for troubleshooting"""
    
    def __init__(self, log_dir="telegram_messages"):
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        
        # Create daily log file
        today = datetime.now().strftime("%Y-%m-%d")
        self.log_file = os.path.join(log_dir, f"messages_{today}.txt")
        
        # Channel name cache
        self.channel_names = {}
    
    def log_message(self, channel_id, channel_name, message_id, message_text, received_time):
        """Log message to text file with full details"""
        
        separator = "=" * 80
        
        log_entry = f"""
{separator}
MESSAGE RECEIVED: {received_time}
{separator}
Channel ID: {channel_id}
Channel Name: {channel_name}
Message ID: {message_id}
Timestamp: {received_time}

--- MESSAGE CONTENT ---
{message_text}

{separator}

"""
        
        try:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(log_entry)
        except Exception as e:
            logger.error(f"Failed to write to message log: {e}")

# ==================== TELEGRAM READER ====================

class TelegramSignalReader:
    """Read and store signals from Telegram with enhanced logging"""
    
    def __init__(self, api_id, api_hash, phone, channels):
        self.client = TelegramClient('trading_session', api_id, api_hash)
        self.phone = phone
        self.channels = channels
        self.db = TradingDatabase()
        
        # Initialize Claude-enhanced parser with fallback
        self.parser = SignalParserWithClaude(
            claude_api_key=CLAUDE_API_KEY,
            instruments_csv_path="instruments.csv"  # Optional: add if you have it
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
        """Start Telegram client and register handlers"""
        
        await self.client.start(phone=self.phone)
        logger.info(f"[OK] Telegram client started")
        
        # Get channel names for better logging
        for channel_id in self.channels:
            try:
                entity = await self.client.get_entity(channel_id)
                self.message_logger.channel_names[channel_id] = entity.title
                logger.info(f"[CHANNEL] {channel_id} -> {entity.title}")
            except Exception as e:
                logger.error(f"[ERROR] Could not get name for channel {channel_id}: {e}")
                self.message_logger.channel_names[channel_id] = f"Unknown_{channel_id}"
        
        # Register message handler
        @self.client.on(events.NewMessage(chats=self.channels))
        async def handle_new_message(event):
            await self.process_message(event)
        
        logger.info(f"[OK] Monitoring {len(self.channels)} channels")
        logger.info(f"[INFO] ALL messages will be logged to: {self.message_logger.log_file}")
        logger.info(f"[INFO] Waiting for signals...")
        
        # Keep running
        await self.client.run_until_disconnected()
    
    async def process_message(self, event):
        """Process incoming Telegram message with comprehensive logging"""
        
        self.stats['total_messages'] += 1
        
        message_id = str(event.message.id)
        channel_id = str(event.chat_id)
        message_text = event.message.message
        received_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Get channel name
        channel_name = self.message_logger.channel_names.get(
            int(channel_id), 
            f"Unknown_{channel_id}"
        )
        
        if not message_text:
            return
        
        # === LOG EVERY MESSAGE TO FILE ===
        self.message_logger.log_message(
            channel_id=channel_id,
            channel_name=channel_name,
            message_id=message_id,
            message_text=message_text,
            received_time=received_time
        )
        
        # Console output
        logger.info(f"\n{'='*60}")
        logger.info(f"[NEW] Message from: {channel_name}")
        logger.info(f"[TIME] {received_time}")
        logger.info(f"[PREVIEW] {message_text[:150]}...")
        
        # Try to parse signal (uses regex first, Claude API as fallback)
        parsed_signal = self.parser.parse(message_text, channel_id=channel_id)
        
        if not parsed_signal:
            logger.debug(f"[SKIP] Not a trading signal (or parsing failed)")
            self.stats['parsing_failures'] += 1
            logger.info(f"{'='*60}\n")
            return
        
        self.stats['parsed_signals'] += 1
        logger.info(f"[✓ PARSED] {parsed_signal['trading_symbol']}")
        logger.info(f"[DETAILS] Entry: {parsed_signal['entry_price']}, SL: {parsed_signal['stop_loss']}, Targets: {parsed_signal['targets']}")
        
        # Store in database (will skip if duplicate)
        signal_id = self.db.insert_signal(
            message_id=message_id,
            channel_id=channel_id,
            raw_text=message_text,
            parsed_data=parsed_signal
        )
        
        if signal_id:
            self.stats['stored_signals'] += 1
            logger.info(f"[✓ STORED] Signal ID: {signal_id} → Ready for order placement")
        else:
            self.stats['duplicates'] += 1
            logger.info(f"[DUPLICATE] Signal already exists, skipping")
        
        # Print stats every 10 messages
        if self.stats['total_messages'] % 10 == 0:
            self._print_stats()
        
        logger.info(f"{'='*60}\n")
    
    def _print_stats(self):
        """Print current statistics including parser performance"""
        logger.info(f"\n{'='*60}")
        logger.info(f"SESSION STATISTICS")
        logger.info(f"{'='*60}")
        logger.info(f"Total Messages:     {self.stats['total_messages']}")
        logger.info(f"Parsed Signals:     {self.stats['parsed_signals']}")
        logger.info(f"Stored in DB:       {self.stats['stored_signals']}")
        logger.info(f"Duplicates:         {self.stats['duplicates']}")
        logger.info(f"Parsing Failures:   {self.stats['parsing_failures']}")
        
        # Show parser performance (regex vs Claude)
        parser_stats = self.parser.get_stats()
        if parser_stats['total'] > 0:
            logger.info(f"---")
            logger.info(f"Parser Performance:")
            logger.info(f"  Regex Success:    {parser_stats['regex_success']} ({parser_stats.get('regex_rate', '0%')})")
            logger.info(f"  Claude Fallback:  {parser_stats['claude_success']} ({parser_stats.get('claude_rate', '0%')})")
            logger.info(f"  Success Rate:     {parser_stats.get('success_rate', '0%')}")
        
        logger.info(f"{'='*60}\n")
    
    async def read_history(self, limit=50):
        """Read recent messages (one-time catch-up) with logging"""
        
        logger.info(f"\n[HISTORY] Reading last {limit} messages from each channel...")
        
        for channel_id in self.channels:
            try:
                channel = await self.client.get_entity(channel_id)
                channel_name = channel.title
                self.message_logger.channel_names[channel_id] = channel_name
                
                logger.info(f"[CHANNEL] {channel_name}")
                
                count = 0
                async for message in self.client.iter_messages(channel, limit=limit):
                    if message.text:
                        message_id = str(message.id)
                        received_time = message.date.strftime("%Y-%m-%d %H:%M:%S")
                        
                        # Log to file
                        self.message_logger.log_message(
                            channel_id=str(channel_id),
                            channel_name=channel_name,
                            message_id=message_id,
                            message_text=message.text,
                            received_time=received_time
                        )
                        
                        # Try to parse (uses regex first, Claude API as fallback)
                        parsed_signal = self.parser.parse(message.text, channel_id=str(channel_id))
                        
                        if parsed_signal:
                            signal_id = self.db.insert_signal(
                                message_id=message_id,
                                channel_id=str(channel_id),
                                raw_text=message.text,
                                parsed_data=parsed_signal
                            )
                            if signal_id:
                                count += 1
                
                logger.info(f"[STORED] {count} new signals from this channel")
                
            except Exception as e:
                logger.error(f"[ERROR] Failed to read from channel {channel_id}: {e}")
        
        logger.info(f"[DONE] History read complete\n")


# ==================== MAIN ====================

async def main():
    """Main execution"""
    
    try:
        reader = TelegramSignalReader(
            api_id=TELEGRAM_API_ID,
            api_hash=TELEGRAM_API_HASH,
            phone=TELEGRAM_PHONE,
            channels=MONITORED_CHANNELS
        )
        
        # Optional: Read history first
        import sys
        if '--history' in sys.argv:
            limit = 100
            if '--limit' in sys.argv:
                try:
                    limit = int(sys.argv[sys.argv.index('--limit') + 1])
                except:
                    pass
            await reader.read_history(limit=limit)
        
        # Start monitoring for new messages
        logger.info("\n" + "="*60)
        logger.info("TELEGRAM SIGNAL READER - CLAUDE AI ENHANCED")
        logger.info("="*60)
        logger.info(f"Monitoring {len(MONITORED_CHANNELS)} channels")
        logger.info(f"Parser: Hybrid (Regex + Claude API)")
        logger.info(f"Message log directory: telegram_messages/")
        logger.info(f"Current log file: {reader.message_logger.log_file}")
        logger.info("Press Ctrl+C to stop")
        logger.info("="*60 + "\n")
        
        await reader.start()
        
    except KeyboardInterrupt:
        logger.info("\n[STOP] Telegram reader stopped by user")
        reader._print_stats()
    except Exception as e:
        logger.error(f"[ERROR] Fatal error: {e}", exc_info=True)


if __name__ == "__main__":
    """
    USAGE:
    
    1. Normal mode (monitor new messages):
       python telegram_reader_enhanced.py
    
    2. Read history first, then monitor:
       python telegram_reader_enhanced.py --history
    
    3. Read history with custom limit:
       python telegram_reader_enhanced.py --history --limit 200
    
    FEATURES:
    ✓ Logs ALL messages to telegram_messages/messages_YYYY-MM-DD.txt
    ✓ Each message includes: Channel name, ID, timestamp, full content
    ✓ Parses trading signals automatically
    ✓ Stores valid signals in SQLite database
    ✓ Shows statistics every 10 messages
    ✓ Detects duplicates automatically
    
    TROUBLESHOOTING:
    - Check telegram_messages/ folder for ALL received messages
    - Compare message text with signal_parser.py patterns
    - Check telegram_reader.log for detailed error logs
    - Use --history flag to process recent messages
    
    FILE STRUCTURE:
    telegram_messages/
      └── messages_2025-11-17.txt  (Daily log of ALL messages)
    telegram_reader.log             (Error and status log)
    trading.db                      (SQLite database)
    """
    asyncio.run(main())