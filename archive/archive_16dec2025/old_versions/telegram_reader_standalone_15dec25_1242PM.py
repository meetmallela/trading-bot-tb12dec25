"""
telegram_reader_standalone.py - Standalone version with all config inline
No external config files needed - everything is in this file
"""

import logging
import json
from telethon import TelegramClient, events
from datetime import datetime, timedelta
import asyncio
import os
from calendar import monthrange

# ==================== MCX EXPIRY RESOLUTION ====================

def resolve_mcx_monthly_expiry(symbol, month_text):
    """
    Resolve MCX monthly expiry date from month name like 'DECEMBER'
    Uses last working day of the month
    """
    month_map = {
        'JANUARY': 1, 'FEBRUARY': 2, 'MARCH': 3,
        'APRIL': 4, 'MAY': 5, 'JUNE': 6,
        'JULY': 7, 'AUGUST': 8, 'SEPTEMBER': 9,
        'OCTOBER': 10, 'NOVEMBER': 11, 'DECEMBER': 12
    }

    if not month_text:
        return None

    month = month_map.get(month_text.upper())
    if not month:
        return None

    today = datetime.now()
    year = today.year

    if month < today.month:
        year += 1

    last_day = monthrange(year, month)[1]
    expiry = datetime(year, month, last_day)

    while expiry.weekday() >= 5:
        expiry -= timedelta(days=1)

    return expiry.strftime('%Y-%m-%d')


# ==================== IMPORTS ====================

from signal_parser_enhanced_v2 import EnhancedSignalParser
from database import TradingDatabase

# ==================== CONFIGURATION ====================

CLAUDE_API_KEY = "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"

TELEGRAM_API_ID = 25677420
TELEGRAM_API_HASH = "3fe3d6d76fdffd005104a5df5db5ba6f"
TELEGRAM_PHONE = "+919833459174"

MONITORED_CHANNELS = [
    -1002498088029,
    -1002770917134,
    -1002842854743,
    -1003089362819,
    -1001903138387,
    -1002380215256,
    -1002568252699,
    -1002201480769,
    -1001294857397,
    -1002431924245,
    -1001389090145,
    -1001456128948,
    -1003282204738,
]

# ==================== LOGGING ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - TELEGRAM - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('telegram_reader.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== MESSAGE LOGGER ====================

class MessageLogger:
    def __init__(self, log_dir="telegram_messages"):
        os.makedirs(log_dir, exist_ok=True)
        today = datetime.now().strftime("%Y-%m-%d")
        self.log_file = os.path.join(log_dir, f"messages_{today}.txt")
        self.channel_names = {}

    def log_message(self, channel_id, channel_name, message_id, message_text, received_time):
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(f"""
================================================================================
MESSAGE RECEIVED: {received_time}
================================================================================
Channel ID: {channel_id}
Channel Name: {channel_name}
Message ID: {message_id}

{message_text}

================================================================================
""")


# ==================== TELEGRAM READER ====================

class TelegramSignalReader:
    def __init__(self, api_id, api_hash, phone, channels):
        self.client = TelegramClient('trading_session', api_id, api_hash)
        self.phone = phone
        self.channels = channels
        self.db = TradingDatabase()

        self.parser = EnhancedSignalParser(
            rules_file='parsing_rules_enhanced_v2.json',
            instruments_cache='instruments_cache.csv'
        )

        self.message_logger = MessageLogger()

        self.stats = {
            'total_messages': 0,
            'parsed_signals': 0,
            'stored_signals': 0,
            'duplicates': 0,
            'parsing_failures': 0
        }

    async def start(self):
        await self.client.start(phone=self.phone)

        for channel_id in self.channels:
            try:
                entity = await self.client.get_entity(channel_id)
                self.message_logger.channel_names[channel_id] = entity.title
            except:
                self.message_logger.channel_names[channel_id] = f"Unknown_{channel_id}"

        @self.client.on(events.NewMessage(chats=self.channels))
        async def handle_new_message(event):
            await self.process_message(event)

        await self.client.run_until_disconnected()

    async def process_message(self, event):
        self.stats['total_messages'] += 1

        message_text = event.message.message
        if not message_text:
            return

        message_id = str(event.message.id)
        channel_id = str(event.chat_id)
        received_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        channel_name = self.message_logger.channel_names.get(
            int(channel_id), f"Unknown_{channel_id}"
        )

        self.message_logger.log_message(
            channel_id, channel_name, message_id, message_text, received_time
        )

        parsed_signal = self.parser.parse(message_text, channel_id=channel_id)

        # ===== MCX EXPIRY RESOLUTION =====
        if parsed_signal and isinstance(parsed_signal, dict):
            symbol = parsed_signal.get('symbol')
            expiry_text = parsed_signal.get('expiry_text')

            mcx_symbols = ['GOLD', 'GOLDM', 'SILVER', 'SILVERM', 'CRUDEOIL', 'NATURALGAS']

            if symbol in mcx_symbols and expiry_text:
                resolved = resolve_mcx_monthly_expiry(symbol, expiry_text)
                if resolved:
                    parsed_signal['expiry_date'] = resolved
                    logger.info(f"[EXPIRY] {symbol} {expiry_text} → {resolved}")

            if symbol in ['NATURALGAS', 'CRUDEOIL'] and not parsed_signal.get('expiry_date'):
                fallback = {
                    'NATURALGAS': '2025-12-23',
                    'CRUDEOIL': '2025-12-19'
                }.get(symbol)

                if fallback:
                    parsed_signal['expiry_date'] = fallback
                    logger.info(f"[EXPIRY FALLBACK] {symbol} → {fallback}")

        if not parsed_signal:
            self.stats['parsing_failures'] += 1
            return

        self.stats['parsed_signals'] += 1

        signal_id = self.db.insert_signal(
            message_id=message_id,
            channel_id=channel_id,
            raw_text=message_text,
            parsed_data=parsed_signal
        )

        if signal_id:
            self.stats['stored_signals'] += 1
        else:
            self.stats['duplicates'] += 1

    def _print_stats(self):
        logger.info(json.dumps(self.stats, indent=2))


# ==================== MAIN ====================

async def main():
    reader = TelegramSignalReader(
        api_id=TELEGRAM_API_ID,
        api_hash=TELEGRAM_API_HASH,
        phone=TELEGRAM_PHONE,
        channels=MONITORED_CHANNELS
    )
    await reader.start()

if __name__ == "__main__":
    asyncio.run(main())
