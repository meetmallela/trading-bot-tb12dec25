"""
Get correct channel IDs from your Telegram account
Run this to get the right IDs for telegram_config.json
"""

import asyncio
from telethon import TelegramClient
import json

# Load config
with open('telegram_config.json', 'r') as f:
    config = json.load(f)

client = TelegramClient('trading_bot', config['api_id'], config['api_hash'])

async def main():
    await client.start(config['phone_number'])
    
    print("="*80)
    print("YOUR TELEGRAM CHANNELS")
    print("="*80)
    
    # Get all dialogs (channels you're in)
    async for dialog in client.iter_dialogs():
        if dialog.is_channel:
            # Get the correct channel ID format
            channel_id = str(dialog.id)
            
            # Fix the format: Telegram uses -100XXXXXXXXXX format
            if not channel_id.startswith('-100'):
                channel_id = f"-100{dialog.id}"
            
            print(f"\nChannel: {dialog.name}")
            print(f"ID: {channel_id}")
    
    print("\n" + "="*80)
    print("COPY THE IDs YOU WANT TO MONITOR")
    print("="*80)

if __name__ == '__main__':
    asyncio.run(main())
