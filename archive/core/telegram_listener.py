"""
Telegram Listener
Monitors Telegram channels for trading signals
"""

from telethon import TelegramClient, events
from typing import Dict, Callable
import asyncio
from datetime import datetime
from utils.logger import get_logger

logger = get_logger()


class TelegramListener:
    """Listens to Telegram channels and routes messages"""
    
    def __init__(self, config: Dict, message_handler: Callable):
        self.config = config
        self.message_handler = message_handler
        
        # Telegram config
        tg_config = config['telegram']
        self.api_id = tg_config['api_id']
        self.api_hash = tg_config['api_hash']
        self.phone = tg_config['phone']
        self.channels = {ch['id']: ch['name'] for ch in tg_config['channels']}
        
        # Create client
        self.client = TelegramClient('trading_session', self.api_id, self.api_hash)
        
        logger.info(f"Telegram listener initialized for {len(self.channels)} channels")
    
    async def start(self):
        """Start listening to channels"""
        try:
            await self.client.start(phone=self.phone)
            logger.info("Telegram client started successfully")
            
            # Register message handler
            @self.client.on(events.NewMessage(chats=list(self.channels.keys())))
            async def handle_new_message(event):
                await self._on_new_message(event)
            
            logger.info("Listening for messages...")
            logger.info(f"Monitoring channels: {list(self.channels.values())}")
            
            # Keep running
            await self.client.run_until_disconnected()
            
        except Exception as e:
            logger.error(f"Error starting Telegram listener: {e}")
            raise
    
    async def _on_new_message(self, event):
        """Handle new message from channel"""
        try:
            # Get message details
            message_text = event.message.message
            channel_id = event.chat_id
            channel_name = self.channels.get(channel_id, f"Unknown_{channel_id}")
            timestamp = datetime.now()
            
            if not message_text:
                return  # Skip empty messages
            
            logger.debug(f"Message from {channel_name}: {message_text[:100]}...")
            
            # Process message through handler (synchronously)
            self.message_handler(
                message=message_text,
                channel_id=channel_id,
                channel_name=channel_name,
                timestamp=timestamp
            )
            
        except Exception as e:
            logger.error(f"Error handling message: {e}")
    
    def run(self):
        """Run the listener (blocking)"""
        try:
            logger.info("Starting Telegram listener...")
            asyncio.run(self.start())
        except KeyboardInterrupt:
            logger.info("Telegram listener stopped by user")
        except Exception as e:
            logger.error(f"Telegram listener error: {e}")
            raise


class TelegramListenerThread:
    """Run Telegram listener in a separate thread"""
    
    def __init__(self, config: Dict, message_handler: Callable):
        self.listener = TelegramListener(config, message_handler)
        self.thread = None
    
    def start(self):
        """Start listener in background thread"""
        import threading
        
        self.thread = threading.Thread(target=self.listener.run, daemon=True)
        self.thread.start()
        logger.info("Telegram listener thread started")
    
    def stop(self):
        """Stop listener"""
        if self.thread and self.thread.is_alive():
            logger.info("Stopping Telegram listener...")
            # Telethon will stop when main program exits
    
    def is_alive(self) -> bool:
        """Check if listener thread is running"""
        return self.thread and self.thread.is_alive()
