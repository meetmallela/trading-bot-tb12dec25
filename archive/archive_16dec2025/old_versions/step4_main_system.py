"""
STEP 4: Main Trading System - FINAL FIXED VERSION
Run this ONLY after Steps 1-3 are complete and tested

Prerequisites:
- kite_token.txt exists (from Step 1)
- config/instruments_cache.csv exists (from Step 2)
- Parser tests passed (Step 3)
- config.yaml configured with Telegram credentials
"""

import yaml
import time
import signal
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from kiteconnect import KiteConnect

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class SimpleTradingSystem:
    """Simplified trading system"""
    
    def __init__(self):
        logger.info("="*80)
        logger.info("TELEGRAM TRADING SYSTEM v2.0 - SIMPLIFIED")
        logger.info("="*80)
        
        # Load configuration
        with open('config/config.yaml', 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize components
        self.kite = None
        self.instruments_df = None
        self.parsers = {}
        self.telegram_listener = None
        self.shutdown_flag = False
        
        # Setup CSV logging
        self.signals_log_file = "logs/parsed_signals.csv"
        self._setup_csv_logging()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown"""
        logger.info("Shutdown signal received")
        self.shutdown()
        sys.exit(0)
    
    def _setup_csv_logging(self):
        """Setup CSV file for logging parsed signals"""
        import os
        
        # Create logs directory
        os.makedirs("logs", exist_ok=True)
        
        # Create CSV file with headers if it doesn't exist
        if not Path(self.signals_log_file).exists():
            with open(self.signals_log_file, 'w') as f:
                f.write("timestamp,channel,underlying,strike,option_type,entry_price,stop_loss,targets,expiry_date,raw_message,parse_status\n")
            logger.info(f"Created signals log: {self.signals_log_file}")
    
    def _log_signal_to_csv(self, channel_name, signal, raw_message, parse_status):
        """Log parsed signal to CSV"""
        try:
            import csv
            from datetime import datetime
            
            with open(self.signals_log_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                if signal:
                    writer.writerow([
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        channel_name,
                        signal.underlying,
                        signal.strike,
                        signal.option_type,
                        signal.entry_price,
                        signal.stop_loss or "",
                        "|".join(map(str, signal.targets)) if signal.targets else "",
                        signal.expiry_date.strftime("%Y-%m-%d") if signal.expiry_date else "",
                        raw_message.replace('\n', ' ')[:200],
                        parse_status
                    ])
                else:
                    # Log failed parse
                    writer.writerow([
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        channel_name,
                        "", "", "", "", "", "", "",
                        raw_message.replace('\n', ' ')[:200],
                        parse_status
                    ])
        except Exception as e:
            logger.error(f"Error logging to CSV: {e}")
    
    def initialize(self):
        """Initialize all components"""
        try:
            logger.info("Initializing system components...")
            
            # 1. Load Kite credentials
            self._load_kite()
            
            # 2. Load instruments from cache
            self._load_instruments()
            
            # 3. Initialize parsers
            self._initialize_parsers()
            
            # 4. Initialize Telegram
            self._initialize_telegram()
            
            logger.info("All components initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def _load_kite(self):
        """Load Kite credentials from token file"""
        logger.info("Loading Kite credentials...")
        
        if not Path("kite_token.txt").exists():
            raise FileNotFoundError(
                "kite_token.txt not found! Please run step1_kite_login.py first"
            )
        
        # Read token file
        with open("kite_token.txt", "r") as f:
            lines = f.readlines()
            access_token = None
            api_key = None
            
            for line in lines:
                if line.startswith("ACCESS_TOKEN="):
                    access_token = line.split("=")[1].strip()
                elif line.startswith("API_KEY="):
                    api_key = line.split("=")[1].strip()
        
        if not api_key or not access_token:
            raise ValueError("Invalid kite_token.txt format")
        
        # Initialize Kite
        self.kite = KiteConnect(api_key=api_key)
        self.kite.set_access_token(access_token)
        
        logger.info("Kite API initialized")
    
    def _load_instruments(self):
        """Load instruments from cache"""
        logger.info("Loading instruments from cache...")
        
        cache_file = "config/instruments_cache.csv"
        
        if not Path(cache_file).exists():
            raise FileNotFoundError(
                f"{cache_file} not found! Please run step2_load_instruments.py first"
            )
        
        # Load from cache
        self.instruments_df = pd.read_csv(cache_file)
        
        # Convert expiry_date to datetime if present
        if 'expiry_date' in self.instruments_df.columns:
            self.instruments_df['expiry_date'] = pd.to_datetime(
                self.instruments_df['expiry_date'], 
                errors='coerce'
            )
        
        logger.info(f"Loaded {len(self.instruments_df)} instruments from cache")
    
    def _initialize_parsers(self):
        """Initialize channel parsers"""
        logger.info("Initializing parsers...")
        
        # Import parsers
        from parsers.parser_channel1 import Channel1Parser
        from parsers.parser_channel2 import Channel2Parser
        from parsers.parser_channel3 import Channel3Parser
        
        expiry_rules = self.config['expiry_rules']
        
        # Create parsers
        self.parsers = {
            'MCX_PREMIUM': Channel1Parser(expiry_rules),
            'VIP_RJ': Channel2Parser(expiry_rules),
            'RJ_STUDENT': Channel3Parser(expiry_rules)
        }
        
        logger.info(f"Initialized {len(self.parsers)} channel parsers")
        logger.info("NOTE: Claude fallback disabled (not needed)")
    
    def _initialize_telegram(self):
        """Initialize Telegram listener"""
        logger.info("Initializing Telegram listener...")
        
        from telethon import TelegramClient, events as telethon_events
        
        # Store events module for later use
        self.events = telethon_events
        
        tg_config = self.config['telegram']
        
        # Create client
        self.telegram_client = TelegramClient(
            'trading_session',
            tg_config['api_id'],
            tg_config['api_hash']
        )
        
        # Store channel mapping
        # Config has: 1002842854743
        # Telegram sends: -1002842854743 (with -100 prefix)
        self.channel_configs = {ch['id']: ch['name'] for ch in tg_config['channels']}
        
        logger.info(f"Configured channels: {self.channel_configs}")
        logger.info("Telegram listener ready")
    
    def handle_message(self, message: str, channel_id: int, timestamp: datetime):
        """Handle incoming Telegram message"""
        try:
            # Map channel ID to name
            # Telegram sends: -1002842854743
            # Config has: 1002842854743
            # Strip the -100 prefix: -1002842854743 -> 1002842854743
            
            channel_name = None
            lookup_id = channel_id
            
            if channel_id < 0:
                # Convert: -1002842854743 -> 1002842854743
                # Remove the minus sign and the first 3 digits (100)
                channel_id_str = str(abs(channel_id))  # "1002842854743"
                if channel_id_str.startswith("100"):
                    lookup_id = int(channel_id_str)  # 1002842854743
            
            # Now lookup
            if lookup_id in self.channel_configs:
                channel_name = self.channel_configs[lookup_id]
            
            if not channel_name:
                logger.warning(f"Unknown channel ID: {channel_id}")
                logger.info(f"After conversion: {lookup_id}")
                logger.info(f"Configured channels: {list(self.channel_configs.keys())}")
                return
            
            logger.info(f"[{channel_name}] Message received")
            
            # Get parser for this channel
            parser = self.parsers.get(channel_name)
            if not parser:
                logger.warning(f"No parser for channel: {channel_name}")
                return
            
            # Check for exit signal
            if parser.is_exit_signal(message):
                logger.info(f"[{channel_name}] EXIT signal detected")
                # TODO: Implement exit logic
                return
            
            # Parse message
            signal = parser.parse(message, timestamp)
            
            if signal:
                logger.info(
                    f"[{channel_name}] ✅ Parsed: {signal.underlying} "
                    f"{signal.strike} {signal.option_type} @ {signal.entry_price}"
                )
                
                # Log to CSV
                self._log_signal_to_csv(channel_name, signal, message, "SUCCESS")
                
                # TODO: Place order here
                mode = self.config['trading']['mode']
                if mode == 'test':
                    logger.info(f"[TEST MODE] Would place order for {signal.underlying}")
                else:
                    logger.info(f"[LIVE MODE] Placing order for {signal.underlying}")
                    # self._place_order(signal)
                
            else:
                logger.warning(f"[{channel_name}] ❌ Could not parse message")
                logger.debug(f"Message text: {message[:200]}")
                
                # Log failed parse to CSV
                self._log_signal_to_csv(channel_name, None, message, "FAILED")
                
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _telegram_handler(self, event):
        """Telegram event handler"""
        message_text = event.message.message
        channel_id = event.chat_id
        timestamp = datetime.now()
        
        if message_text:
            self.handle_message(message_text, channel_id, timestamp)
    
    async def start_telegram(self):
        """Start Telegram client"""
        await self.telegram_client.start(phone=self.config['telegram']['phone'])
        logger.info("Telegram client started")
        
        # Get all channel IDs (both formats)
        all_channel_ids = list(self.channel_configs.keys())
        # Also add -100 prefixed versions
        for ch_id in list(self.channel_configs.keys()):
            all_channel_ids.append(int(f"-100{ch_id}"))
        
        # Register handler for all possible channel ID formats
        @self.telegram_client.on(self.events.NewMessage(chats=all_channel_ids))
        async def message_handler(event):
            await self._telegram_handler(event)
        
        logger.info(f"Monitoring {len(self.channel_configs)} channels")
        logger.info("System is LIVE and monitoring channels")
        logger.info("Press Ctrl+C to shutdown gracefully")
        
        # Keep running
        await self.telegram_client.run_until_disconnected()
    
    def run(self):
        """Main run loop"""
        try:
            import asyncio
            
            # Show mode
            mode = self.config['trading']['mode']
            if mode == 'test':
                logger.info("="*80)
                logger.info("RUNNING IN TEST MODE - NO REAL ORDERS WILL BE PLACED")
                logger.info("="*80)
            
            # Start Telegram
            asyncio.run(self.start_telegram())
            
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Graceful shutdown"""
        if self.shutdown_flag:
            return
        
        self.shutdown_flag = True
        logger.info("="*80)
        logger.info("SHUTTING DOWN")
        logger.info("="*80)
        logger.info("System shutdown complete")


def main():
    """Main entry point"""
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║         TELEGRAM TRADING SYSTEM v2.0                         ║
    ║         Simplified & Tested                                  ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    # Check prerequisites
    if not Path("kite_token.txt").exists():
        print("\n❌ ERROR: kite_token.txt not found!")
        print("Please run step1_kite_login.py first\n")
        return
    
    if not Path("config/instruments_cache.csv").exists():
        print("\n❌ ERROR: config/instruments_cache.csv not found!")
        print("Please run step2_load_instruments.py first\n")
        return
    
    # Create system
    system = SimpleTradingSystem()
    
    # Initialize
    if not system.initialize():
        print("\n❌ Failed to initialize system")
        return
    
    # Run
    system.run()


if __name__ == "__main__":
    main()