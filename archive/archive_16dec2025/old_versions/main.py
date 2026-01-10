"""
Main Entry Point
Telegram Trading System
"""

import yaml
import time
import signal
import sys
from pathlib import Path
from datetime import datetime, time as dt_time

# Import our modules
from utils.logger import setup_logging, get_logger
from trading.kite_integration import get_kite
from trading.instrument_downloader import get_instrument_downloader
from trading.order_manager import get_order_manager
from core.telegram_listener import TelegramListenerThread
from parsers import Channel1Parser, Channel2Parser, Channel3Parser, ClaudeFallbackParser


class TradingSystem:
    """Main trading system orchestrator"""
    
    def __init__(self, config_file: str = "config/config.yaml"):
        # Load configuration
        with open(config_file, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Setup logging
        log_config = self.config['logging']
        self.logger = setup_logging(
            log_dir=log_config['log_dir'],
            level=log_config['level']
        )
        
        self.logger.info("=" * 80)
        self.logger.info("TELEGRAM TRADING SYSTEM STARTING")
        self.logger.info("=" * 80)
        
        # Initialize components
        self.kite = None
        self.instrument_downloader = None
        self.order_manager = None
        self.parsers = {}
        self.claude_parser = None
        self.telegram_listener = None
        
        # Shutdown flag
        self.shutdown_flag = False
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info("Shutdown signal received")
        self.shutdown()
        sys.exit(0)
    
    def initialize(self):
        """Initialize all system components"""
        try:
            self.logger.info("Initializing system components...")
            
            # 1. Initialize Kite API
            self._initialize_kite()
            
            # 2. Download/Load instruments
            self._load_instruments()
            
            # 3. Initialize parsers
            self._initialize_parsers()
            
            # 4. Initialize order manager
            self._initialize_order_manager()
            
            # 5. Initialize Telegram listener
            self._initialize_telegram()
            
            self.logger.info("All components initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Initialization failed: {e}")
            return False
    
    def _initialize_kite(self):
        """Initialize Kite Connect API"""
        self.logger.info("Initializing Kite API...")
        
        kite_config = self.config['kite']
        api_key = kite_config['api_key']
        api_secret = kite_config['api_secret']
        
        # Check if credentials are filled
        if api_key == "YOUR_KITE_API_KEY":
            raise ValueError("Please update Kite API credentials in config.yaml")
        
        # Initialize Kite (access token will be set later)
        from trading.kite_integration import KiteIntegration, _kite_instance
        global _kite_instance
        _kite_instance = KiteIntegration(api_key, api_secret)
        self.kite = _kite_instance
        
        # Get access token
        self._get_kite_access_token()
        
        self.logger.info("Kite API initialized")
    
    def _get_kite_access_token(self):
        """Get Kite access token"""
        # For now, show login URL
        login_url = self.kite.get_login_url()
        
        print("\n" + "=" * 80)
        print("KITE LOGIN REQUIRED")
        print("=" * 80)
        print(f"\n1. Open this URL in your browser:\n{login_url}\n")
        print("2. Login and authorize")
        print("3. Copy the 'request_token' from the redirect URL")
        print("=" * 80)
        
        request_token = input("\nEnter request token: ").strip()
        
        if not request_token:
            raise ValueError("Request token is required")
        
        # Generate session
        access_token = self.kite.generate_session(request_token)
        self.logger.info("Kite session established")
        
        # Save access token to file for today
        with open(".kite_token", "w") as f:
            f.write(f"{datetime.now().date()}\n{access_token}")
    
    def _load_instruments(self):
        """Load or download instruments"""
        self.logger.info("Loading instruments...")
        
        inst_config = self.config['instruments']
        cache_file = inst_config['cache_file']
        
        # Initialize downloader
    def _initialize_parsers(self):
        """Initialize channel parsers"""
        self.logger.info("Initializing parsers...")
        
        expiry_rules = self.config['expiry_rules']
        
        # Create channel-specific parsers
        self.parsers = {
            'MCX_PREMIUM': Channel1Parser(expiry_rules),
            'VIP_RJ': Channel2Parser(expiry_rules),
            'RJ_STUDENT': Channel3Parser(expiry_rules)
        }
        
        # Claude fallback parser - TEMPORARILY DISABLED TO AVOID LIBRARY ISSUE
        self.claude_parser = None
        self.logger.warning("Claude API fallback temporarily disabled - regex parsers only")
        
        self.logger.info(f"Initialized {len(self.parsers)} channel parsers")
        # Create channel-specific parsers
        self.parsers = {
            'MCX_PREMIUM': Channel1Parser(expiry_rules),
            'VIP_RJ': Channel2Parser(expiry_rules),
            'RJ_STUDENT': Channel3Parser(expiry_rules)
        }
        
        # Create Claude fallback parser
        claude_config = self.config['claude']
        if claude_config['api_key'] != "YOUR_ANTHROPIC_API_KEY":
            instruments_list = self.instrument_downloader.instruments_df.to_dict('records') if self.instrument_downloader.instruments_df is not None else []
            
            self.claude_parser = ClaudeFallbackParser(
                api_key=claude_config['api_key'],
                model=claude_config['model'],
                timeout=claude_config['timeout'],
                instruments_list=instruments_list
            )
            self.logger.info("Claude fallback parser enabled")
        else:
            self.logger.warning("Claude API key not configured - fallback disabled")
        
        self.logger.info(f"Initialized {len(self.parsers)} channel parsers")
    
    def _initialize_order_manager(self):
        """Initialize order manager"""
        self.logger.info("Initializing order manager...")
        
        from trading.order_manager import OrderManager, _order_manager_instance
        global _order_manager_instance
        _order_manager_instance = OrderManager(self.config)
        self.order_manager = _order_manager_instance
        
        mode = self.config['trading']['mode']
        self.logger.info(f"Order manager initialized in {mode.upper()} mode")
        
        if mode == 'test':
            self.logger.warning("=" * 80)
            self.logger.warning("RUNNING IN TEST MODE - NO REAL ORDERS WILL BE PLACED")
            self.logger.warning("=" * 80)
    
    def _initialize_telegram(self):
        """Initialize Telegram listener"""
        self.logger.info("Initializing Telegram listener...")
        
        self.telegram_listener = TelegramListenerThread(
            config=self.config,
            message_handler=self.handle_message
        )
        
        self.logger.info("Telegram listener ready")
    
    def handle_message(self, message: str, channel_id: int, channel_name: str, timestamp: datetime):
        """
        Handle incoming Telegram message
        This is the core message processing pipeline
        """
        try:
            self.logger.info(f"[{channel_name}] Processing message...")
            
            # Get appropriate parser
            parser = self.parsers.get(channel_name)
            
            if not parser:
                self.logger.warning(f"No parser for channel: {channel_name}")
                return
            
            # Try parsing with channel-specific parser
            signal = parser.parse(message, timestamp)
            
            # If failed and Claude parser available, try fallback
            if not signal and self.claude_parser:
                self.logger.info(f"[{channel_name}] Regex failed, trying Claude API...")
                signal = self.claude_parser.parse(message, channel_name, timestamp)
            
            # Log parser result
            self.logger.log_parser_result(
                channel=channel_name,
                success=signal is not None,
                message=f"{signal.underlying} {signal.strike} {signal.option_type}" if signal else "Parse failed"
            )
            
            # If we have a valid signal, process it
            if signal:
                signal_dict = signal.to_dict()
                
                # Check if exit signal
                if parser.is_exit_signal(message):
                    self.logger.info(f"[{channel_name}] EXIT signal received")
                    self.order_manager.exit_all_positions(reason=f"EXIT_{channel_name}")
                    return
                
                # Process buy signal
                self.logger.info(f"[{channel_name}] Valid signal: {signal.underlying} {signal.strike} {signal.option_type} @ {signal.entry_price}")
                
                # Check max positions limit
                max_positions = self.config['trading']['max_positions']
                if self.order_manager.get_active_positions_count() >= max_positions:
                    self.logger.warning(f"Max positions ({max_positions}) reached, skipping signal")
                    return
                
                # Process the signal (place orders)
                success = self.order_manager.process_signal(signal_dict)
                
                if success:
                    self.logger.info(f"[{channel_name}] Signal processed successfully")
                else:
                    self.logger.error(f"[{channel_name}] Failed to process signal")
            else:
                self.logger.warning(f"[{channel_name}] Could not parse message")
                
        except Exception as e:
            self.logger.error(f"Error handling message: {e}")
    
    def run(self):
        """Main run loop"""
        try:
            self.logger.info("Starting trading system...")
            
            # Start Telegram listener
            self.telegram_listener.start()
            
            # Wait for listener to be ready
            time.sleep(3)
            
            if not self.telegram_listener.is_alive():
                raise Exception("Telegram listener failed to start")
            
            self.logger.info("System is LIVE and monitoring channels")
            self.logger.info("Press Ctrl+C to shutdown gracefully")
            
            # Main loop - update trailing stops
            check_interval = self.config['trading']['trailing_check_interval']
            
            while not self.shutdown_flag:
                try:
                    # Update trailing stops every 30 seconds
                    self.order_manager.update_trailing_stops()
                    
                    # Sleep
                    time.sleep(check_interval)
                    
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    self.logger.error(f"Error in main loop: {e}")
                    time.sleep(5)
            
        except KeyboardInterrupt:
            self.logger.info("Interrupted by user")
        except Exception as e:
            self.logger.error(f"Fatal error: {e}")
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Graceful shutdown"""
        if self.shutdown_flag:
            return
        
        self.shutdown_flag = True
        
        self.logger.info("=" * 80)
        self.logger.info("SHUTTING DOWN TRADING SYSTEM")
        self.logger.info("=" * 80)
        
        # Stop Telegram listener
        if self.telegram_listener:
            self.telegram_listener.stop()
        
        # Close all positions (optional - comment out if you want to keep positions)
        # if self.order_manager:
        #     self.order_manager.exit_all_positions(reason="SYSTEM_SHUTDOWN")
        
        self.logger.info("System shutdown complete")


def main():
    """Main entry point"""
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║         TELEGRAM TRADING SYSTEM v1.0                         ║
    ║         Automated Options Trading Bot                        ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    # Create system
    system = TradingSystem()
    
    # Initialize
    if not system.initialize():
        print("Failed to initialize system. Check logs for details.")
        sys.exit(1)
    
    # Run
    system.run()


if __name__ == "__main__":
    main()
