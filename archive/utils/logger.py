"""
Logging System
Comprehensive logging for trading system
"""

import logging
import logging.handlers
from pathlib import Path
from datetime import datetime
import sys


class TradingLogger:
    """Centralized logging system"""
    
    def __init__(self, log_dir: str = "logs", level: str = "INFO"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.level = getattr(logging, level.upper())
        
        # Create loggers
        self.setup_loggers()
    
    def setup_loggers(self):
        """Setup all loggers"""
        
        # Main logger
        self.main_logger = self._create_logger(
            'main',
            self.log_dir / 'main.log',
            self.level
        )
        
        # Trade logger (all trades)
        self.trade_logger = self._create_logger(
            'trades',
            self.log_dir / 'trades.log',
            logging.INFO
        )
        
        # Error logger
        self.error_logger = self._create_logger(
            'errors',
            self.log_dir / 'errors.log',
            logging.ERROR
        )
        
        # Parser performance logger
        self.parser_logger = self._create_logger(
            'parser',
            self.log_dir / 'parser_performance.log',
            logging.INFO
        )
        
        # Order logger
        self.order_logger = self._create_logger(
            'orders',
            self.log_dir / 'orders.log',
            logging.INFO
        )
    
    def _create_logger(self, name: str, log_file: Path, level: int) -> logging.Logger:
        """Create a logger with file and console handlers"""
        
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.handlers.clear()  # Clear existing handlers
        
        # File handler with rotation
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(level)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        
        # Formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        console_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(message)s',
            datefmt='%H:%M:%S'
        )
        
        file_handler.setFormatter(detailed_formatter)
        console_handler.setFormatter(console_formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def log_trade(self, signal: dict, order_id: str = None, status: str = "PENDING"):
        """Log trade details"""
        msg = (
            f"TRADE | {signal.get('underlying')} {signal.get('strike')} {signal.get('option_type')} | "
            f"Entry: {signal.get('entry_price')} | SL: {signal.get('stop_loss')} | "
            f"Channel: {signal.get('channel')} | Order: {order_id} | Status: {status}"
        )
        self.trade_logger.info(msg)
    
    def log_order(self, action: str, instrument: str, quantity: int, price: float, 
                  order_id: str = None, status: str = "PLACED"):
        """Log order details"""
        msg = f"{action} | {instrument} | Qty: {quantity} | Price: {price} | Order: {order_id} | {status}"
        self.order_logger.info(msg)
    
    def log_error(self, error: str, context: dict = None):
        """Log error with context"""
        msg = f"ERROR | {error}"
        if context:
            msg += f" | Context: {context}"
        self.error_logger.error(msg)
        self.main_logger.error(msg)
    
    def log_parser_result(self, channel: str, success: bool, message: str = ""):
        """Log parser performance"""
        status = "SUCCESS" if success else "FAILED"
        msg = f"PARSER | {channel} | {status} | {message}"
        self.parser_logger.info(msg)
    
    def info(self, message: str):
        """General info log"""
        self.main_logger.info(message)
    
    def warning(self, message: str):
        """Warning log"""
        self.main_logger.warning(message)
    
    def error(self, message: str):
        """Error log"""
        self.main_logger.error(message)
        self.error_logger.error(message)
    
    def debug(self, message: str):
        """Debug log"""
        self.main_logger.debug(message)


# Global logger instance
_logger_instance = None


def get_logger(log_dir: str = "logs", level: str = "INFO") -> TradingLogger:
    """Get singleton logger instance"""
    global _logger_instance
    
    if _logger_instance is None:
        _logger_instance = TradingLogger(log_dir, level)
    
    return _logger_instance


def setup_logging(log_dir: str = "logs", level: str = "INFO"):
    """Setup logging system"""
    return get_logger(log_dir, level)
