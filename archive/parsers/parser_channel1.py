"""
Channel 1 Parser: MCX PREMIUM (ID: 1002770917134)
Handles format variations in MCX Premium channel
"""

import re
import logging
from datetime import datetime
from typing import Optional
from .base_parser import BaseParser, ParsedSignal

logger = logging.getLogger(__name__)


class Channel1Parser(BaseParser):
    """Parser for MCX PREMIUM channel"""
    
    def __init__(self, expiry_rules: dict):
        super().__init__("MCX_PREMIUM", expiry_rules)
    
    def parse(self, message: str, timestamp: datetime = None) -> Optional[ParsedSignal]:
        """
        Parse message from Channel 1
        
        Sample formats:
        1. BUY - NIFTY  26000 PE
           NEAR LEVEL --   115
           TARGET -  160 / 170/200
           STOPLOSS -- 100
           EXPIRY -  09 DECEMBER
           
        2. [2025-12-08 06:27:58]
           INDIGO    4700 PE
           ABOVE  120
           TARGET 150,170
           SL 102
           #STOCK_OPTIONS
           
        3. **COMMODITY MCX TRADE
           BUY GOLD   136000 CE
           NEAR LEVEL -  800
           TARGET -830/850/900
           STOPLOSS -   770
           EXPIRY - DECEMBER**
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        # Check for exit or update signals
        if self.is_exit_signal(message):
            logger.info(f"[{self.channel_name}] EXIT signal detected")
            return None
        
        if self.is_update_signal(message):
            logger.debug(f"[{self.channel_name}] Update message, ignoring")
            return None
        
        try:
            # Extract underlying, strike, and option type
            instrument_info = self.extract_underlying_and_strike(message)
            if not instrument_info:
                logger.warning(f"[{self.channel_name}] Could not extract instrument info")
                return None
            
            underlying, strike, option_type = instrument_info
            
            # Extract entry price
            entry_price = self.extract_entry_price(message)
            if not entry_price:
                logger.warning(f"[{self.channel_name}] Could not extract entry price")
                return None
            
            # Extract stop loss
            stop_loss = self.extract_stop_loss(message)
            
            # Extract targets
            targets = self.extract_targets(message)
            
            # Extract or calculate expiry
            expiry_date = self.extract_expiry_date(message, underlying)
            
            # Create parsed signal
            signal = ParsedSignal(
                underlying=underlying,
                strike=strike,
                option_type=option_type,
                entry_price=entry_price,
                stop_loss=stop_loss,
                targets=targets,
                expiry_date=expiry_date,
                raw_message=message,
                channel=self.channel_name,
                timestamp=timestamp
            )
            
            # Validate
            if not self.validate_signal(signal):
                return None
            
            logger.info(f"[{self.channel_name}] Parsed: {underlying} {strike} {option_type} @ {entry_price}")
            return signal
            
        except Exception as e:
            logger.error(f"[{self.channel_name}] Parse error: {e}")
            return None
