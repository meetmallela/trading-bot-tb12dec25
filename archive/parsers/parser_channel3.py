"""
Channel 3 Parser: RJ - STUDENT PRACTICE CALLS (ID: 1002498088029)
Similar format to Channel 2 with explicit month mention
"""

import re
import logging
from datetime import datetime
from typing import Optional
from .base_parser import BaseParser, ParsedSignal

logger = logging.getLogger(__name__)


class Channel3Parser(BaseParser):
    """Parser for RJ STUDENT channel"""
    
    def __init__(self, expiry_rules: dict):
        super().__init__("RJ_STUDENT", expiry_rules)
    
    def parse(self, message: str, timestamp: datetime = None) -> Optional[ParsedSignal]:
        """
        Parse message from Channel 3
        
        Sample formats:
        1. **INDIGO DEC 4700PE BUY :- 135 ABOVE 
           SL :- 122 TARGET :- 148/178/198++**
           
        2. **HEROMOTOCO 6200PE BUY :- 112 ABOVE 
           SL :- 100 TARGET :- 122/138/158++**
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
            # Remove ** markers
            message = message.replace('**', '')
            
            # Extract underlying, strike, and option type
            # Format: INDIGO DEC 4700PE or HEROMOTOCO 6200PE
            instrument_info = self.extract_underlying_and_strike(message)
            if not instrument_info:
                logger.warning(f"[{self.channel_name}] Could not extract instrument info")
                return None
            
            underlying, strike, option_type = instrument_info
            
            # Extract entry price
            # Format: "BUY :- 135 ABOVE"
            entry_price = self.extract_entry_price(message)
            if not entry_price:
                logger.warning(f"[{self.channel_name}] Could not extract entry price")
                return None
            
            # Extract stop loss
            stop_loss = self.extract_stop_loss(message)
            
            # Extract targets
            targets = self.extract_targets(message)
            
            # Expiry - check for "DEC" mention
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
