"""
Channel 2 Parser: VIP RJ Paid Education Purpose (ID: 1002842854743)
Handles consistent format with timestamps and "ABOVE" keyword
"""

import re
import logging
from datetime import datetime
from typing import Optional
from .base_parser import BaseParser, ParsedSignal

logger = logging.getLogger(__name__)


class Channel2Parser(BaseParser):
    """Parser for VIP RJ channel"""
    
    def __init__(self, expiry_rules: dict):
        super().__init__("VIP_RJ", expiry_rules)
    
    def parse(self, message: str, timestamp: datetime = None) -> Optional[ParsedSignal]:
        """
        Parse message from Channel 2
        
        Sample formats:
        1. [2025-12-08 08:43:15]
           **SENSEX 85200CE BUY :- 342 ABOVE 
           SL :- 330 TARGET :- 370/450/600++**
           
        2. [2025-12-08 06:46:11]
           **NIFTY 26200PE BUY :- 115/120
           SL :- 105 TARGET :- 138/158/178++**
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
            # Remove timestamp if present: [2025-12-08 08:43:15]
            message = re.sub(r'\[\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\]', '', message)
            
            # Remove ** markers
            message = message.replace('**', '')
            
            # Extract underlying, strike, and option type
            # This channel uses format: SENSEX 85200CE or NIFTY26200PE (no space)
            instrument_info = self.extract_underlying_and_strike(message)
            if not instrument_info:
                logger.warning(f"[{self.channel_name}] Could not extract instrument info")
                return None
            
            underlying, strike, option_type = instrument_info
            
            # Extract entry price - specific to this channel's format
            # "BUY :- 342 ABOVE" or "BUY :- 115/120"
            entry_price = self.extract_entry_price(message)
            if not entry_price:
                logger.warning(f"[{self.channel_name}] Could not extract entry price")
                return None
            
            # Extract stop loss
            stop_loss = self.extract_stop_loss(message)
            
            # Extract targets
            targets = self.extract_targets(message)
            
            # Expiry - usually not mentioned, use defaults
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
