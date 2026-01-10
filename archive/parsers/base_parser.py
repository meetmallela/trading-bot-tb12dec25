"""
Base Parser Class
Common parsing logic shared across all channel parsers
"""

import re
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ParsedSignal:
    """Parsed trading signal"""
    underlying: str  # NIFTY, SENSEX, INDIGO, etc
    strike: float
    option_type: str  # CE or PE
    entry_price: float
    stop_loss: Optional[float]
    targets: List[float]
    expiry_date: Optional[datetime]
    raw_message: str
    channel: str
    timestamp: datetime
    
    def to_dict(self) -> Dict:
        return {
            'underlying': self.underlying,
            'strike': self.strike,
            'option_type': self.option_type,
            'entry_price': self.entry_price,
            'stop_loss': self.stop_loss,
            'targets': self.targets,
            'expiry_date': self.expiry_date.strftime('%Y-%m-%d') if self.expiry_date else None,
            'raw_message': self.raw_message,
            'channel': self.channel,
            'timestamp': self.timestamp.isoformat()
        }


class BaseParser:
    """Base class for all channel parsers"""
    
    # Common patterns
    STRIKE_PATTERN = r'(\d{4,6})\s*(CE|PE|ce|pe)'
    PRICE_PATTERN = r'(\d+(?:\.\d+)?)'
    TARGET_PATTERN = r'TARGET\s*[:-]?\s*([0-9,/\s+]+)'
    SL_PATTERN = r'(?:SL|STOPLOSS)\s*[:-]?\s*(\d+(?:\.\d+)?)'
    
    # Expiry patterns
    EXPIRY_DATE_PATTERN = r'(?:EXPIRY|expiry)\s*[:-]?\s*(\d{1,2})\s*(?:DECEMBER|DEC|december|dec)'
    EXPIRY_MONTH_PATTERN = r'(?:DECEMBER|DEC|december|dec)'
    
    def __init__(self, channel_name: str, expiry_rules: Dict):
        self.channel_name = channel_name
        self.expiry_rules = expiry_rules
    
    def parse(self, message: str, timestamp: datetime = None) -> Optional[ParsedSignal]:
        """
        Parse message - to be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement parse()")
    
    def is_exit_signal(self, message: str) -> bool:
        """Check if message is an exit signal"""
        exit_keywords = [
            'exit', 'EXIT', 'CLOSE', 'close', 'square off', 'SQUARE OFF',
            'book profit', 'BOOK PROFIT', 'safe exit', 'SAFE EXIT'
        ]
        message_upper = message.upper()
        return any(keyword.upper() in message_upper for keyword in exit_keywords)
    
    def is_update_signal(self, message: str) -> bool:
        """Check if message is just an update (not actionable)"""
        update_keywords = [
            'target achieved', 'TARGET ACHIEVED', 'running', 'RUNNING',
            'back to back', 'BACK TO BACK', 'fire', 'FIRE', '🎯'
        ]
        message_lower = message.lower()
        return any(keyword.lower() in message_lower for keyword in update_keywords)
    
    def extract_underlying_and_strike(self, message: str) -> Optional[tuple]:
        """
        Extract underlying instrument and strike price
        Returns: (underlying, strike, option_type) or None
        """
        # Pattern: NIFTY 26000 PE, SENSEX 85200CE, INDIGO DEC 4700PE, etc
        patterns = [
            r'([A-Z]+)\s+(\d{4,6})\s*(CE|PE)',  # NIFTY 26000 PE
            r'([A-Z]+)\s+(?:DEC|DECEMBER)\s+(\d{4,6})\s*(CE|PE)',  # INDIGO DEC 4700 PE
            r'([A-Z]+)\s*(\d{4,6})(CE|PE)',  # SENSEX85200CE
        ]
        
        for pattern in patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                underlying = match.group(1).upper()
                strike = float(match.group(2))
                option_type = match.group(3).upper()
                return (underlying, strike, option_type)
        
        return None
    
    def extract_entry_price(self, message: str) -> Optional[float]:
        """
        Extract entry price from message
        Handles: "NEAR LEVEL -- 115", "ABOVE 120", "BUY :- 342 ABOVE", "115/120"
        """
        # Pattern 1: "NEAR LEVEL -- 115"
        match = re.search(r'NEAR\s+LEVEL\s*[:-]*\s*(\d+(?:\.\d+)?)', message, re.IGNORECASE)
        if match:
            return float(match.group(1))
        
        # Pattern 2: "ABOVE 120" or "BUY :- 342 ABOVE"
        match = re.search(r'(?:BUY\s*[:-]?\s*)?(\d+(?:\.\d+)?)\s+ABOVE', message, re.IGNORECASE)
        if match:
            return float(match.group(1))
        
        # Pattern 3: "BUY :- 115/120" (range - take lower)
        match = re.search(r'BUY\s*[:-]?\s*(\d+(?:\.\d+)?)/(\d+(?:\.\d+)?)', message, re.IGNORECASE)
        if match:
            return float(match.group(1))  # Take lower price
        
        # Pattern 4: Just a number after BUY
        match = re.search(r'BUY\s*[:-]?\s*(\d+(?:\.\d+)?)', message, re.IGNORECASE)
        if match:
            return float(match.group(1))
        
        return None
    
    def extract_stop_loss(self, message: str) -> Optional[float]:
        """Extract stop-loss from message"""
        match = re.search(self.SL_PATTERN, message, re.IGNORECASE)
        if match:
            return float(match.group(1))
        return None
    
    def extract_targets(self, message: str) -> List[float]:
        """Extract target prices from message"""
        match = re.search(self.TARGET_PATTERN, message, re.IGNORECASE)
        if not match:
            return []
        
        target_str = match.group(1)
        # Remove ++ and other special chars
        target_str = target_str.replace('++', '').strip()
        
        # Extract all numbers
        numbers = re.findall(r'\d+(?:\.\d+)?', target_str)
        return [float(n) for n in numbers]
    
    def extract_expiry_date(self, message: str, underlying: str) -> Optional[datetime]:
        """
        Extract or calculate expiry date
        """
        # Check for explicit date: "EXPIRY - 09 DECEMBER"
        match = re.search(self.EXPIRY_DATE_PATTERN, message, re.IGNORECASE)
        if match:
            day = int(match.group(1))
            # Assume current year and month
            now = datetime.now()
            return datetime(now.year, 12, day)  # December
        
        # Check for month mention only: "DEC" or "DECEMBER"
        if re.search(self.EXPIRY_MONTH_PATTERN, message, re.IGNORECASE):
            return self._get_monthly_expiry(underlying)
        
        # No expiry mentioned - use default rules
        return self._get_default_expiry(underlying)
    
    def _get_default_expiry(self, underlying: str) -> datetime:
        """Get default expiry based on instrument and rules"""
        underlying_upper = underlying.upper()
        
        # Check expiry rules
        if underlying_upper in self.expiry_rules:
            rule = self.expiry_rules[underlying_upper]
            if rule['type'] == 'weekly':
                return self._get_weekly_expiry(rule['day'])
            else:
                return self._get_monthly_expiry(underlying_upper)
        else:
            # Default: monthly expiry
            return self._get_monthly_expiry(underlying_upper)
    
    def _get_weekly_expiry(self, day_name: str) -> datetime:
        """Get current week's expiry (Thursday for NIFTY, Friday for SENSEX)"""
        now = datetime.now()
        
        # Map day names to weekday numbers (Monday=0, Sunday=6)
        day_map = {
            'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 
            'Thursday': 3, 'Friday': 4, 'Saturday': 5, 'Sunday': 6
        }
        
        target_day = day_map[day_name]
        current_day = now.weekday()
        
        # Calculate days until target day
        if current_day <= target_day:
            days_ahead = target_day - current_day
        else:
            # Next week
            days_ahead = 7 - current_day + target_day
        
        expiry_date = now + timedelta(days=days_ahead)
        return expiry_date.replace(hour=0, minute=0, second=0, microsecond=0)
    
    def _get_monthly_expiry(self, underlying: str) -> datetime:
        """Get month-end expiry (last Thursday/Friday of month)"""
        now = datetime.now()
        
        # Get last day of current month
        if now.month == 12:
            last_day = datetime(now.year + 1, 1, 1) - timedelta(days=1)
        else:
            last_day = datetime(now.year, now.month + 1, 1) - timedelta(days=1)
        
        # Find last Thursday (for most instruments)
        while last_day.weekday() != 3:  # 3 = Thursday
            last_day -= timedelta(days=1)
        
        return last_day.replace(hour=0, minute=0, second=0, microsecond=0)
    
    def round_to_tick(self, price: float, tick_size: float = 0.05) -> float:
        """Round price to nearest tick size"""
        return round(price / tick_size) * tick_size
    
    def validate_signal(self, signal: ParsedSignal) -> bool:
        """Validate parsed signal has all required fields"""
        if not signal.underlying:
            logger.warning("Signal missing underlying")
            return False
        if not signal.strike or signal.strike <= 0:
            logger.warning("Signal missing or invalid strike")
            return False
        if not signal.option_type or signal.option_type not in ['CE', 'PE']:
            logger.warning("Signal missing or invalid option type")
            return False
        if not signal.entry_price or signal.entry_price <= 0:
            logger.warning("Signal missing or invalid entry price")
            return False
        
        return True
