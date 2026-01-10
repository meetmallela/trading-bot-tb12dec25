"""
signal_parser_enhanced_v2.py - Enhanced parser with channel-specific rules
Implements all requirements from telegram_messages_mannual_marking_14dec2025.csv
"""

import re
import json
import logging
from datetime import datetime, timedelta
import csv

class EnhancedSignalParser:
    """Enhanced parser with comprehensive rules"""
    
    def __init__(self, rules_file='parsing_rules_enhanced_v2.json', 
                 instruments_cache='instruments_cache.csv'):
        # Initialize logger first
        self.logger = logging.getLogger('PARSER')
        
        # Then load everything else
        self.rules = self.load_rules(rules_file)
        self.instruments_cache = self.load_instruments_cache(instruments_cache)
        
    def load_rules(self, rules_file):
        """Load parsing rules"""
        try:
            with open(rules_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load rules: {e}")
            return {}
    
    def load_instruments_cache(self, cache_file):
        """Load instruments cache for expiry calculation"""
        instruments = {}
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    symbol = row.get('tradingsymbol', '')
                    if symbol:
                        # Extract base symbol (NIFTY, BANKNIFTY, etc.)
                        base = symbol.split(str(datetime.now().year % 100))[0]
                        if base not in instruments:
                            instruments[base] = []
                        instruments[base].append({
                            'tradingsymbol': symbol,
                            'expiry': row.get('expiry', ''),
                            'strike': row.get('strike', ''),
                            'option_type': row.get('instrument_type', '')
                        })
            self.logger.info(f"Loaded {len(instruments)} instrument families")
        except Exception as e:
            self.logger.error(f"Failed to load instruments cache: {e}")
        return instruments
    
    def should_ignore(self, message, channel_id=None):
        """
        Check if message should be ignored
        Returns: (should_ignore: bool, reason: str)
        """
        if not message or not isinstance(message, str):
            return True, "Empty message"
        
        # Rule 9: Minimum length check
        if len(message) < self.rules.get('ignoreable_patterns', {}).get('minimum_length', 10):
            return True, "Less than 10 characters"
        
        ignore_rules = self.rules.get('ignoreable_patterns', {})
        
        # Check exact matches
        for exact in ignore_rules.get('exact_matches', []):
            if message.strip().upper() == exact.upper():
                return True, f"Exact match: {exact}"
        
        # Check contains keywords (Rules 6, 7, 8, 10, 11)
        for keyword in ignore_rules.get('contains_keywords', []):
            if keyword.lower() in message.lower():
                return True, f"Contains keyword: {keyword}"
        
        # Check regex patterns
        for pattern in ignore_rules.get('regex_patterns', []):
            if re.search(pattern, message, re.IGNORECASE):
                return True, f"Matches pattern: {pattern}"
        
        # Channel-specific ignore (Rule 1)
        if channel_id:
            channel_ignore = ignore_rules.get('channel_specific_ignore', {}).get(str(channel_id), {})
            for pattern in channel_ignore.get('ignore_patterns', []):
                if pattern.lower() in message.lower():
                    return True, f"Channel-specific ignore: {pattern}"
        
        return False, ""
    
    def detect_symbol_by_strike(self, strike_value):
        """
        Detect symbol based on strike range (Rules 3, 4)
        """
        if not strike_value or not str(strike_value).isdigit():
            return None
        
        strike = int(strike_value)
        ranges = self.rules.get('symbol_detection', {}).get('by_strike_range', {}).get('ranges', {})
        
        for symbol, range_data in ranges.items():
            if range_data['min'] <= strike <= range_data['max']:
                self.logger.info(f"[AUTO-DETECT] Strike {strike} → Symbol {symbol}")
                return symbol
        
        return None
    
    def calculate_expiry(self, symbol):
        """
        Calculate expiry date from instrument cache (Rule 2)
        """
        if not symbol:
            return None
        
        # Find next expiry for this symbol
        symbol_instruments = self.instruments_cache.get(symbol, [])
        if not symbol_instruments:
            self.logger.warning(f"No instruments found for {symbol}")
            return None
        
        # Get unique expiry dates
        expiry_dates = set()
        for inst in symbol_instruments:
            expiry = inst.get('expiry')
            if expiry:
                expiry_dates.add(expiry)
        
        # Sort and get next expiry
        sorted_expiries = sorted(expiry_dates)
        today = datetime.now().date()
        
        for expiry_str in sorted_expiries:
            try:
                expiry_date = datetime.strptime(expiry_str, '%Y-%m-%d').date()
                if expiry_date >= today:
                    self.logger.info(f"[EXPIRY-CALC] {symbol} → {expiry_str}")
                    return expiry_str
            except:
                continue
        
        # Fallback to first expiry if none in future
        if sorted_expiries:
            return sorted_expiries[0]
        
        return None
    
    def is_commodity_trade(self, message, channel_id=None):
        """
        Check if this is a commodity trade (Rule 5)
        """
        # Check commodity keywords
        for keyword in self.rules.get('commodity_detection', {}).get('keywords', []):
            if keyword in message.upper():
                return True
        
        # Check channel-specific rules
        if channel_id:
            channel_rules = self.rules.get('channel_specific_rules', {}).get(str(channel_id), {})
            if channel_rules.get('force_commodity'):
                for keyword in channel_rules.get('commodity_keywords', []):
                    if keyword in message.upper():
                        return True
        
        # Check commodity symbols
        for symbol in self.rules.get('commodity_detection', {}).get('symbols', []):
            if symbol in message.upper():
                return True
        
        return False
    
    def parse(self, message, channel_id=None):
        """
        Main parsing function with all rules applied
        
        Args:
            message: Message text
            channel_id: Telegram channel ID
            
        Returns:
            dict or None
        """
        # Step 1: Check if should ignore
        should_ignore, reason = self.should_ignore(message, channel_id)
        if should_ignore:
            self.logger.info(f"[SKIP] {reason}: {message[:50]}")
            return None
        
        # Step 2: Get channel-specific rules
        channel_rules = self.rules.get('channel_specific_rules', {}).get(str(channel_id), {})
        
        # Step 3: Basic extraction
        result = self.extract_basic_info(message)
        if not result:
            return None
        
        # Step 4: Detect symbol if missing (Rules 3, 4)
        if not result.get('symbol') and result.get('strike'):
            if channel_rules.get('auto_detect_symbol_by_strike', {}).get('enabled'):
                detected_symbol = self.detect_symbol_by_strike(result['strike'])
                if detected_symbol:
                    result['symbol'] = detected_symbol
                    result['symbol_auto_detected'] = True
        
        # Step 5: Calculate expiry if missing (Rule 2)
        if not result.get('expiry_date') and result.get('symbol'):
            if channel_rules.get('never_has_expiry') or channel_rules.get('expiry_calculation', {}).get('enabled'):
                calculated_expiry = self.calculate_expiry(result['symbol'])
                if calculated_expiry:
                    result['expiry_date'] = calculated_expiry
                    result['expiry_auto_calculated'] = True
        
        # Step 6: Mark as commodity if detected (Rule 5)
        if self.is_commodity_trade(message, channel_id):
            result['commodity'] = True
        
        return result
    
    def extract_basic_info(self, message):
        """Extract basic trading information"""
        result = {}
        
        # Extract action
        action_match = re.search(r'\b(BUY|SELL|EXIT|BOOK)\b', message, re.IGNORECASE)
        if action_match:
            result['action'] = action_match.group(1).upper()
        
        # Extract symbol
        # First check for known indices and commodities
        known_symbols = ['NIFTY', 'BANKNIFTY', 'SENSEX', 'FINNIFTY', 'MIDCPNIFTY',
                        'GOLD', 'GOLDM', 'SILVER', 'SILVERM', 'NATURALGAS', 
                        'CRUDEOIL', 'COPPER', 'ZINC', 'AMBER', 'LEAD', 'NICKEL',
                        'ALUMINUM', 'ALUMINIUM']
        
        for symbol in known_symbols:
            if symbol in message.upper():
                result['symbol'] = symbol
                break
        
        # If no known symbol found, try to extract stock symbol
        # Pattern: SYMBOLNAME followed by strike and CE/PE
        # Examples: EICHERMOT 7300CE, DIXON 14000 CE, RELIANCE 2500CE
        if not result.get('symbol'):
            stock_pattern = r'\b([A-Z]{2,20})\s*(\d{3,6})\s*(CE|PE)\b'
            stock_match = re.search(stock_pattern, message, re.IGNORECASE)
            if stock_match:
                result['symbol'] = stock_match.group(1).upper()
                result['strike'] = int(stock_match.group(2))
                result['option_type'] = stock_match.group(3).upper()
                result['is_stock_option'] = True
                self.logger.info(f"[STOCK] Detected stock option: {result['symbol']}")
        
        # Extract strike (if not already extracted from stock pattern)
        if not result.get('strike'):
            strike_match = re.search(r'\b(\d{5,6})\b', message)
            if strike_match:
                result['strike'] = int(strike_match.group(1))
        
        # Extract option type (if not already extracted)
        if not result.get('option_type'):
            option_match = re.search(r'\b(CE|PE)\b', message, re.IGNORECASE)
            if option_match:
                result['option_type'] = option_match.group(1).upper()
        
        # Extract prices
        # Entry price with multiple patterns
        entry_patterns = [
            r':-\s*(\d+\.?\d*)\s*ABOVE',  # BUY :- 140 ABOVE
            r'BUY\s*:-\s*(\d+\.?\d*)',    # BUY :- 140
            r'(?:NEAR|ABOVE|BELOW|CMP|@|LEVEL)\s*[-~:]?\s*(\d+\.?\d*)',
            r'(?:BUY|SELL)\s+(?:@|AT|NEAR)?\s*(\d+\.?\d*)',
            r'\bCE\s+(\d+\.?\d*)',
            r'\bPE\s+(\d+\.?\d*)'
        ]
        for pattern in entry_patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                result['entry_price'] = float(match.group(1))
                break
        
        # Stop loss with multiple patterns
        sl_patterns = [
            r'SL\s*:-\s*(\d+\.?\d*)',  # SL :- 130
            r'(?:SL|STOPLOSS|STOP LOSS)\s*[-:]?\s*(\d+\.?\d*)'
        ]
        for pattern in sl_patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                result['stop_loss'] = float(match.group(1))
                break
        
        # Targets with multiple patterns
        target_patterns = [
            r'TARGET\s*:-\s*([\d/,.\s]+)',  # TARGET :- 155/178/198
            r'(?:TARGET|TGT|T)\s*[-:]?\s*([\d/,.\s]+)'
        ]
        for pattern in target_patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                target_str = match.group(1)
                # Extract all numbers from target string
                target_nums = re.findall(r'\d+\.?\d*', target_str)
                if target_nums:
                    result['targets'] = [float(t) for t in target_nums[:5]]  # Max 5 targets
                    break
        
        # Need at least symbol or strike, and some price
        if (result.get('symbol') or result.get('strike')) and \
           (result.get('entry_price') or result.get('stop_loss')):
            return result
        
        return None


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    parser = EnhancedSignalParser()
    
    # Test cases
    test_messages = [
        # Should ignore
        ("LEVEL ACTIVE 🔥🔥🔥", None),
        ("160++❤️🎁✔️", None),
        ("Profit booked", None),
        ("Hi", None),  # Too short
        
        # Should parse - Channel -1003282204738
        ("59800 CE 380 SL 350", "-1003282204738"),
        ("25500 PE 120 SL 100", "-1003282204738"),
        
        # Should parse - Commodity
        ("COMMODITY MCX TRADE BUY GOLD 137000 CE NEAR 1330 SL 1300", "-1002770917134"),
        
        # Should parse - Regular
        ("SENSEX 85200ce Above 134 Sl 120 Target 150", None)
    ]
    
    print("="*80)
    print("TESTING ENHANCED PARSER")
    print("="*80)
    
    for i, (msg, channel) in enumerate(test_messages, 1):
        print(f"\n{i}. Message: {msg[:60]}")
        if channel:
            print(f"   Channel: {channel}")
        
        result = parser.parse(msg, channel_id=channel)
        if result:
            print(f"   ✓ PARSED: {json.dumps(result, indent=6)}")
        else:
            print(f"   ✗ IGNORED")
