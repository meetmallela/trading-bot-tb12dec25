"""
signal_parser_enhanced.py - Full-Featured Signal Parser
Features:
- Ignoreable patterns
- Channel-specific rules
- Strike price inference (symbol detection from strike range)
- Expiry calculation for channels that don't provide it
- Custom rules in JSON file
"""

import re
import json
import logging
from anthropic import Anthropic
from datetime import datetime, timedelta
import os

logger = logging.getLogger(__name__)


def calculate_next_expiry(symbol, current_date=None):
    """
    Calculate next expiry date for a given symbol
    
    Args:
        symbol: Index symbol (NIFTY, BANKNIFTY, etc.)
        current_date: Reference date (defaults to today)
    
    Returns:
        datetime object for next expiry
    """
    if current_date is None:
        current_date = datetime.now()
    
    # Expiry days mapping
    expiry_days = {
        'NIFTY': 3,      # Thursday
        'BANKNIFTY': 2,  # Wednesday
        'FINNIFTY': 1,   # Tuesday
        'SENSEX': 4,     # Friday (monthly - last Friday)
        'BANKEX': 4,     # Friday (monthly - last Friday)
        'MIDCPNIFTY': 0  # Monday (monthly - last Monday)
    }
    
    if symbol not in expiry_days:
        logger.warning(f"Unknown symbol for expiry calculation: {symbol}")
        return None
    
    target_weekday = expiry_days[symbol]
    
    # For weekly expiries (NIFTY, BANKNIFTY, FINNIFTY)
    if symbol in ['NIFTY', 'BANKNIFTY', 'FINNIFTY']:
        # Calculate days until next target weekday
        days_ahead = target_weekday - current_date.weekday()
        if days_ahead <= 0:  # Target day already happened this week
            days_ahead += 7
        
        expiry_date = current_date + timedelta(days=days_ahead)
        return expiry_date
    
    # For monthly expiries (SENSEX, BANKEX, MIDCPNIFTY)
    else:
        # Find last occurrence of target weekday in current month
        # Start from last day of month and work backwards
        year = current_date.year
        month = current_date.month
        
        # Get last day of month
        if month == 12:
            last_day = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            last_day = datetime(year, month + 1, 1) - timedelta(days=1)
        
        # Find last occurrence of target weekday
        while last_day.weekday() != target_weekday:
            last_day -= timedelta(days=1)
        
        # If that day has passed, use next month
        if last_day < current_date:
            if month == 12:
                next_month = datetime(year + 1, 1, 1)
            else:
                next_month = datetime(year, month + 1, 1)
            
            return calculate_next_expiry(symbol, next_month)
        
        return last_day


class ParsingRules:
    """Manages custom parsing rules including channel-specific settings"""
    
    def __init__(self, rules_file='parsing_rules_enhanced.json'):
        self.rules_file = rules_file
        self.rules = self._load_rules()
    
    def _load_rules(self):
        """Load rules from file or use defaults"""
        if os.path.exists(self.rules_file):
            try:
                with open(self.rules_file, 'r', encoding='utf-8') as f:
                    rules = json.load(f)
                logger.info(f"Loaded enhanced rules from {self.rules_file}")
                return rules
            except Exception as e:
                logger.warning(f"Could not load rules file: {e}, using defaults")
        
        logger.warning(f"Rules file not found: {self.rules_file}")
        return {}
    
    def should_ignore_message(self, message):
        """Check if message should be completely ignored"""
        
        if 'ignoreable_patterns' not in self.rules:
            return False
        
        patterns = self.rules['ignoreable_patterns']
        message_lower = message.lower().strip()
        message_upper = message.upper().strip()
        
        # Check exact matches
        if 'exact_matches' in patterns:
            for exact in patterns['exact_matches']:
                if message_lower == exact.lower():
                    logger.debug(f"Ignoring: exact match '{exact}'")
                    return True
        
        # Check contains keywords
        if 'contains_keywords' in patterns:
            for keyword in patterns['contains_keywords']:
                if keyword.lower() in message_lower:
                    logger.debug(f"Ignoring: contains '{keyword}'")
                    return True
        
        # Check starts with
        if 'starts_with' in patterns:
            for prefix in patterns['starts_with']:
                if message.startswith(prefix) or message_lower.startswith(prefix.lower()):
                    logger.debug(f"Ignoring: starts with '{prefix}'")
                    return True
        
        # Check regex patterns
        if 'regex_patterns' in patterns:
            for pattern in patterns['regex_patterns']:
                if re.search(pattern, message_lower, re.IGNORECASE):
                    logger.debug(f"Ignoring: matches pattern '{pattern}'")
                    return True
        
        # Check emoji only
        if patterns.get('emoji_only', False):
            # Remove emojis and check if anything left
            cleaned = re.sub(r'[^\w\s]', '', message, flags=re.UNICODE)
            if not cleaned.strip():
                logger.debug("Ignoring: emoji only message")
                return True
        
        # Check numbers only (like "170++")
        if patterns.get('numbers_only', False):
            if re.match(r'^[\d\s\+\-\.]+$', message.strip()):
                logger.debug("Ignoring: numbers only message")
                return True
        
        # Check minimum word count
        min_words = patterns.get('min_word_count', 0)
        if min_words > 0:
            words = message.split()
            if len(words) < min_words:
                logger.debug(f"Ignoring: too few words ({len(words)} < {min_words})")
                return True
        
        return False
    
    def infer_symbol_from_strike(self, strike, channel_id=None):
        """
        Infer symbol from strike price based on ranges
        
        Args:
            strike: Strike price
            channel_id: Channel ID (for channel-specific rules)
        
        Returns:
            Inferred symbol or None
        """
        # Check channel-specific rules first
        if channel_id and 'channel_specific_rules' in self.rules:
            channel_rules = self.rules['channel_specific_rules'].get(str(channel_id), {})
            if channel_rules.get('infer_symbol_from_strike', False):
                ranges = channel_rules.get('strike_ranges', {})
                for symbol, (min_strike, max_strike) in ranges.items():
                    if min_strike <= strike <= max_strike:
                        logger.info(f"Inferred symbol {symbol} from strike {strike} (channel-specific)")
                        return symbol
        
        # Check global strike price inference
        if 'strike_price_inference' in self.rules and self.rules['strike_price_inference'].get('enabled', False):
            ranges = self.rules['strike_price_inference'].get('ranges', {})
            for symbol, range_data in ranges.items():
                min_strike = range_data.get('min', 0)
                max_strike = range_data.get('max', 999999)
                if min_strike <= strike <= max_strike:
                    logger.info(f"Inferred symbol {symbol} from strike {strike} (global rule)")
                    return symbol
        
        return None
    
    def should_calculate_expiry(self, symbol, channel_id=None):
        """Check if expiry should be calculated for this channel/symbol"""
        if not channel_id:
            return False
        
        if 'channel_specific_rules' not in self.rules:
            return False
        
        channel_rules = self.rules['channel_specific_rules'].get(str(channel_id), {})
        
        if not channel_rules.get('no_expiry_date', False):
            return False
        
        calc_expiry = channel_rules.get('calculate_expiry', {})
        if not calc_expiry.get('enabled', False):
            return False
        
        # Check if symbol is in the list of indices
        indices = calc_expiry.get('indices', [])
        return symbol in indices
    
    def get_rules_text(self):
        """Convert rules to text format for Claude API"""
        text_parts = []
        
        # General rules
        if 'general_rules' in self.rules:
            text_parts.append("GENERAL RULES:")
            for i, rule in enumerate(self.rules['general_rules'], 1):
                text_parts.append(f"{i}. {rule}")
        
        # Symbol rules
        if 'symbol_rules' in self.rules:
            text_parts.append("\nSYMBOL RULES:")
            for rule in self.rules['symbol_rules']:
                text_parts.append(f"- {rule}")
        
        # Strike price inference
        if 'strike_price_inference' in self.rules:
            text_parts.append("\nSTRIKE PRICE INFERENCE:")
            text_parts.append("- If symbol is missing, infer from strike price:")
            ranges = self.rules['strike_price_inference'].get('ranges', {})
            for symbol, range_data in ranges.items():
                min_val = range_data.get('min')
                max_val = range_data.get('max')
                text_parts.append(f"  * {symbol}: {min_val}-{max_val}")
        
        # Price rules
        if 'price_rules' in self.rules:
            text_parts.append("\nPRICE EXTRACTION RULES:")
            for rule in self.rules['price_rules']:
                text_parts.append(f"- {rule}")
        
        # Action rules
        if 'action_rules' in self.rules:
            text_parts.append("\nACTION RULES:")
            for rule in self.rules['action_rules']:
                text_parts.append(f"- {rule}")
        
        # Special cases
        if 'special_cases' in self.rules:
            text_parts.append("\nSPECIAL CASES:")
            for rule in self.rules['special_cases']:
                text_parts.append(f"- {rule}")
        
        # Validation rules
        if 'validation_rules' in self.rules:
            text_parts.append("\nVALIDATION RULES:")
            for rule in self.rules['validation_rules']:
                text_parts.append(f"- {rule}")
        
        return "\n".join(text_parts)


class SignalParserEnhanced:
    """
    Enhanced signal parser with full rule support
    """
    
    def __init__(self, claude_api_key, instruments_csv_path=None, rules_file='parsing_rules_enhanced.json'):
        self.claude_client = Anthropic(api_key=claude_api_key)
        self.instruments = self._load_instruments(instruments_csv_path) if instruments_csv_path else []
        self.rules = ParsingRules(rules_file)
        
        # Statistics
        self.stats = {
            'total': 0,
            'regex_success': 0,
            'claude_success': 0,
            'failed': 0,
            'ignored': 0,
            'symbol_inferred': 0
        }
        
        # Build cached system prompt
        self.cached_system_prompt = self._build_knowledge_base()
        
        logger.info("Enhanced signal parser initialized")
    
    def _load_instruments(self, csv_path):
        """Load valid instruments from CSV"""
        try:
            with open(csv_path, 'r') as f:
                instruments = [line.strip() for line in f if line.strip()]
            logger.info(f"Loaded {len(instruments)} valid instruments")
            return instruments
        except Exception as e:
            logger.warning(f"Could not load instruments CSV: {e}")
            return []
    
    def _build_knowledge_base(self):
        """Build cached knowledge base for Claude API"""
        kb = [
            {
                "type": "text",
                "text": """You are a trading signal parser for Indian equity derivatives markets.

SUPPORTED INSTRUMENTS:
- NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY (index options)
- SENSEX, BANKEX (BSE index options)
- CRUDEOIL, NATURALGAS, GOLD, SILVER, COPPER, ZINC (commodity options)
- Individual stocks (MARUTI, RELIANCE, TCS, etc.)

SYMBOL NORMALIZATION:
- "NATURAL GAS" or "NATURALGAS" → Use "NATURALGAS"
- "CRUDE OIL" or "CRUDEOIL" → Use "CRUDEOIL"
- Remove spaces from commodity names

OPTION TYPES:
- CE = Call Option, PE = Put Option

OUTPUT FORMAT:
Return ONLY valid JSON:
{
  "action": "BUY or SELL",
  "symbol": "NIFTY, BANKNIFTY, MARUTI, NATURALGAS, etc.",
  "strike": number,
  "option_type": "CE or PE",
  "entry_price": number,
  "stop_loss": number,
  "targets": [array of numbers]
}""",
                "cache_control": {"type": "ephemeral"}
            }
        ]
        
        # Add rules text if available
        rules_text = self.rules.get_rules_text()
        if rules_text and rules_text.strip():  # Only add if not empty
            kb.append({
                "type": "text",
                "text": rules_text,
                "cache_control": {"type": "ephemeral"}
            })
        
        if self.instruments:
            kb.append({
                "type": "text",
                "text": "\n\nVALID INSTRUMENTS:\n" + "\n".join(self.instruments[:100]),
                "cache_control": {"type": "ephemeral"}
            })
        
        kb.append({
            "type": "text",
            "text": """EXAMPLES:

Input: "BUY NIFTY 24200 CE @ 165 SL 150 TGT 180, 195"
Output: {"action": "BUY", "symbol": "NIFTY", "strike": 24200, "option_type": "CE", "entry_price": 165, "stop_loss": 150, "targets": [180, 195]}

Input: "BUY 25500 CE @ 160 SL 150"
Output: {"action": "BUY", "symbol": "NIFTY", "strike": 25500, "option_type": "CE", "entry_price": 160, "stop_loss": 150, "targets": null}

Input: "MARUTI 16600CE BUY :- 201 ABOVE SL :- 190 TARGET :- 218/248/298"
Output: {"action": "BUY", "symbol": "MARUTI", "strike": 16600, "option_type": "CE", "entry_price": 201, "stop_loss": 190, "targets": [218, 248, 298]}

Input: "NATURAL GAS 370 PE ABOVE 13 TARGET 25,28 SL 11"
Output: {"action": "BUY", "symbol": "NATURALGAS", "strike": 370, "option_type": "PE", "entry_price": 13, "stop_loss": 11, "targets": [25, 28]}

Now parse the following signal:""",
            "cache_control": {"type": "ephemeral"}
        })
        
        return kb
    
    def _clean_text(self, text):
        """Remove emojis and special characters"""
        cleaned = re.sub(r'[^\w\s.,@:\/\-\%]', '', text, flags=re.UNICODE)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        return cleaned
    
    def parse_with_regex(self, message, channel_id=None):
        """Parse with regex, applying channel-specific rules"""
        try:
            message_upper = message.upper().strip()
            
            # Extract action
            action = None
            for keyword in ['BUY', 'SELL', 'LONG', 'SHORT']:
                if keyword in message_upper:
                    action = 'BUY' if keyword in ['BUY', 'LONG'] else 'SELL'
                    break
            
            if not action:
                return None
            
            # Extract symbol
            symbol = None
            symbols = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX', 'CRUDEOIL']
            for sym in symbols:
                if sym in message_upper:
                    symbol = sym
                    break
            
            # Extract strike price
            strike = None
            strike_match = re.search(r'(\d{4,6})\s*[CP]E', message_upper)
            if strike_match:
                strike = int(strike_match.group(1))
            
            # If symbol missing but strike found, try to infer
            if not symbol and strike:
                symbol = self.rules.infer_symbol_from_strike(strike, channel_id)
                if symbol:
                    self.stats['symbol_inferred'] += 1
            
            if not symbol:
                return None
            
            # Extract option type
            option_type = 'CE' if 'CE' in message_upper else 'PE' if 'PE' in message_upper else None
            
            # Extract entry price
            entry_price = None
            entry_patterns = [
                r'@\s*(\d+\.?\d*)',
                r'ENTRY[:\s-]+(\d+\.?\d*)',
                r'NEAR[:\s]+(\d+\.?\d*)',
                r'LEVEL[:\s-]+(\d+\.?\d*)',
                r'CMP[:\s-]+(\d+\.?\d*)',
                r'LTP[:\s-]+(\d+\.?\d*)'
            ]
            for pattern in entry_patterns:
                match = re.search(pattern, message_upper)
                if match:
                    entry_price = float(match.group(1))
                    break
            
            # Extract stop loss
            stop_loss = None
            sl_patterns = [
                r'SL[:\s-]+(\d+\.?\d*)',
                r'STOPLOSS[:\s-]+(\d+\.?\d*)',
                r'STOP\s+LOSS[:\s-]+(\d+\.?\d*)'
            ]
            for pattern in sl_patterns:
                match = re.search(pattern, message_upper)
                if match:
                    stop_loss = float(match.group(1))
                    break
            
            # Extract targets
            targets = []
            tgt_patterns = [
                r'TGT[:\s-]+([\d\s,/]+)',
                r'TARGET[:\s-]+([\d\s,/]+)',
                r'TP[:\s-]+([\d\s,/]+)'
            ]
            for pattern in tgt_patterns:
                match = re.search(pattern, message_upper)
                if match:
                    tgt_str = match.group(1)
                    targets = [float(x) for x in re.findall(r'\d+\.?\d*', tgt_str)]
                    break
            
            parsed = {
                'action': action,
                'symbol': symbol,
                'strike': strike,
                'option_type': option_type,
                'entry_price': entry_price,
                'stop_loss': stop_loss,
                'targets': targets if targets else None,
                'channel_id': channel_id
            }
            
            if action and symbol and entry_price:
                return parsed
            
            return None
            
        except Exception as e:
            logger.debug(f"Regex parsing failed: {e}")
            return None
    
    def parse_with_claude(self, message, channel_id=None):
        """Parse with Claude API"""
        try:
            cleaned_message = self._clean_text(message)
            logger.info(f"Using Claude API for: {cleaned_message[:50]}...")
            
            response = self.claude_client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=200,
                system=self.cached_system_prompt,
                messages=[{"role": "user", "content": cleaned_message}]
            )
            
            usage = response.usage
            logger.info(f"Claude API tokens - Input: {usage.input_tokens}, Output: {usage.output_tokens}")
            
            response_text = response.content[0].text.strip()
            
            if "```json" in response_text:
                json_str = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                json_str = response_text.split("```")[1].split("```")[0].strip()
            else:
                json_str = response_text
            
            parsed = json.loads(json_str)
            
            # If symbol missing, try to infer from strike
            if not parsed.get('symbol') and parsed.get('strike'):
                inferred_symbol = self.rules.infer_symbol_from_strike(parsed['strike'], channel_id)
                if inferred_symbol:
                    parsed['symbol'] = inferred_symbol
                    self.stats['symbol_inferred'] += 1
            
            parsed['channel_id'] = channel_id
            
            logger.info(f"Claude API success: {parsed}")
            return parsed
            
        except Exception as e:
            logger.error(f"Claude API parsing failed: {e}")
            return None
    
    def parse(self, message, channel_id=None):
        """Main parsing method"""
        self.stats['total'] += 1
        
        # Check if message should be ignored
        if self.rules.should_ignore_message(message):
            self.stats['ignored'] += 1
            logger.debug(f"[IGNORED] Message filtered by rules")
            return None
        
        # Try regex first
        parsed = self.parse_with_regex(message, channel_id)
        
        if parsed:
            self.stats['regex_success'] += 1
            logger.info(f"[REGEX] Parsed successfully")
            return parsed
        
        # Fallback to Claude API
        logger.info(f"[FALLBACK] Regex failed, trying Claude API...")
        parsed = self.parse_with_claude(message, channel_id)
        
        if parsed:
            self.stats['claude_success'] += 1
            logger.info(f"[CLAUDE] Parsed successfully")
            return parsed
        
        self.stats['failed'] += 1
        logger.warning(f"[FAILED] Could not parse: {message[:50]}...")
        return None
    
    def get_stats(self):
        """Return statistics"""
        total = self.stats['total']
        if total == 0:
            return self.stats
        
        return {
            **self.stats,
            'regex_rate': f"{(self.stats['regex_success'] / total * 100):.1f}%",
            'claude_rate': f"{(self.stats['claude_success'] / total * 100):.1f}%",
            'success_rate': f"{((self.stats['regex_success'] + self.stats['claude_success']) / total * 100):.1f}%",
            'ignored_rate': f"{(self.stats['ignored'] / total * 100):.1f}%",
            'inference_rate': f"{(self.stats['symbol_inferred'] / max(self.stats['regex_success'] + self.stats['claude_success'], 1) * 100):.1f}%"
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
    
    API_KEY = "your-api-key-here"
    parser = SignalParserEnhanced(claude_api_key=API_KEY)
    
    # Test cases
    tests = [
        ("BUY 25500 CE @ 160 SL 150", "-1003282204738"),  # Missing symbol, should infer NIFTY
        ("SELL 58000 PE ENTRY 200", "-1003089362819"),  # Missing symbol, should infer BANKNIFTY
        ("Good morning all", None),  # Should be ignored
        ("170++", None),  # Should be ignored
        ("BUY NIFTY 24200 CE @ 165", None),  # Standard format
    ]
    
    print("\n" + "="*80)
    print("TESTING ENHANCED PARSER")
    print("="*80)
    
    for msg, channel in tests:
        print(f"\nTest: {msg}")
        print(f"Channel: {channel}")
        result = parser.parse(msg, channel_id=channel)
        print(f"Result: {result}")
    
    print("\n" + "="*80)
    print("STATISTICS")
    print("="*80)
    stats = parser.get_stats()
    for key, value in stats.items():
        print(f"{key}: {value}")


# Alias for backward compatibility
SignalParserWithClaude = SignalParserEnhanced
