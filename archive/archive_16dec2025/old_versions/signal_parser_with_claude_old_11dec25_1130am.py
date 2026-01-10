"""
signal_parser_with_claude.py - Enhanced Signal Parser with Claude API Fallback
Uses regex first (fast, free), falls back to Claude API with prompt caching
"""

import re
import json
import logging
from anthropic import Anthropic
from datetime import datetime

logger = logging.getLogger(__name__)


class SignalParserWithClaude:
    """
    Hybrid signal parser:
    1. Try regex parsing first (fast, free)
    2. If regex fails, use Claude API with cached KB (accurate, cheap)
    """
    
    def __init__(self, claude_api_key, instruments_csv_path=None):
        """
        Initialize parser with Claude API
        
        Args:
            claude_api_key: Your Anthropic API key
            instruments_csv_path: Path to valid instruments CSV (optional)
        """
        self.claude_client = Anthropic(api_key=claude_api_key)
        self.instruments = self._load_instruments(instruments_csv_path) if instruments_csv_path else []
        
        # Statistics
        self.stats = {
            'total': 0,
            'regex_success': 0,
            'claude_success': 0,
            'failed': 0
        }
        
        # Build cached system prompt (uploaded once, reused for all calls)
        self.cached_system_prompt = self._build_knowledge_base()
        
        logger.info("Signal parser initialized with Claude API fallback")
    
    
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
        """
        Build cached knowledge base for Claude API
        This gets cached and reused across all API calls (huge cost savings)
        """
        
        # Build instrument list text
        instruments_text = ""
        if self.instruments:
            instruments_text = f"\n\nVALID INSTRUMENTS:\n" + "\n".join(self.instruments[:100])  # Limit to 100 for token efficiency
        
        # Knowledge base with prompt caching
        kb = [
            {
                "type": "text",
                "text": """You are a trading signal parser for Indian equity derivatives markets.

Your task is to extract trading information from Telegram messages and return structured JSON.

SUPPORTED INSTRUMENTS:
- NIFTY (index options)
- BANKNIFTY (index options)
- FINNIFTY (index options)
- MIDCPNIFTY (index options)
- SENSEX (index options)
- BANKEX (index options)
- CRUDEOIL (commodity options)
- Individual stocks (RELIANCE, TCS, INFY, etc.)

OPTION TYPES:
- CE = Call Option
- PE = Put Option

COMMON PATTERNS:
- "BUY NIFTY 24200 CE @ 165" → Buy call at 165
- "SELL BANKNIFTY 48000 PE near 200" → Sell put around 200
- "ENTRY - 205" → Entry price 205
- "SL 150" or "STOPLOSS 150" → Stop loss at 150
- "TGT 180, 195" or "TARGET 180/195" → Targets 180 and 195

RULES:
1. Extract: action, symbol, strike, option_type, entry_price, stop_loss, targets
2. Return ONLY valid JSON, no explanations
3. If information is missing, use null
4. Targets should be an array of numbers""",
                "cache_control": {"type": "ephemeral"}  # CACHE THIS
            }
        ]
        
        # Add instruments if available (also cached)
        if instruments_text:
            kb.append({
                "type": "text",
                "text": instruments_text,
                "cache_control": {"type": "ephemeral"}  # CACHE THIS TOO
            })
        
        # Add parsing examples (cached)
        kb.append({
            "type": "text",
            "text": """EXAMPLES:

Input: "BUY NIFTY 24200 CE @ 165 SL 150 TGT 180, 195"
Output: {"action": "BUY", "symbol": "NIFTY", "strike": 24200, "option_type": "CE", "entry_price": 165, "stop_loss": 150, "targets": [180, 195]}

Input: "SELL BANKNIFTY 48000 PE NEAR LEVEL - 200 STOPLOSS - 180 TARGET - 220/240"
Output: {"action": "SELL", "symbol": "BANKNIFTY", "strike": 48000, "option_type": "PE", "entry_price": 200, "stop_loss": 180, "targets": [220, 240]}

Input: "BUY CRUDEOIL 5500 PE ENTRY 205 SL 195 TGT 255/270"
Output: {"action": "BUY", "symbol": "CRUDEOIL", "strike": 5500, "option_type": "PE", "entry_price": 205, "stop_loss": 195, "targets": [255, 270]}

Now parse the following signal:""",
            "cache_control": {"type": "ephemeral"}  # CACHE THIS TOO
        })
        
        return kb
    
    
    def parse_with_regex(self, message):
        """
        Try to parse signal using regex patterns (fast, free)
        Returns parsed dict or None if failed
        """
        try:
            message = message.upper().strip()
            
            # Extract action
            action = None
            if re.search(r'\bBUY\b', message):
                action = 'BUY'
            elif re.search(r'\bSELL\b', message):
                action = 'SELL'
            
            if not action:
                return None
            
            # Extract symbol
            symbol = None
            symbols = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX', 'CRUDEOIL']
            for sym in symbols:
                if sym in message:
                    symbol = sym
                    break
            
            if not symbol:
                return None
            
            # Extract strike price
            strike = None
            strike_match = re.search(r'(\d{4,5})\s*[CP]E', message)
            if strike_match:
                strike = int(strike_match.group(1))
            
            # Extract option type
            option_type = None
            if 'CE' in message:
                option_type = 'CE'
            elif 'PE' in message:
                option_type = 'PE'
            
            # Extract entry price
            entry_price = None
            entry_patterns = [
                r'@\s*(\d+\.?\d*)',
                r'ENTRY[:\s-]+(\d+\.?\d*)',
                r'NEAR[:\s]+(\d+\.?\d*)',
                r'LEVEL[:\s-]+(\d+\.?\d*)'
            ]
            for pattern in entry_patterns:
                match = re.search(pattern, message)
                if match:
                    entry_price = float(match.group(1))
                    break
            
            # Extract stop loss
            stop_loss = None
            sl_patterns = [
                r'SL[:\s-]+(\d+\.?\d*)',
                r'STOPLOSS[:\s-]+(\d+\.?\d*)'
            ]
            for pattern in sl_patterns:
                match = re.search(pattern, message)
                if match:
                    stop_loss = float(match.group(1))
                    break
            
            # Extract targets
            targets = []
            tgt_patterns = [
                r'TGT[:\s-]+([\d\s,/]+)',
                r'TARGET[:\s-]+([\d\s,/]+)'
            ]
            for pattern in tgt_patterns:
                match = re.search(pattern, message)
                if match:
                    tgt_str = match.group(1)
                    # Extract all numbers from target string
                    targets = [float(x) for x in re.findall(r'\d+\.?\d*', tgt_str)]
                    break
            
            # Build result
            parsed = {
                'action': action,
                'symbol': symbol,
                'strike': strike,
                'option_type': option_type,
                'entry_price': entry_price,
                'stop_loss': stop_loss,
                'targets': targets if targets else None
            }
            
            # Check if we have minimum required fields
            if action and symbol and entry_price:
                return parsed
            
            return None
            
        except Exception as e:
            logger.debug(f"Regex parsing failed: {e}")
            return None
    
    
    def _clean_text(self, text):
        """
        Remove emojis, special characters, and clean text for parsing
        
        Args:
            text: Raw message text
            
        Returns:
            Cleaned text with only alphanumeric, spaces, and basic punctuation
        """
        import re
        
        # Remove emojis and special Unicode characters
        # Keep only: letters, numbers, spaces, and basic punctuation (.,@:/-%)
        cleaned = re.sub(r'[^\w\s.,@:\/\-\%]', '', text, flags=re.UNICODE)
        
        # Remove multiple spaces
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        return cleaned
    
    
    def _is_worth_parsing(self, message):
        """
        Pre-filter to check if message is worth sending to Claude API
        Saves API costs by rejecting obvious non-signals
        
        Args:
            message: Message text
            
        Returns:
            True if worth parsing, False if obvious junk
        """
        # Clean first
        cleaned = self._clean_text(message.upper())
        
        # Minimum length check (signals are usually 20+ chars)
        if len(cleaned) < 15:
            logger.debug(f"[SKIP] Too short: {len(cleaned)} chars")
            return False
        
        # Check for signal keywords (at least ONE should be present)
        signal_keywords = [
            'BUY', 'SELL', 'CALL', 'PUT', 'CE', 'PE',
            'NIFTY', 'BANKNIFTY', 'FINNIFTY', 'SENSEX', 'CRUDEOIL',
            'ENTRY', 'TARGET', 'TGT', 'SL', 'STOPLOSS',
            '@', 'LEVEL', 'NEAR'
        ]
        
        has_keyword = any(keyword in cleaned for keyword in signal_keywords)
        
        if not has_keyword:
            logger.debug(f"[SKIP] No signal keywords found")
            return False
        
        # Check if it's just a price update (number followed by +/++ only)
        # Examples: "168++", "170++", "155+"
        if re.match(r'^\d+\+*\s*$', cleaned):
            logger.debug(f"[SKIP] Just a price update: {cleaned}")
            return False
        
        # Check for minimum word count (signals have multiple words)
        words = cleaned.split()
        if len(words) < 3:
            logger.debug(f"[SKIP] Too few words: {len(words)}")
            return False
        
        return True
    
    
    def parse_with_claude(self, message):
        """
        Parse signal using Claude API with cached knowledge base
        Returns parsed dict or None if failed
        """
        try:
            # Clean the text before sending to API
            cleaned_message = self._clean_text(message)
            
            logger.info(f"Using Claude API for: {cleaned_message[:50]}...")
            
            response = self.claude_client.messages.create(
                model="claude-haiku-4-5-20251001",  # Fastest, cheapest
                max_tokens=200,
                system=self.cached_system_prompt,  # Uses cached KB!
                messages=[{
                    "role": "user",
                    "content": cleaned_message  # Use cleaned text!
                }]
            )
            
            # Log token usage
            usage = response.usage
            logger.info(f"Claude API tokens - Input: {usage.input_tokens}, Output: {usage.output_tokens}")
            
            # Extract JSON from response
            response_text = response.content[0].text.strip()
            
            # Handle markdown code blocks
            if "```json" in response_text:
                json_str = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                json_str = response_text.split("```")[1].split("```")[0].strip()
            else:
                json_str = response_text
            
            # Parse JSON
            parsed = json.loads(json_str)
            
            logger.info(f"Claude API success: {parsed}")
            return parsed
            
        except Exception as e:
            logger.error(f"Claude API parsing failed: {e}")
            return None
    
    
    def parse(self, message, channel_id=None):
        """
        Main parsing method - tries regex first, then Claude API
        
        Args:
            message: Telegram message text
            channel_id: Optional channel identifier for logging
            
        Returns:
            dict: Parsed signal or None if failed
        """
        self.stats['total'] += 1
        
        # Try regex first (fast, free)
        parsed = self.parse_with_regex(message)
        
        if parsed:
            self.stats['regex_success'] += 1
            logger.info(f"[REGEX] Parsed successfully")
            return parsed
        
        # Pre-filter: Check if worth sending to Claude API
        if not self._is_worth_parsing(message):
            self.stats['failed'] += 1
            logger.debug(f"[SKIP] Not worth parsing with API: {message[:50]}...")
            return None
        
        # Fallback to Claude API (only for valid-looking signals)
        logger.info(f"[FALLBACK] Regex failed, trying Claude API...")
        parsed = self.parse_with_claude(message)
        
        if parsed:
            self.stats['claude_success'] += 1
            logger.info(f"[CLAUDE] Parsed successfully")
            return parsed
        
        # Both failed
        self.stats['failed'] += 1
        logger.warning(f"[FAILED] Could not parse: {message}")
        return None
    
    
    def get_stats(self):
        """Return parsing statistics"""
        total = self.stats['total']
        if total == 0:
            return self.stats
        
        return {
            **self.stats,
            'regex_rate': f"{(self.stats['regex_success'] / total * 100):.1f}%",
            'claude_rate': f"{(self.stats['claude_success'] / total * 100):.1f}%",
            'success_rate': f"{((self.stats['regex_success'] + self.stats['claude_success']) / total * 100):.1f}%"
        }


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Initialize parser
    API_KEY = "your-api-key-here"  # Replace with your key
    parser = SignalParserWithClaude(
        claude_api_key=API_KEY,
        instruments_csv_path="instruments.csv"  # Optional
    )
    
    # Test signals
    test_signals = [
        "BUY NIFTY 24200 CE @ 165 SL 150 TGT 180, 195",
        "SELL BANKNIFTY 48000 PE NEAR LEVEL - 200 TARGET - 220/240 STOPLOSS - 180",
        "BUY CRUDEOIL 5500 PE NEAR LEVEL - 205 TARGET - 255/270 STOPLOSS - 195 EXPIRY - DECEMBER",
        "NIFTY CE 24500 buy around 150, targets 170/190, sl 140",
    ]
    
    print("=" * 70)
    print("TESTING SIGNAL PARSER WITH CLAUDE FALLBACK")
    print("=" * 70)
    
    for i, signal in enumerate(test_signals, 1):
        print(f"\n[TEST {i}] {signal}")
        print("-" * 70)
        
        result = parser.parse(signal)
        
        if result:
            print(json.dumps(result, indent=2))
        else:
            print("❌ Parsing failed")
    
    # Show statistics
    print("\n" + "=" * 70)
    print("STATISTICS")
    print("=" * 70)
    stats = parser.get_stats()
    for key, value in stats.items():
        print(f"{key}: {value}")