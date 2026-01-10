"""
signal_parser_ultimate.py - Enhanced parser with instruments knowledge base
Improvements:
1. Loads instruments_cache.csv for validation
2. Loads parsing_rules_enhanced.json for custom rules
3. Better expiry date extraction and validation
4. Smarter ignoreable pattern matching
5. Detailed logging of parsing decisions
"""

import json
import logging
import os
import re
from datetime import datetime, timedelta
import csv

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InstrumentsKnowledgeBase:
    """Knowledge base of valid instruments from CSV"""
    
    def __init__(self, csv_paths=None):
        if csv_paths is None:
            csv_paths = [
                'instruments_cache.csv',
                'config/valid_instruments.csv'
            ]
        
        self.instruments = {}  # tradingsymbol -> details
        self.symbols = set()
        self.strikes_by_symbol = {}
        self.expiries_by_symbol = {}
        
        self._load_instruments(csv_paths)
    
    def _load_instruments(self, csv_paths):
        """Load instruments from CSV files"""
        total_loaded = 0
        
        for csv_path in csv_paths:
            if not os.path.exists(csv_path):
                logger.warning(f"Instruments file not found: {csv_path}")
                continue
            
            try:
                with open(csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    for row in reader:
                        if not row or len(row) < 2:
                            continue
                        
                        tradingsymbol = row[0]
                        
                        # Parse instrument details
                        # Format: NIFTY24DEC24200CE, GOLD26JAN136000CE
                        details = self._parse_tradingsymbol(tradingsymbol, row)
                        
                        if details:
                            self.instruments[tradingsymbol] = details
                            self.symbols.add(details['symbol'])
                            
                            # Track strikes by symbol
                            if details['symbol'] not in self.strikes_by_symbol:
                                self.strikes_by_symbol[details['symbol']] = set()
                            self.strikes_by_symbol[details['symbol']].add(details['strike'])
                            
                            # Track expiries by symbol
                            if details['symbol'] not in self.expiries_by_symbol:
                                self.expiries_by_symbol[details['symbol']] = set()
                            self.expiries_by_symbol[details['symbol']].add(details['expiry_date'])
                            
                            total_loaded += 1
                
                logger.info(f"Loaded {total_loaded} instruments from {csv_path}")
            
            except Exception as e:
                logger.error(f"Error loading {csv_path}: {e}")
        
        logger.info(f"Total instruments loaded: {total_loaded}")
        logger.info(f"Unique symbols: {len(self.symbols)}")
    
    def _parse_tradingsymbol(self, tradingsymbol, row):
        """Parse tradingsymbol to extract details"""
        try:
            # Try to extract symbol, expiry, strike, option_type
            # Format examples:
            # NIFTY24DEC24200CE -> NIFTY, 2024-12-XX, 24200, CE
            # GOLD26JAN136000PE -> GOLD, 2026-01-XX, 136000, PE
            
            # Find where CE or PE is
            option_type = None
            if tradingsymbol.endswith('CE'):
                option_type = 'CE'
                base = tradingsymbol[:-2]
            elif tradingsymbol.endswith('PE'):
                option_type = 'PE'
                base = tradingsymbol[:-2]
            else:
                return None
            
            # Extract strike (numbers at the end before CE/PE)
            strike_match = re.search(r'(\d+)$', base)
            if not strike_match:
                return None
            
            strike = int(strike_match.group(1))
            base_without_strike = base[:strike_match.start()]
            
            # Extract symbol (letters at start)
            symbol_match = re.match(r'^([A-Z]+)', base_without_strike)
            if not symbol_match:
                return None
            
            symbol = symbol_match.group(1)
            
            # Get expiry date from CSV if available
            expiry_date = row[3] if len(row) > 3 else None
            
            return {
                'tradingsymbol': tradingsymbol,
                'symbol': symbol,
                'strike': strike,
                'option_type': option_type,
                'expiry_date': expiry_date
            }
        
        except Exception as e:
            return None
    
    def find_instrument(self, symbol, strike, option_type, expiry_date=None):
        """Find matching instrument"""
        matches = []
        
        for ts, details in self.instruments.items():
            if (details['symbol'] == symbol and
                details['strike'] == strike and
                details['option_type'] == option_type):
                
                if expiry_date:
                    if details['expiry_date'] == expiry_date:
                        return details  # Exact match
                else:
                    matches.append(details)
        
        # Return nearest expiry if multiple matches
        if matches:
            matches.sort(key=lambda x: x.get('expiry_date', ''))
            return matches[0]
        
        return None
    
    def get_nearest_strike(self, symbol, target_strike):
        """Get nearest available strike"""
        if symbol not in self.strikes_by_symbol:
            return None
        
        strikes = list(self.strikes_by_symbol[symbol])
        strikes.sort()
        
        # Find closest
        closest = min(strikes, key=lambda x: abs(x - target_strike))
        return closest
    
    def get_available_expiries(self, symbol):
        """Get available expiry dates for symbol"""
        return sorted(list(self.expiries_by_symbol.get(symbol, [])))


class EnhancedIgnoreablePatterns:
    """Enhanced ignoreable pattern matching"""
    
    def __init__(self, rules_file='parsing_rules_enhanced.json'):
        self.patterns = self._load_patterns(rules_file)
    
    def _load_patterns(self, rules_file):
        """Load ignoreable patterns from rules file"""
        if not os.path.exists(rules_file):
            return self._get_default_patterns()
        
        try:
            with open(rules_file, 'r', encoding='utf-8') as f:
                rules = json.load(f)
            return rules.get('ignoreable_patterns', self._get_default_patterns())
        except:
            return self._get_default_patterns()
    
    def _get_default_patterns(self):
        """Default ignoreable patterns"""
        return {
            "exact_matches": [
                "Good morning", "Good evening", "Have a great day",
                "Happy trading", "All the best", "LEVEL ACTIVE"
            ],
            "contains_keywords": [
                "book profit", "exit", "close position", "square off",
                "book partial", "trail sl", "modify sl", "update sl",
                "++", "running", "hold all", "sl remove",
                "enjoy", "share your feedback", "profit screenshot"
            ],
            "regex_patterns": [
                r"^\d+\+*$",  # Just numbers with ++
                r"^[\d\s\+❤️🎁✔️😜💪🔥]+$",  # Numbers with emojis
                r"good morning|good evening|good night",
                r"happy.*trading",
                r"all.*best|best.*luck"
            ],
            "starts_with": [
                "Note:", "Disclaimer:", "Practice:", "Demo:", 
                "Test:", "Update:", "Modify:", "Share Your"
            ],
            "min_word_count": 3
        }
    
    def should_ignore(self, message):
        """Check if message should be ignored"""
        message_lower = message.lower().strip()
        message_upper = message.upper().strip()
        
        # Check exact matches
        for pattern in self.patterns.get('exact_matches', []):
            if message_lower == pattern.lower():
                logger.info(f"Ignored (exact match): {pattern}")
                return True
        
        # Check contains keywords
        for keyword in self.patterns.get('contains_keywords', []):
            if keyword.lower() in message_lower:
                logger.info(f"Ignored (contains): {keyword}")
                return True
        
        # Check regex patterns
        for pattern in self.patterns.get('regex_patterns', []):
            if re.search(pattern, message_lower):
                logger.info(f"Ignored (regex): {pattern}")
                return True
        
        # Check starts with
        for prefix in self.patterns.get('starts_with', []):
            if message.startswith(prefix):
                logger.info(f"Ignored (starts with): {prefix}")
                return True
        
        # Check minimum word count
        min_words = self.patterns.get('min_word_count', 0)
        if min_words > 0:
            words = message.split()
            if len(words) < min_words:
                logger.info(f"Ignored (too few words): {len(words)} < {min_words}")
                return True
        
        return False


# Logging configuration for analysis
PARSE_LOG_FILE = 'parsing_analysis.jsonl'

def log_parse_result(message, result, success, reason=''):
    """Log parsing result for analysis"""
    try:
        with open(PARSE_LOG_FILE, 'a', encoding='utf-8') as f:
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'message': message[:200],  # First 200 chars
                'result': result,
                'success': success,
                'reason': reason
            }
            f.write(json.dumps(log_entry) + '\n')
    except:
        pass


# Export for use
if __name__ == "__main__":
    # Test
    kb = InstrumentsKnowledgeBase()
    print(f"Loaded {len(kb.instruments)} instruments")
    print(f"Symbols: {sorted(list(kb.symbols))[:20]}")
    
    # Test find
    result = kb.find_instrument('GOLD', 136000, 'CE', '2026-01-27')
    if result:
        print(f"\nFound: {result['tradingsymbol']}")
    
    # Test ignoreable
    ignore = EnhancedIgnoreablePatterns()
    tests = [
        "162++❤️🎁✔️",
        "LEVEL ACTIVE 🔥🔥🔥",
        "BUY NIFTY 24200 CE @ 165 SL 150"
    ]
    
    for test in tests:
        should_ignore = ignore.should_ignore(test)
        print(f"\n'{test}': {'IGNORE' if should_ignore else 'PARSE'}")
