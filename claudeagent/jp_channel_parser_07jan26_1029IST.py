"""
jp_channel_parser.py
Specialized parser for channel -1003282204738 (JP Paper trade)
Handles both index options (NIFTY/BANKNIFTY/SENSEX) and stock options
"""

import re
import logging
from datetime import datetime, timedelta
from anthropic import Anthropic

class JPChannelParser:
    """Parser specifically designed for JP Paper trade channel"""
    
    # Known index symbols
    INDEX_SYMBOLS = {
        'NIFTY': {'min_strike': 24000, 'max_strike': 28000},
        'BANKNIFTY': {'min_strike': 56000, 'max_strike': 62000},
        'SENSEX': {'min_strike': 82000, 'max_strike': 86000}
    }
    
    # Skip patterns (non-trading messages)
    SKIP_PATTERNS = [
        r'wait for',
        r'hold below',
        r'pass\s*🔥',
        r'^\d+\.?\d*\s*high',  # Just "123 high"
        r'^\d+\s*$',  # Just a number
        r'make or brake',
        r'revised level',
        r'back to',
        r'eye again',
        r'still trading',
        r'profit booking',
        r'new level',
        r'night\s*\n',  # Matka numbers
        r'bazar\s*\n',
        r'december month',
        r'whatsapp',
        r'renewed',
    ]
    
    def __init__(self, claude_api_key=None, instruments_csv=None, rulebook_path=None):
        self.logger = logging.getLogger('JP_PARSER')
        self.claude_client = Anthropic(api_key=claude_api_key) if claude_api_key else None
        
        # Load channel rulebook
        self.rulebook = self._load_rulebook(rulebook_path) if rulebook_path else None
        
        # Load valid instruments with caching
        self.valid_instruments = self._load_instruments_cached(instruments_csv) if instruments_csv else {}
        
        self.logger.info("[INIT] JP Channel Parser initialized")
    
    def _load_rulebook(self, rulebook_path):
        """Load channel-specific rulebook"""
        try:
            with open(rulebook_path, 'r', encoding='utf-8') as f:
                content = f.read()
            self.logger.info(f"[OK] Rulebook loaded: {len(content)} characters")
            return content
        except FileNotFoundError:
            self.logger.warning(f"[WARN] Rulebook not found: {rulebook_path}")
            return None
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to load rulebook: {e}")
            return None
    
    def _load_instruments_cached(self, csv_path):
        """Load instruments with weekly caching"""
        import pickle
        from pathlib import Path
        
        cache_file = 'jp_instruments_cache.pkl'
        cache_path = Path(cache_file)
        
        # Check if cache exists and is < 7 days old
        if cache_path.exists():
            from datetime import datetime, timedelta
            cache_age = datetime.now() - datetime.fromtimestamp(cache_path.stat().st_mtime)
            if cache_age.days < 7:
                self.logger.info(f"[CACHE] Loading from cache (age: {cache_age.days} days)")
                try:
                    with open(cache_file, 'rb') as f:
                        return pickle.load(f)
                except Exception as e:
                    self.logger.warning(f"[WARN] Cache load failed: {e}, loading from CSV...")
        
        # Load fresh from CSV
        self.logger.info("[LOADING] Fresh load from CSV...")
        try:
            import pandas as pd
            df = pd.read_csv(csv_path)
            
            # Filter for options
            if 'instrument_type' in df.columns:
                options = df[df['instrument_type'].isin(['OPTIDX', 'INDEX_OPTION', 'OPTSTK'])].copy()
            else:
                options = df.copy()
            
            self.logger.info(f"[LOAD] Found {len(options)} option instruments")
            
            # Create lookup dictionary
            instruments = {}
            for _, row in options.iterrows():
                key = f"{row['symbol']}_{row['strike']}_{row['option_type']}"
                instruments[key] = {
                    'tradingsymbol': row.get('tradingsymbol', ''),
                    'expiry_date': row.get('expiry_date', ''),
                    'lot_size': row.get('lot_size', 1),
                    'exchange': row.get('exchange', 'NSE')
                }
            
            # Save to cache
            try:
                with open(cache_file, 'wb') as f:
                    pickle.dump(instruments, f)
                self.logger.info(f"[CACHE] Saved to {cache_file}")
            except Exception as e:
                self.logger.warning(f"[WARN] Failed to save cache: {e}")
            
            return instruments
            
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to load instruments: {e}")
            return {}
    
    def parse(self, message, message_date=None):
        """Main parsing function"""
        # Clean message
        message_clean = message.strip()
        
        # Skip non-trading messages
        if self._should_skip(message_clean):
            self.logger.info(f"[SKIP] Non-trading: {message_clean[:50]}...")
            return None
        
        # Try regex patterns first (fast)
        result = self._parse_with_regex(message_clean)
        
        if result:
            # Enrich with message date
            result = self._enrich(result, message_date)
            self.logger.info(f"[REGEX] Parsed: {result.get('symbol')} {result.get('strike')} {result.get('option_type')}")
            return result
        
        # Fallback to Claude if available
        if self.claude_client:
            result = self._parse_with_claude(message_clean)
            if result:
                result = self._enrich(result, message_date)
                self.logger.info(f"[CLAUDE] Parsed: {result.get('symbol')} {result.get('strike')} {result.get('option_type')}")
                return result
        
        self.logger.info(f"[FAIL] Could not parse: {message_clean[:50]}...")
        return None
    
    def _should_skip(self, message):
        """Check if message should be skipped"""
        message_lower = message.lower()
        
        # Check skip patterns
        for pattern in self.SKIP_PATTERNS:
            if re.search(pattern, message_lower, re.IGNORECASE):
                return True
        
        # Skip if no CE or PE
        if 'CE' not in message.upper() and 'PE' not in message.upper():
            return True
        
        # Skip if too short
        if len(message) < 10:
            return True
        
        return False
    
    def _parse_with_regex(self, message):
        """Parse using regex patterns"""
        
        # Pattern 1: SYMBOL STRIKE CE/PE PRICE [SL PRICE]
        # Examples: "Nifty 26000 CE 173", "CDSL 1640 CE 51.5 SL 47"
        pattern1 = r'([A-Za-z]+)\s+(\d+)\s+(CE|PE)\s+([\d.]+)(?:\s+SL\s+([\d.]+))?'
        match = re.search(pattern1, message, re.IGNORECASE)
        
        if match:
            symbol_raw = match.group(1)
            strike = int(match.group(2))
            option_type = match.group(3).upper()
            entry_price = float(match.group(4))
            stop_loss = float(match.group(5)) if match.group(5) else None
            
            # Normalize symbol
            symbol = self._identify_symbol(symbol_raw, strike)
            
            # Build result
            result = {
                'symbol': symbol,
                'strike': strike,
                'option_type': option_type,
                'action': 'BUY',
                'entry_price': entry_price,
                'stop_loss': stop_loss or self._generate_default_sl(entry_price, option_type),
                'targets': [],
                'quantity': self._get_lot_size(symbol, strike, option_type),
                'message_type': 'index' if symbol in self.INDEX_SYMBOLS else 'stock'
            }
            
            # Note: _enrich will be called in parse() with message_date
            return result
        
        # Pattern 2: Just STRIKE CE/PE PRICE (no symbol - infer from strike)
        # Examples: "26200 CE 140", "60800 CE 500"
        pattern2 = r'(\d{5,6})\s+(CE|PE)\s+([\d.]+)(?:\s+SL\s+([\d.]+))?'
        match = re.search(pattern2, message, re.IGNORECASE)
        
        if match:
            strike = int(match.group(1))
            option_type = match.group(2).upper()
            entry_price = float(match.group(3))
            stop_loss = float(match.group(4)) if match.group(4) else None
            
            # Identify index by strike range
            symbol = self._identify_symbol_by_strike(strike)
            
            if symbol:
                result = {
                    'symbol': symbol,
                    'strike': strike,
                    'option_type': option_type,
                    'action': 'BUY',
                    'entry_price': entry_price,
                    'stop_loss': stop_loss or self._generate_default_sl(entry_price, option_type),
                    'targets': [],
                    'quantity': self._get_lot_size(symbol, strike, option_type),
                    'message_type': 'index'
                }
                
                # Note: _enrich will be called in parse() with message_date
                return result
        
        return None
    
    def _identify_symbol(self, symbol_raw, strike):
        """Identify if it's an index or stock symbol"""
        symbol_upper = symbol_raw.upper()
        
        # Check if it's a known index
        if symbol_upper in ['NIFTY', 'BANKNIFTY', 'SENSEX', 'BANKEX']:
            return symbol_upper
        
        # Check if strike suggests an index
        for index, ranges in self.INDEX_SYMBOLS.items():
            if ranges['min_strike'] <= strike <= ranges['max_strike']:
                return index
        
        # Otherwise, it's a stock - normalize the name
        return symbol_upper
    
    def _identify_symbol_by_strike(self, strike):
        """Identify index symbol by strike price alone"""
        for index, ranges in self.INDEX_SYMBOLS.items():
            if ranges['min_strike'] <= strike <= ranges['max_strike']:
                return index
        return None
    
    def _generate_default_sl(self, entry_price, option_type):
        """Generate default stop loss if not provided"""
        if option_type == 'CE':
            # For calls, SL is 15% below entry
            return round(entry_price * 0.85, 2)
        else:
            # For puts, SL is 15% above entry
            return round(entry_price * 1.15, 2)
    
    def _get_lot_size(self, symbol, strike, option_type):
        """Get lot size for symbol"""
        # Check in loaded instruments
        key = f"{symbol}_{strike}_{option_type}"
        if key in self.valid_instruments:
            return self.valid_instruments[key]['lot_size']
        
        # Default lot sizes for indices (CURRENT as of Dec 30, 2024)
        # Based on actual Zerodha error messages
        defaults = {
            'NIFTY': 65,      # Error said: "should be multiple of 65"
            'BANKNIFTY': 30,  # Error said: "should be multiple of 35"
            'SENSEX': 10,
            'BANKEX': 10,
            'FINNIFTY': 40,
            'MIDCPNIFTY': 75
        }
        
        if symbol in defaults:
            return defaults[symbol]
        
        # For stocks, default to 1 (usually unknown)
        return 1
    
    def _get_last_thursday_of_month(self, year, month):
        """Get last Thursday of a given month"""
        # Start from last day of month
        if month == 12:
            last_day = datetime(year, month, 31)
        else:
            last_day = datetime(year, month + 1, 1) - timedelta(days=1)
        
        # Find last Thursday
        while last_day.weekday() != 3:  # 3 = Thursday
            last_day -= timedelta(days=1)
        
        return last_day.strftime('%Y-%m-%d')
    
    def _get_last_wednesday_of_month(self, year, month):
        """Get last Wednesday of a given month (for BANKNIFTY monthly)"""
        # Start from last day of month
        if month == 12:
            last_day = datetime(year, month, 31)
        else:
            last_day = datetime(year, month + 1, 1) - timedelta(days=1)
        
        # Find last Wednesday
        while last_day.weekday() != 2:  # 2 = Wednesday
            last_day -= timedelta(days=1)
        
        return last_day.strftime('%Y-%m-%d')
    
    def _get_next_weekly_expiry(self, symbol, reference_date=None):
        """Get next weekly expiry for index from a reference date"""
        if reference_date is None:
            reference_date = datetime.now()
        elif isinstance(reference_date, str):
            reference_date = datetime.fromisoformat(reference_date.replace('Z', '+00:00'))
            if reference_date.tzinfo is not None:
                reference_date = reference_date.replace(tzinfo=None)
        elif hasattr(reference_date, 'tzinfo') and reference_date.tzinfo is not None:
            reference_date = reference_date.replace(tzinfo=None)
        
        # Weekly expiry days
        expiry_days = {
            'NIFTY': 3,      # Thursday
            'BANKNIFTY': 2,  # Wednesday
            'SENSEX': 4,     # Friday
            'FINNIFTY': 1,   # Tuesday
            'MIDCPNIFTY': 0  # Monday
        }
        
        target_day = expiry_days.get(symbol, 3)  # Default Thursday
        
        # Calculate days until target day
        days_ahead = (target_day - reference_date.weekday()) % 7
        if days_ahead == 0:
            days_ahead = 7  # If today is the day, get next week
        
        next_expiry = reference_date + timedelta(days=days_ahead)
        return next_expiry.strftime('%Y-%m-%d')
    
    def _enrich(self, result, message_date=None):
        """Enrich result with expiry and tradingsymbol - use ACTUAL expiry from instruments CSV"""
        symbol = result['symbol']
        strike = result['strike']
        option_type = result['option_type']
        
        # CRITICAL: Look up instrument in CSV to get ACTUAL expiry date
        key = f"{symbol}_{strike}_{option_type}"
        
        if key in self.valid_instruments:
            # Found exact match in instruments - use its data
            instrument = self.valid_instruments[key]
            result['expiry_date'] = instrument['expiry_date']
            result['tradingsymbol'] = instrument['tradingsymbol']
            result['exchange'] = instrument['exchange']
            result['quantity'] = instrument['lot_size']
            
            self.logger.info(f"[INSTRUMENT] Found in CSV: {instrument['tradingsymbol']}, Expiry: {instrument['expiry_date']}")
            return result
        
        # If not found, look for closest strike (for ATM/OTM options)
        self.logger.warning(f"[WARN] {key} not found in instruments, searching for closest strike...")
        
        # Find all instruments for this symbol and option type
        similar_instruments = []
        for inst_key, inst_data in self.valid_instruments.items():
            parts = inst_key.split('_')
            if len(parts) == 3:
                inst_symbol, inst_strike_str, inst_type = parts
                if inst_symbol == symbol and inst_type == option_type:
                    try:
                        inst_strike = int(inst_strike_str)
                        similar_instruments.append((inst_strike, inst_data))
                    except ValueError:
                        continue
        
        if similar_instruments:
            # Find closest strike
            similar_instruments.sort(key=lambda x: abs(x[0] - strike))
            closest_strike, closest_data = similar_instruments[0]
            
            self.logger.warning(f"[APPROX] Using closest: Strike {closest_strike} (wanted {strike})")
            result['expiry_date'] = closest_data['expiry_date']
            result['tradingsymbol'] = closest_data['tradingsymbol']
            result['exchange'] = closest_data['exchange']
            result['quantity'] = closest_data['lot_size']
            
            # Update strike to match actual instrument
            result['strike'] = closest_strike
            
            return result
        
        # Last resort: Calculate expiry (fallback only)
        self.logger.error(f"[FALLBACK] No instruments found for {symbol}, calculating expiry...")
        
        # Use message date if provided, otherwise current date
        if message_date is None:
            reference_date = datetime.now()
        elif isinstance(message_date, str):
            reference_date = datetime.fromisoformat(message_date.replace('Z', '+00:00'))
            if reference_date.tzinfo is not None:
                reference_date = reference_date.replace(tzinfo=None)
        else:
            reference_date = message_date
            if hasattr(reference_date, 'tzinfo') and reference_date.tzinfo is not None:
                reference_date = reference_date.replace(tzinfo=None)
        
        # Calculate expiry (last resort)
        if symbol == 'BANKNIFTY':
            expiry_date = self._get_last_wednesday_of_month(reference_date.year, reference_date.month)
        elif symbol in self.INDEX_SYMBOLS:
            expiry_date = self._get_next_weekly_expiry(symbol, reference_date)
        else:
            expiry_date = self._get_last_thursday_of_month(reference_date.year, reference_date.month)
        
        result['expiry_date'] = expiry_date
        
        # Build tradingsymbol
        expiry_dt = datetime.strptime(expiry_date, '%Y-%m-%d')
        
        if symbol in self.INDEX_SYMBOLS:
            expiry_str = expiry_dt.strftime('%y%b').upper()
            result['tradingsymbol'] = f"{symbol}{expiry_str}{strike}{option_type}"
        else:
            expiry_str = expiry_dt.strftime('%d%b%y').upper()
            result['tradingsymbol'] = f"{symbol}{expiry_str}{strike}{option_type}"
        
        # Set exchange
        if symbol in ['SENSEX', 'BANKEX']:
            result['exchange'] = 'BFO'
        elif symbol in self.INDEX_SYMBOLS:
            result['exchange'] = 'NFO'
        else:
            result['exchange'] = 'NSE'
        
        # Get lot size
        result['quantity'] = self._get_lot_size(symbol, strike, option_type)
        
        return result
    
    def _parse_with_claude(self, message):
        """Fallback to Claude for complex messages"""
        if not self.claude_client:
            return None
        
        try:
            prompt = f"""Parse this trading signal and return ONLY a JSON object:

Message: {message}

Rules:
1. If it mentions CE or PE, it's an option signal
2. Extract: symbol, strike, option_type (CE/PE), entry_price
3. Stop loss is optional (after "SL")
4. If no symbol mentioned and strike is 24000-28000: symbol is NIFTY
5. If no symbol mentioned and strike is 56000-62000: symbol is BANKNIFTY
6. If symbol is a company name: it's a stock option
7. Action is always BUY for this channel

Return JSON:
{{
  "symbol": "NIFTY or STOCKNAME",
  "strike": 26000,
  "option_type": "CE",
  "entry_price": 173.0,
  "stop_loss": 160.0
}}

Return ONLY JSON, nothing else."""
            
            response = self.claude_client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}]
            )
            
            text = response.content[0].text
            
            # Extract JSON
            import json
            json_match = re.search(r'\{.*\}', text, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group())
                
                # Add defaults
                data['action'] = 'BUY'
                data['targets'] = []
                data['quantity'] = self._get_lot_size(
                    data['symbol'],
                    data.get('strike', 0),
                    data.get('option_type', 'CE')
                )
                data['message_type'] = 'claude_parsed'
                
                # Generate SL if missing
                if not data.get('stop_loss'):
                    data['stop_loss'] = self._generate_default_sl(
                        data['entry_price'],
                        data['option_type']
                    )
                
                # Enrich
                data = self._enrich(data)
                
                return data
        
        except Exception as e:
            self.logger.error(f"[CLAUDE ERROR] {e}")
        
        return None


if __name__ == "__main__":
    # Test
    logging.basicConfig(level=logging.INFO)
    
    parser = JPChannelParser()
    
    test_messages = [
        "Muthootfin 3800 CE 101",
        "CDSL 1640 CE 51.5",
        "Nifty 26000 CE 173",
        "Banknifty 60500 CE 444 SL 400",
        "26200 CE 140",
        "Persistent 6400 CE 185 SL 180",
        "62 high",  # Should skip
        "Wait for the levels",  # Should skip
    ]
    
    for msg in test_messages:
        print(f"\n[TEST] {msg}")
        result = parser.parse(msg)
        if result:
            print(f"  ✓ {result['symbol']} {result['strike']} {result['option_type']}")
            print(f"    Entry: {result['entry_price']}, SL: {result['stop_loss']}")
            print(f"    Tradingsymbol: {result['tradingsymbol']}")
        else:
            print(f"  ✗ Skipped/Failed")
