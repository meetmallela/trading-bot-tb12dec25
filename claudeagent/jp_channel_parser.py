"""
jp_channel_parser.py - CORRECTED VERSION
Specialized parser for JP Paper trade channel
USES ACTUAL INSTRUMENT DATA FROM CSV (not calculated)
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
        'BANKNIFTY': {'min_strike': 56000, 'max_strike': 64000},
        'SENSEX': {'min_strike': 82000, 'max_strike': 88000}
    }
    
    # Skip patterns (non-trading messages)
    SKIP_PATTERNS = [
        r'wait for',
        r'hold below',
        r'pass\s*🔥',
        r'^\d+\.?\d*\s*high',
        r'^\d+\s*$',
        r'make or brake',
        r'revised level',
        r'back to',
        r'eye again',
        r'still trading',
        r'profit booking',
        r'new level',
        r'night\s*\n',
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
        """Load instruments using FastInstrumentFinder (Parquet support)"""
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
                    self.logger.warning(f"[WARN] Cache load failed: {e}, loading fresh...")
        
        # Try to use FastInstrumentFinder with Parquet
        try:
            self.logger.info("[FAST] Attempting Parquet load...")
            from instrument_finder_FAST import FastInstrumentFinder
            
            self.finder = FastInstrumentFinder()
            
            # Convert DataFrame to dictionary for compatibility
            instruments = {}
            for _, row in self.finder.df.iterrows():
                key = f"{row['symbol']}_{int(row['strike'])}_{row['option_type']}"
                instruments[key] = {
                    'tradingsymbol': row['tradingsymbol'],
                    'expiry_date': row['expiry_date'].strftime('%Y-%m-%d') if hasattr(row['expiry_date'], 'strftime') else str(row['expiry_date']),
                    'lot_size': int(row['lot_size']),
                    'exchange': row['exchange']
                }
            
            # Save to cache
            try:
                with open(cache_file, 'wb') as f:
                    pickle.dump(instruments, f)
                self.logger.info(f"[CACHE] Saved to {cache_file}")
            except Exception as e:
                self.logger.warning(f"[WARN] Failed to save cache: {e}")
            
            self.logger.info(f"[OK] Loaded {len(instruments)} instruments via Parquet")
            return instruments
            
        except ImportError:
            self.logger.warning("[WARN] FastInstrumentFinder not found, using CSV...")
        except Exception as e:
            self.logger.warning(f"[WARN] Parquet load failed: {e}, using CSV...")
        
        # Fallback to CSV loading
        self.logger.info("[LOADING] Fresh load from CSV...")
        try:
            import pandas as pd
            df = pd.read_csv(csv_path)
            
            # Filter for options
            options = df[df['option_type'].isin(['CE', 'PE'])].copy()
            
            self.logger.info(f"[LOAD] Found {len(options)} option instruments")
            
            # Create lookup dictionary
            instruments = {}
            for _, row in options.iterrows():
                key = f"{row['symbol']}_{row['strike']}_{row['option_type']}"
                instruments[key] = {
                    'tradingsymbol': row.get('tradingsymbol', ''),
                    'expiry_date': row.get('expiry_date', ''),
                    'lot_size': row.get('lot_size', 1),
                    'exchange': row.get('exchange', 'NFO')
                }
            
            # Save to cache
            try:
                with open(cache_file, 'wb') as f:
                    pickle.dump(instruments, f)
                self.logger.info(f"[CACHE] Saved to {cache_file}")
            except Exception as e:
                self.logger.warning(f"[WARN] Failed to save cache: {e}")
            
            self.logger.info(f"[OK] Loaded {len(instruments)} instruments")
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
            # Enrich with CSV data
            result = self._enrich(result, message_date)
            if result:
                self.logger.info(f"[REGEX] Parsed: {result.get('symbol')} {result.get('strike')} {result.get('option_type')}")
                return result
        
        # Fallback to Claude if available
        if self.claude_client:
            result = self._parse_with_claude(message_clean)
            if result:
                result = self._enrich(result, message_date)
                if result:
                    self.logger.info(f"[CLAUDE] Parsed: {result.get('symbol')} {result.get('strike')} {result.get('option_type')}")
                    return result
        
        self.logger.info(f"[FAIL] Could not parse: {message_clean[:50]}...")
        return None
    
    def _should_skip(self, message):
        """Check if message should be skipped"""
        message_lower = message.lower()
        
        for pattern in self.SKIP_PATTERNS:
            if re.search(pattern, message_lower):
                return True
        
        return False
    
    def _parse_with_regex(self, message):
        """Fast regex-based parsing"""
        
        # Pattern 1: Just strike and type (infer symbol from strike)
        # Example: "26200 PE 195"
        pattern1 = r'(\d{5,6})\s*(CE|PE|ce|pe)\s+(\d+\.?\d*)'
        match = re.search(pattern1, message, re.IGNORECASE)
        
        if match:
            strike = int(match.group(1))
            option_type = match.group(2).upper()
            entry_price = float(match.group(3))
            
            # Infer symbol from strike range
            symbol = self._identify_symbol_by_strike(strike)
            
            if symbol:
                # Extract SL if present
                sl_match = re.search(r'SL\s*(\d+\.?\d*)', message, re.IGNORECASE)
                stop_loss = float(sl_match.group(1)) if sl_match else self._generate_default_sl(entry_price, option_type)
                
                return {
                    'symbol': symbol,
                    'strike': strike,
                    'option_type': option_type,
                    'entry_price': entry_price,
                    'stop_loss': stop_loss,
                    'action': 'BUY'
                }
        
        # Pattern 2: Symbol + strike + type
        # Example: "TATASTEEL 180 CE 5.5"
        pattern2 = r'([a-zA-Z]+)\s+(\d+)\s*(CE|PE|ce|pe)\s+(\d+\.?\d*)'
        match = re.search(pattern2, message, re.IGNORECASE)
        
        if match:
            symbol = match.group(1).upper()
            strike = int(match.group(2))
            option_type = match.group(3).upper()
            entry_price = float(match.group(4))
            
            # Extract SL if present
            sl_match = re.search(r'SL\s*(\d+\.?\d*)', message, re.IGNORECASE)
            stop_loss = float(sl_match.group(1)) if sl_match else self._generate_default_sl(entry_price, option_type)
            
            return {
                'symbol': symbol,
                'strike': strike,
                'option_type': option_type,
                'entry_price': entry_price,
                'stop_loss': stop_loss,
                'action': 'BUY'
            }
        
        return None
    
    def _identify_symbol_by_strike(self, strike):
        """Identify index symbol by strike price alone"""
        for index, ranges in self.INDEX_SYMBOLS.items():
            if ranges['min_strike'] <= strike <= ranges['max_strike']:
                return index
        return None
    
    def _generate_default_sl(self, entry_price, option_type):
        """Generate default stop loss if not provided"""
        if option_type == 'CE':
            return round(entry_price * 0.85, 2)
        else:
            return round(entry_price * 1.15, 2)
    
    def _enrich(self, result, message_date=None):
        """Enrich result with data from instruments CSV"""
        symbol = result['symbol']
        strike = result['strike']
        option_type = result['option_type']
        
        # CRITICAL: Look up instrument in CSV
        key = f"{symbol}_{strike}_{option_type}"
        
        if key in self.valid_instruments:
            # Found exact match - use CSV data
            instrument = self.valid_instruments[key]
            result['tradingsymbol'] = instrument['tradingsymbol']
            result['expiry_date'] = instrument['expiry_date']
            result['exchange'] = instrument['exchange']
            result['quantity'] = instrument['lot_size']
            
            self.logger.info(f"[INSTRUMENT] Found in CSV: {instrument['tradingsymbol']}, Expiry: {instrument['expiry_date']}, Lot: {instrument['lot_size']}")
            return result
        
        # Try to find closest strike
        self.logger.warning(f"[WARN] {key} not found, searching closest strike...")
        
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
            # Find closest
            similar_instruments.sort(key=lambda x: abs(x[0] - strike))
            closest_strike, closest_data = similar_instruments[0]
            
            self.logger.warning(f"[APPROX] Using closest: Strike {closest_strike} (wanted {strike})")
            result['strike'] = closest_strike
            result['tradingsymbol'] = closest_data['tradingsymbol']
            result['expiry_date'] = closest_data['expiry_date']
            result['exchange'] = closest_data['exchange']
            result['quantity'] = closest_data['lot_size']
            
            return result
        
        # Last resort: Use defaults (will likely fail at order placement)
        self.logger.error(f"[FALLBACK] {symbol} not found in CSV - using defaults (will likely fail)")
        
        # Default lot sizes (UPDATED Dec 2024)
        defaults = {
            'NIFTY': 25,        # Updated Dec 2024
            'BANKNIFTY': 30,    # Updated Dec 2024 (was 35)
            'SENSEX': 10,
            'BANKEX': 10,
            'FINNIFTY': 25,     # Updated Dec 2024
            'MIDCPNIFTY': 50    # Updated Dec 2024
        }
        
        result['quantity'] = defaults.get(symbol, 1)
        
        # Set exchange
        if symbol in ['SENSEX', 'BANKEX']:
            result['exchange'] = 'BFO'
        elif symbol in self.INDEX_SYMBOLS:
            result['exchange'] = 'NFO'
        else:
            result['exchange'] = 'NFO'  # Stock options are in NFO
        
        # Build tradingsymbol as fallback (probably wrong)
        if message_date:
            try:
                if isinstance(message_date, str):
                    ref_date = datetime.fromisoformat(message_date.replace('Z', '+00:00'))
                    if ref_date.tzinfo:
                        ref_date = ref_date.replace(tzinfo=None)
                else:
                    ref_date = message_date
            except:
                ref_date = datetime.now()
        else:
            ref_date = datetime.now()
        
        # Calculate expiry (will be wrong for stocks)
        expiry_date = self._calculate_expiry(symbol, ref_date)
        expiry_dt = datetime.strptime(expiry_date, '%Y-%m-%d')
        
        if symbol in self.INDEX_SYMBOLS:
            expiry_str = expiry_dt.strftime('%y%b').upper()
            result['tradingsymbol'] = f"{symbol}{expiry_str}{strike}{option_type}"
        else:
            expiry_str = expiry_dt.strftime('%d%b%y').upper()
            result['tradingsymbol'] = f"{symbol}{expiry_str}{strike}{option_type}"
        
        result['expiry_date'] = expiry_date
        
        return result
    
    def _calculate_expiry(self, symbol, reference_date):
        """Calculate expiry (fallback only - will be inaccurate)"""
        if symbol == 'BANKNIFTY':
            # Last Wednesday of month
            year = reference_date.year
            month = reference_date.month
            if month == 12:
                last_day = datetime(year, month, 31)
            else:
                last_day = datetime(year, month + 1, 1) - timedelta(days=1)
            
            while last_day.weekday() != 2:  # Wednesday
                last_day -= timedelta(days=1)
            
            return last_day.strftime('%Y-%m-%d')
        
        elif symbol in self.INDEX_SYMBOLS:
            # Weekly expiry
            expiry_days = {'NIFTY': 3, 'BANKNIFTY': 2, 'SENSEX': 4}
            target_day = expiry_days.get(symbol, 3)
            
            days_ahead = (target_day - reference_date.weekday()) % 7
            if days_ahead == 0:
                days_ahead = 7
            
            next_expiry = reference_date + timedelta(days=days_ahead)
            return next_expiry.strftime('%Y-%m-%d')
        
        else:
            # Monthly expiry (last Thursday)
            year = reference_date.year
            month = reference_date.month
            if month == 12:
                last_day = datetime(year, month, 31)
            else:
                last_day = datetime(year, month + 1, 1) - timedelta(days=1)
            
            while last_day.weekday() != 3:  # Thursday
                last_day -= timedelta(days=1)
            
            return last_day.strftime('%Y-%m-%d')
    
    def _parse_with_claude(self, message):
        """Fallback to Claude for complex messages"""
        if not self.claude_client:
            return None
        
        # Claude parsing implementation here
        return None
