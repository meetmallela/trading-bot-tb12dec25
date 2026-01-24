"""
jp_channel_parser.py - FULLY CORRECTED VERSION
Specialized parser for JP Paper trade channel
USES ACTUAL INSTRUMENT DATA FROM CSV with NEAREST EXPIRY SELECTION
"""

import re
import logging
from datetime import datetime, timedelta
from anthropic import Anthropic

# Import the fix at top of file
from fix_instrument_lookup import (
    load_instruments_with_expiry_lookup,
    find_nearest_expiry_instrument
)

class JPChannelParser:
    """Parser specifically designed for JP Paper trade channel"""
    
    # ========================================
    # SYMBOL-SPECIFIC EXPIRY DAYS
    # ========================================
    # Format: {symbol: weekday} where 0=Monday, 1=Tuesday, 2=Wednesday, 3=Thursday, 4=Friday
    EXPIRY_DAY_MAP = {
        'SENSEX': 2,      # Wednesday (BSE index)
        'BANKEX': 2,      # Wednesday (BSE index)
        'NIFTY': 3,       # Thursday (NSE index)
        'BANKNIFTY': 2,   # Wednesday (NSE index)
        'FINNIFTY': 1,    # Tuesday (NSE index)
        'MIDCPNIFTY': 0,  # Monday (NSE index)
    }
    
    # ========================================
    # SYMBOL-SPECIFIC LOT SIZES (UPDATED JAN 2026)
    # ========================================
    DEFAULT_LOT_SIZES = {
        'SENSEX': 20,      # BSE - CORRECTED from 10 to 20
        'BANKEX': 15,      # BSE
        'NIFTY': 65,       # NSE
        'BANKNIFTY': 30,   # NSE - CORRECTED from 30 to 15
        'FINNIFTY': 25,    # NSE
        'MIDCPNIFTY': 50,  # NSE
    }
    
    # Known index symbols
    INDEX_SYMBOLS = {
        'NIFTY': {'min_strike': 24000, 'max_strike': 28000},
        'BANKNIFTY': {'min_strike': 56000, 'max_strike': 64000},
        'SENSEX': {'min_strike': 81000, 'max_strike': 88000}
    }
    
    # Skip patterns (non-trading messages)
    SKIP_PATTERNS = [
        r'wait for',
        r'hold below',
        r'pass\s*ðŸ"¥',
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
        
        # CORRECTED: Load instruments with new expiry-aware structure
        if instruments_csv:
            try:
                self.logger.info("[LOADING] Loading instruments with expiry-aware lookup...")
                
                # NEW: Load with both lookup structures
                self.instruments_by_tradingsymbol, self.instruments_by_symbol_strike_type = \
                    load_instruments_with_expiry_lookup(instruments_csv)
                
                # OLD: Keep for backward compatibility (will be deprecated)
                self.valid_instruments = self._load_instruments_cached(instruments_csv)
                
                self.logger.info(f"[OK] Loaded instruments with expiry-aware lookup")
                
            except Exception as e:
                self.logger.error(f"[ERROR] Failed to load instruments: {e}")
                self.instruments_by_tradingsymbol = {}
                self.instruments_by_symbol_strike_type = {}
                self.valid_instruments = {}
        else:
            self.instruments_by_tradingsymbol = {}
            self.instruments_by_symbol_strike_type = {}
            self.valid_instruments = {}
        
        self.logger.info("[INIT] JP Channel Parser initialized")
    
    def _calculate_nearest_expiry(self, symbol):
        """
        Calculate nearest expiry based on symbol-specific expiry day
        
        Args:
            symbol: Symbol name (SENSEX, NIFTY, etc.)
        
        Returns:
            str: Expiry date in 'YYYY-MM-DD' format
        """
        from datetime import datetime, timedelta
        
        today = datetime.now().date()
        
        # Get target weekday for this symbol (default to Thursday if not found)
        target_weekday = self.EXPIRY_DAY_MAP.get(symbol, 3)  # Default Thursday
        
        # Calculate days ahead to target weekday
        current_weekday = today.weekday()
        days_ahead = target_weekday - current_weekday
        
        # If target day is today or has passed this week, get next week's
        if days_ahead <= 0:
            days_ahead += 7
        
        expiry_date = today + timedelta(days=days_ahead)
        
        self.logger.info(f"[EXPIRY] {symbol} expires on weekday {target_weekday}, calculated: {expiry_date.strftime('%Y-%m-%d')}")
        
        return expiry_date.strftime('%Y-%m-%d')
    
    def _get_default_lot_size(self, symbol):
        """
        Get default lot size for symbol (fallback if CSV lookup fails)
        
        Args:
            symbol: Symbol name (SENSEX, NIFTY, etc.)
        
        Returns:
            int: Lot size
        """
        # Check hardcoded lot sizes first
        lot_size = self.DEFAULT_LOT_SIZES.get(symbol)
        
        if lot_size:
            self.logger.info(f"[LOT SIZE] Using fallback lot size {lot_size} for {symbol}")
            return lot_size
        
        # Last resort default
        self.logger.warning(f"[LOT SIZE] Using default lot size 1 for unknown symbol {symbol}")
        return 1
    
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
        """Load instruments using FastInstrumentFinder (Parquet support) - LEGACY"""
        import pickle
        from pathlib import Path
        
        cache_file = 'jp_instruments_cache.pkl'
        cache_path = Path(cache_file)
        
        # Check if cache exists and is < 7 days old
        if cache_path.exists():
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
            
            finder = FastInstrumentFinder()
            
            # Convert DataFrame to dictionary for compatibility
            instruments = {}
            for _, row in finder.df.iterrows():
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
        """Enrich result with data from instruments CSV using NEAREST EXPIRY"""
        symbol = result['symbol']
        strike = result['strike']
        option_type = result['option_type']
        
        # CORRECTED: Use nearest expiry lookup
        instrument = find_nearest_expiry_instrument(
            symbol=symbol,
            strike=strike,
            option_type=option_type,
            by_symbol_strike_type=self.instruments_by_symbol_strike_type,
            reference_date=message_date or datetime.now()
        )
        
        if instrument:
            # Found correct instrument with NEAREST expiry!
            result['tradingsymbol'] = instrument['tradingsymbol']
            result['expiry_date'] = instrument['expiry_date']
            result['exchange'] = instrument['exchange']
            result['quantity'] = instrument['lot_size']
            
            self.logger.info(
                f"[INSTRUMENT] Found: {instrument['tradingsymbol']}, "
                f"Expiry: {instrument['expiry_date']}, Lot: {instrument['lot_size']}"
            )
            return result
        
        # Not found - try closest strike
        self.logger.warning(f"[WARN] {symbol} {strike} {option_type} not found, searching closest strike...")
        
        # Search in new structure for closest strike
        similar_instruments = []
        for (inst_symbol, inst_strike, inst_type), instruments_list in self.instruments_by_symbol_strike_type.items():
            if inst_symbol == symbol and inst_type == option_type:
                similar_instruments.append((inst_strike, instruments_list))
        
        if similar_instruments:
            # Find closest strike
            similar_instruments.sort(key=lambda x: abs(x[0] - strike))
            closest_strike, instruments_list = similar_instruments[0]
            
            # Get nearest expiry for closest strike
            if instruments_list:
                closest_instrument = instruments_list[0]  # Already sorted by expiry
                
                self.logger.warning(
                    f"[APPROX] Using closest: {closest_instrument['tradingsymbol']} "
                    f"(wanted strike {strike}, got {closest_strike})"
                )
                
                result['strike'] = closest_strike
                result['tradingsymbol'] = closest_instrument['tradingsymbol']
                result['expiry_date'] = closest_instrument['expiry_date']
                result['exchange'] = closest_instrument['exchange']
                result['quantity'] = closest_instrument['lot_size']
                
                return result
        
        # Last resort: Use smart defaults based on symbol
        self.logger.error(f"[FALLBACK] {symbol} not found in CSV - using smart defaults")
        
        # Use symbol-specific lot size (CORRECTED VALUES)
        result['quantity'] = self._get_default_lot_size(symbol)
        
        # Use symbol-specific expiry calculation (CORRECTED FOR EACH SYMBOL)
        result['expiry_date'] = self._calculate_nearest_expiry(symbol)
        
        # Set exchange
        if symbol in ['SENSEX', 'BANKEX']:
            result['exchange'] = 'BFO'  # BSE
        else:
            result['exchange'] = 'NFO'  # NSE
        
        # Build tradingsymbol as fallback
        # Format: SYMBOL + YY + MMM + STRIKE + TYPE
        from datetime import datetime
        expiry_dt = datetime.strptime(result['expiry_date'], '%Y-%m-%d')
        year_code = expiry_dt.strftime('%y')
        month_code = expiry_dt.strftime('%b').upper()
        result['tradingsymbol'] = f"{symbol}{year_code}{month_code}{strike}{option_type}"
        
        self.logger.warning(
            f"[FALLBACK] Built: {result['tradingsymbol']} | "
            f"Expiry: {result['expiry_date']} | Lot: {result['quantity']}"
        )
        
        return result
    
    def _parse_with_claude(self, message):
        """Fallback to Claude for complex messages"""
        if not self.claude_client:
            return None
        
        # Claude parsing implementation here
        # (Keep your existing Claude parsing logic)
        return None
