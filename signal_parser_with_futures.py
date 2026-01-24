"""
signal_parser_with_futures.py - Enhanced parser with OPTIONS + FUTURES support

Features:
1. Handles both OPTIONS (CE/PE) and FUTURES
2. Regex extraction + Claude API fallback
3. Auto-enrichment for expiry dates and quantities
4. Validates and completes all required fields
"""

import re
import json
import logging
from datetime import datetime
import calendar
import requests
from instrument_finder_df import InstrumentFinderDF
from instrument_finder_FAST import FastInstrumentFinder

class SignalParserWithFutures:
    """Parser with OPTIONS + FUTURES support"""
    
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
    # SYMBOL-SPECIFIC LOT SIZES (FALLBACK)
    # ========================================
    # These are used as fallback if CSV lookup fails
    DEFAULT_LOT_SIZES = {
        'SENSEX': 20,
        'BANKEX': 15,
        'NIFTY': 65,
        'BANKNIFTY': 30,
        'FINNIFTY': 25,
        'MIDCPNIFTY': 50,
    }
    
    def __init__(self, claude_api_key, rules_file='parsing_rules_enhanced_v2.json'):
        self.logger = logging.getLogger('PARSER')
        self.claude_api_key = claude_api_key
        #self.instrument_finder = InstrumentFinderDF('valid_instruments.csv')
        self.instrument_finder = FastInstrumentFinder('valid_instruments.parquet')
        
        # Load rules
        try:
            with open(rules_file, 'r') as f:
                self.rules = json.load(f)
        except FileNotFoundError:
            self.rules = {}
            self.logger.warning(f"Rules file '{rules_file}' not found, using defaults")
        except (json.JSONDecodeError, IOError) as e:
            self.rules = {}
            self.logger.warning(f"Could not load rules file: {e}, using defaults")
        
        # Futures lot sizes (MCX)
        self.futures_lot_sizes = {
            'GOLD': 100,
            'GOLDM': 10,
            'SILVER': 30000,
            'SILVERM': 5000,
            'CRUDEOIL': 100,
            'NATURALGAS': 1250,
            'COPPER': 1000,
            'ZINC': 5000,
            'LEAD': 5000,
            'NICKEL': 250,
            'ALUMINIUM': 5000
        }
    
    def parse(self, message, channel_id=None):
        """
        Parse message - handles both OPTIONS and FUTURES
        
        Returns:
            dict with ALL required fields or None
        """
        # Step 1: Check if should ignore
        if self._should_ignore(message):
            return None
        
        # Step 2: Detect if it's FUTURES or OPTIONS
        is_futures = self._is_futures_signal(message)
        
        if is_futures:
            self.logger.info("[FUTURES] Detected futures signal")
            result = self._parse_futures(message)
        else:
            self.logger.info("[OPTIONS] Detected options signal")
            result = self._parse_options(message)
        
        # Step 3: If parsing failed, try Claude API
        if not result or not self._has_minimum_fields(result, is_futures):
            self.logger.info("[FALLBACK] Using Claude API...")
            result = self._extract_with_claude(message, is_futures)
        
        # Step 4: If still no result, reject
        if not result:
            self.logger.warning("[REJECT] Could not parse signal")
            return None
        
        # Step 5: Enrich with additional data
        if is_futures:
            result = self._enrich_futures_data(result)
        else:
            result = self._enrich_options_data(result)
        
        # Step 6: Final validation
        if not self._validate_complete(result, is_futures):
            self.logger.warning(f"[INCOMPLETE] Missing fields after enrichment")
            return None
        
        # Log success
        if is_futures:
            self.logger.info(f"[✓ VALID] {result['symbol']} FUTURES | " +
                           f"Action: {result['action']} | Entry: {result['entry_price']} | SL: {result['stop_loss']}")
        else:
            self.logger.info(f"[✓ VALID] {result['symbol']} {result['strike']} {result['option_type']} | " +
                           f"Action: {result['action']} | Entry: {result['entry_price']} | SL: {result['stop_loss']}")
        
        return result
    
    def _is_futures_signal(self, message):
        """Detect if message is about futures"""
        futures_indicators = [
            # Month codes
            'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 
            'AUG', 'SEP', 'OCT', 'NOV', 'DEC',
            # Explicit futures mention
            'FUTURES', 'FUT', 'MCX',
        ]
        
        message_upper = message.upper()
        
        # Check for month codes (strong indicator of futures)
        for month in ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 
                     'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']:
            if re.search(rf'\b{month}\b', message_upper):
                # Make sure it's not just a date
                if 'GOLD' in message_upper or 'SILVER' in message_upper or 'CRUDE' in message_upper:
                    return True
        
        # If it has CE or PE, it's definitely options
        if re.search(r'\b(CE|PE)\b', message_upper):
            return False
        
        # If it has strike price pattern, it's options
        if re.search(r'\d{4,5}\s*(CE|PE)', message_upper):
            return False
        
        return False
    
    def _parse_futures(self, message):
        """Parse FUTURES signal"""
        result = {'instrument_type': 'FUTURES'}
        
        # Action
        action_match = re.search(r'\b(BUY|SELL)\b', message, re.IGNORECASE)
        if action_match:
            result['action'] = action_match.group(1).upper()
        
        # Symbol
        futures_symbols = ['GOLD', 'GOLDM', 'SILVER', 'SILVERM', 'CRUDEOIL', 
                          'NATURALGAS', 'COPPER', 'ZINC', 'LEAD', 'NICKEL', 'ALUMINIUM']
        
        for symbol in futures_symbols:
            if symbol in message.upper():
                result['symbol'] = symbol
                break
        
        # Month code
        month_match = re.search(r'\b(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\b', 
                               message, re.IGNORECASE)
        if month_match:
            result['expiry_month'] = month_match.group(1).upper()
        
        # Entry price - futures have 5-6 digit prices
        entry_patterns = [
            r'CMP\s*(\d{5,6})',
            r'(?:ABOVE|NEAR|LEVEL)\s*[-:]*\s*(\d{5,6})',
            r'(?:BUY|SELL)\s+\w+\s+\w+\s+(?:CMP\s+)?(\d{5,6})',
        ]
        for pattern in entry_patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                result['entry_price'] = float(match.group(1))
                break
        
        # Stop loss
        sl_match = re.search(r'(?:SL|STOPLOSS|STOP LOSS)\s*[-:]*\s*(\d{5,6})', 
                            message, re.IGNORECASE)
        if sl_match:
            result['stop_loss'] = float(sl_match.group(1))
        
        # Targets
        target_match = re.search(r'(?:TARGET|TGT|T)\s*[-:]*\s*([\d/,.\s]+)', 
                                message, re.IGNORECASE)
        if target_match:
            targets_str = target_match.group(1)
            targets = re.findall(r'\d+', targets_str)
            result['targets'] = [float(t) for t in targets[:3]]
        
        # Futures don't have strike/option_type - set to None
        result['strike'] = None
        result['option_type'] = None
        
        return result if result.get('symbol') else None
    
    def _parse_options(self, message):
        """Parse OPTIONS signal (existing logic)"""
        result = {'instrument_type': 'OPTIONS'}
        
        # Action
        action_match = re.search(r'\b(BUY|SELL)\b', message, re.IGNORECASE)
        if action_match:
            result['action'] = action_match.group(1).upper()
        
        # Symbol
        known_symbols = ['NIFTY', 'BANKNIFTY', 'SENSEX', 'FINNIFTY', 
                        'GOLD', 'GOLDM', 'SILVER', 'SILVERM']
        
        for symbol in known_symbols:
            if symbol in message.upper():
                result['symbol'] = symbol
                break
        
        # If no known symbol, try stock pattern
        if not result.get('symbol'):
            stock_match = re.search(r'\b([A-Z]{2,20})\s*(\d{3,6})\s*(CE|PE)\b', 
                                   message, re.IGNORECASE)
            if stock_match:
                result['symbol'] = stock_match.group(1).upper()
                result['is_stock_option'] = True
        
        # Strike and option type
        strike_match = re.search(r'\b(\d{4,6})\s*(CE|PE)\b', message, re.IGNORECASE)
        if strike_match:
            result['strike'] = int(strike_match.group(1))
            result['option_type'] = strike_match.group(2).upper()
        
        # Entry price
        entry_patterns = [
            r'(?:ABOVE|BUY|NEAR|LEVEL)\s*[-:]*\s*(\d+\.?\d*)',
            r'ENTRY\s*[-:]*\s*(\d+\.?\d*)',
            r'CMP\s*[-:]*\s*(\d+\.?\d*)'
        ]
        for pattern in entry_patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                result['entry_price'] = float(match.group(1))
                break
        
        # Stop loss
        sl_match = re.search(r'(?:SL|STOPLOSS|STOP LOSS)\s*[-:]*\s*(\d+\.?\d*)', 
                            message, re.IGNORECASE)
        if sl_match:
            result['stop_loss'] = float(sl_match.group(1))
        
        # Targets
        target_match = re.search(r'(?:TARGET|TGT|T)\s*[-:]*\s*([\d/,.\s]+)', 
                                message, re.IGNORECASE)
        if target_match:
            targets_str = target_match.group(1)
            targets = re.findall(r'\d+\.?\d*', targets_str)
            result['targets'] = [float(t) for t in targets[:3]]
        
        return result if result.get('symbol') else None
    
    def _has_minimum_fields(self, result, is_futures):
        """Check if has minimum fields"""
        if not result:
            return False
        
        if is_futures:
            return all(result.get(f) for f in ['symbol', 'action'])
        else:
            return all(result.get(f) for f in ['symbol', 'strike', 'option_type'])
    
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
        for key, lot_size in self.DEFAULT_LOT_SIZES.items():
            if key in symbol.upper():
                self.logger.info(f"[LOT SIZE] Using fallback lot size {lot_size} for {symbol}")
                return lot_size
        
        # Last resort default
        self.logger.warning(f"[LOT SIZE] Using default lot size 10 for unknown symbol {symbol}")
        return 10
    
    def _enrich_futures_data(self, result):
        """Enrich futures with expiry date and quantity"""
        # Convert month code to expiry date
        if result.get('expiry_month') and not result.get('expiry_date'):
            year = datetime.now().year
            month_map = {
                'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4,
                'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8,
                'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
            }
            
            month_num = month_map.get(result['expiry_month'])
            if month_num:
                # MCX futures expire on last trading day of month
                # Simplified: use last day of month
                last_day = calendar.monthrange(year, month_num)[1]
                result['expiry_date'] = f"{year}-{month_num:02d}-{last_day}"
                result['expiry_auto_added'] = True
                self.logger.info(f"[ENRICH] Added expiry: {result['expiry_date']}")
        
        # Add quantity if missing
        if not result.get('quantity'):
            symbol = result.get('symbol')
            result['quantity'] = self.futures_lot_sizes.get(symbol, 1)
            result['quantity_auto_added'] = True
            self.logger.info(f"[ENRICH] Added quantity: {result['quantity']}")
        
        # Add exchange
        result['exchange'] = 'MCX'
        
        # Build tradingsymbol (e.g., GOLD25FEBFUT)
        if result.get('expiry_month'):
            year_code = str(datetime.now().year)[-2:]
            result['tradingsymbol'] = f"{result['symbol']}{year_code}{result['expiry_month']}FUT"
        
        return result
    
    def _enrich_options_data(self, result):
        """Enrich options with instrument data - handles commodities AND SENSEX"""
        if not result.get('symbol') or not result.get('strike') or not result.get('option_type'):
            return result
        
        # ========================================
        # COMMODITY OPTIONS - Build tradingsymbol manually
        # ========================================
        if result.get('is_commodity'):
            symbol = result['symbol']
            strike = result['strike']
            option_type = result['option_type']
            
            # Get expiry month (either from parsing or default to current)
            if result.get('expiry_month_text'):
                month_code = result['expiry_month_text']  # Already 3-letter like "JAN"
            else:
                # Default to current month
                from datetime import datetime
                month_code = datetime.now().strftime('%b').upper()
            
            # Determine year code
            from datetime import datetime
            import calendar
            
            now = datetime.now()
            current_year = now.year
            current_month = now.month
            
            # Month number map
            month_nums = {
                'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
            }
            
            expiry_month_num = month_nums.get(month_code, current_month)
            
            # If expiry month is in the future or current month, use current year
            # If expiry month is in the past, use next year
            if expiry_month_num >= current_month:
                year_code = str(current_year)[-2:]  # "25" or "26"
                expiry_year = current_year
            else:
                year_code = str(current_year + 1)[-2:]
                expiry_year = current_year + 1
            
            # Build tradingsymbol: GOLDM26JAN140000CE
            tradingsymbol = f"{symbol}{year_code}{month_code}{strike}{option_type}"
            
            result['tradingsymbol'] = tradingsymbol
            result['exchange'] = 'MCX'
            
            # Add lot size from MCX options lot sizes
            result['quantity'] = self.mcx_options_lot_sizes.get(symbol, 1)
            result['quantity_auto_added'] = True
            
            # Build expiry date (last trading day of month for commodities)
            last_day = calendar.monthrange(expiry_year, expiry_month_num)[1]
            result['expiry_date'] = f"{expiry_year}-{expiry_month_num:02d}-{last_day:02d}"
            result['expiry_auto_added'] = True
            
            self.logger.info(f"[COMMODITY ENRICH] Built: {tradingsymbol} | Expiry: {result['expiry_date']} | Lot: {result['quantity']}")
            
            return result
        
        # ========================================
        # INDEX OPTIONS (SENSEX, NIFTY, BANKNIFTY, etc.) - Use symbol-specific expiry
        # ========================================
        # Check if this is a known index symbol
        is_known_index = any(result['symbol'].upper().startswith(idx) for idx in self.EXPIRY_DAY_MAP.keys())
        
        if is_known_index:
            from datetime import datetime
            
            # Calculate expiry date using symbol-specific day
            if not result.get('expiry_date'):
                result['expiry_date'] = self._calculate_nearest_expiry(result['symbol'])
                result['expiry_auto_added'] = True
            
            # Build tradingsymbol: SENSEX26JAN84900PE
            expiry_dt = datetime.strptime(result['expiry_date'], '%Y-%m-%d')
            year_code = expiry_dt.strftime('%y')
            month_code = expiry_dt.strftime('%b').upper()
            tradingsymbol = f"{result['symbol']}{year_code}{month_code}{result['strike']}{result['option_type']}"
            result['tradingsymbol'] = tradingsymbol
            
            # Determine exchange
            if result['symbol'] in ['SENSEX', 'BANKEX']:
                result['exchange'] = 'BFO'  # BSE
            else:
                result['exchange'] = 'NFO'  # NSE
            
            # Get lot size (use hardcoded fallback)
            if not result.get('quantity'):
                result['quantity'] = self._get_default_lot_size(result['symbol'])
                result['quantity_auto_added'] = True
            
            self.logger.info(f"[INDEX ENRICH] {result['symbol']}: {tradingsymbol} | " +
                           f"Expiry: {result['expiry_date']} | Lot: {result['quantity']}")
            
            return result
        
        # ========================================
        # REGULAR OPTIONS - Use instrument finder
        # ========================================
        instrument = self.instrument_finder.find_instrument(
            result['symbol'],
            result['strike'],
            result['option_type'],
            result.get('expiry_date')
        )
        
        if instrument:
            #result['tradingsymbol'] = instrument['symbol']
            result['tradingsymbol'] = instrument.get('tradingsymbol', instrument.get('symbol'))
            
            if not result.get('expiry_date'):
                result['expiry_date'] = instrument['expiry_date']
                result['expiry_auto_added'] = True
            
            result['exchange'] = instrument['exchange']
            
            if not result.get('quantity'):
                # Try to get from instrument first
                try:
                    result['quantity'] = self.instrument_finder.get_default_quantity(result['symbol'])
                except (KeyError, AttributeError, ValueError) as e:
                    # Fallback to hardcoded lot sizes
                    self.logger.debug(f"Using fallback lot size for {result['symbol']}: {e}")
                    result['quantity'] = self._get_default_lot_size(result['symbol'])
                result['quantity_auto_added'] = True
            
            self.logger.info(f"[ENRICH] Added: tradingsymbol={instrument.get('tradingsymbol', instrument.get('symbol'))}, " +
                           f"expiry={instrument['expiry_date']}, qty={result.get('quantity')}")
        else:
            # Instrument not found in CSV - use fallbacks
            self.logger.warning(f"[WARN] Instrument not found: {result['symbol']} {result['strike']} {result['option_type']}")
            
            # Add fallback lot size if missing
            if not result.get('quantity'):
                result['quantity'] = self._get_default_lot_size(result['symbol'])
                result['quantity_auto_added'] = True
            
            # Add fallback expiry if missing
            if not result.get('expiry_date'):
                result['expiry_date'] = self._calculate_nearest_expiry(result['symbol'])
                result['expiry_auto_added'] = True
                self.logger.info(f"[FALLBACK] Using calculated expiry: {result['expiry_date']}")
        
        return result
    
    def _validate_complete(self, result, is_futures):
        """Final validation"""
        if is_futures:
            required = ['symbol', 'action', 'entry_price', 'stop_loss', 
                       'expiry_date', 'quantity', 'instrument_type']
        else:
            required = ['symbol', 'strike', 'option_type', 'action', 
                       'entry_price', 'stop_loss', 'expiry_date', 'quantity']
        
        missing = [f for f in required if not result.get(f)]
        
        if missing:
            self.logger.warning(f"[MISSING] Required fields: {missing}")
            return False
        
        # Type validation
        try:
            if not is_futures:
                result['strike'] = int(float(result['strike']))
                result['option_type'] = result['option_type'].upper()
                if result['option_type'] not in ['CE', 'PE']:
                    return False
            
            result['entry_price'] = float(result['entry_price'])
            result['stop_loss'] = float(result['stop_loss'])
            result['quantity'] = int(result['quantity'])
            result['action'] = result['action'].upper()
            
            if result['action'] not in ['BUY', 'SELL']:
                return False
            
            return True
            
        except (ValueError, TypeError) as e:
            self.logger.error(f"[ERROR] Type validation failed: {e}")
            return False
    
    def _should_ignore(self, message):
        """Check if message should be ignored"""
        ignore_patterns = self.rules.get('ignoreable_patterns', {})
        
        if len(message) < ignore_patterns.get('minimum_length', 10):
            return True
        
        for pattern in ignore_patterns.get('exact_matches', []):
            if message.strip().upper() == pattern.upper():
                return True
        
        for keyword in ignore_patterns.get('contains_keywords', []):
            if keyword.upper() in message.upper():
                return True
        
        return False
    
    def _extract_with_claude(self, message, is_futures):
        """Use Claude API as fallback"""
        if not self.claude_api_key:
            return None
        
        try:
            if is_futures:
                prompt = f"""Extract FUTURES trading signal from this message. Return ONLY a JSON object:

{{
  "action": "BUY or SELL",
  "symbol": "GOLD, SILVER, CRUDEOIL, etc.",
  "entry_price": 136830 (number),
  "stop_loss": 136500 (number),
  "targets": [137000, 137500] (array),
  "expiry_month": "FEB" (month code)
}}

Message: {message}

Return ONLY the JSON, no explanation."""
            else:
                prompt = f"""Extract OPTIONS trading signal from this message. Return ONLY a JSON object:

{{
  "action": "BUY or SELL",
  "symbol": "NIFTY, BANKNIFTY, etc.",
  "strike": 25900 (number),
  "option_type": "CE or PE",
  "entry_price": 140 (number),
  "stop_loss": 130 (number),
  "targets": [150, 160] (array)
}}

Message: {message}

Return ONLY the JSON, no explanation."""
            
            response = requests.post(
                'https://api.anthropic.com/v1/messages',
                headers={
                    'x-api-key': self.claude_api_key,
                    'anthropic-version': '2023-06-01',
                    'content-type': 'application/json'
                },
                json={
                    'model': 'claude-sonnet-4-20250514',
                    'max_tokens': 1024,
                    'messages': [{'role': 'user', 'content': prompt}]
                },
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                text = data['content'][0]['text']
                json_match = re.search(r'\{.*\}', text, re.DOTALL)
                if json_match:
                    result = json.loads(json_match.group())
                    result['instrument_type'] = 'FUTURES' if is_futures else 'OPTIONS'
                    if not is_futures:
                        pass  # Options already has strike/option_type
                    else:
                        result['strike'] = None
                        result['option_type'] = None
                    self.logger.info("[CLAUDE] Successfully parsed with API")
                    return result
            
            return None
            
        except Exception as e:
            self.logger.error(f"[CLAUDE] Error: {e}")
            return None
