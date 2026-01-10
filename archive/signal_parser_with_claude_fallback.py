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

class SignalParserWithFutures:
    """Parser with OPTIONS + FUTURES support"""
    
    def __init__(self, claude_api_key, rules_file='parsing_rules_enhanced_v2.json'):
        self.logger = logging.getLogger('PARSER')
        self.claude_api_key = claude_api_key
        self.instrument_finder = InstrumentFinderDF('valid_instruments.csv')
        
        # Load rules
        try:
            with open(rules_file, 'r') as f:
                self.rules = json.load(f)
        except:
            self.rules = {}
            self.logger.warning("Rules file not found, using defaults")
        
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
        """Enrich options with instrument data (existing logic)"""
        if not result.get('symbol') or not result.get('strike') or not result.get('option_type'):
            return result
        
        # Find instrument
        instrument = self.instrument_finder.find_instrument(
            result['symbol'],
            result['strike'],
            result['option_type'],
            result.get('expiry_date')
        )
        
        if instrument:
            result['tradingsymbol'] = instrument['symbol']
            
            if not result.get('expiry_date'):
                result['expiry_date'] = instrument['expiry_date']
                result['expiry_auto_added'] = True
            
            result['exchange'] = instrument['exchange']
            
            if not result.get('quantity'):
                result['quantity'] = self.instrument_finder.get_default_quantity(result['symbol'])
                result['quantity_auto_added'] = True
            
            self.logger.info(f"[ENRICH] Added: tradingsymbol={instrument['symbol']}, " +
                           f"expiry={instrument['expiry_date']}, qty={result.get('quantity')}")
        else:
            self.logger.warning(f"[WARN] Instrument not found: {result['symbol']} {result['strike']} {result['option_type']}")
        
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
