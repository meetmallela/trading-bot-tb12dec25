"""
signal_parser_with_claude_fallback.py - Enhanced parser with validation + Claude API

Features:
1. Regex extraction (fast, free)
2. Comprehensive field validation
3. Claude API fallback when regex fails
4. DataFrame-based instrument validation
5. Auto quantity extraction/defaults
"""

import re
import json
import logging
from datetime import datetime
import requests
from instrument_finder_df import InstrumentFinderDF

class SignalParserWithClaudeFallback:
    """Parser with regex + Claude API fallback"""
    
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
    
    def parse(self, message, channel_id=None):
        """
        Parse message with validation
        
        Returns:
            dict with ALL required fields or None
        """
        # Step 1: Check if should ignore
        if self._should_ignore(message):
            return None
        
        # Step 2: Try regex extraction first (fast)
        result = self._extract_with_regex(message)
        #####
        # Before falling back to Claude, check for "Signal Markers"
        # Before falling back to Claude, check for "Signal Markers"

        # TOKEN SAVER: Check if it even looks like a signal before falling back to Claude
        if not result or not self._has_minimum_fields(result):
            # If no numbers and no CE/PE, it's just a chat message. Skip Claude to save tokens.
            if not (re.search(r'\d+', message) and re.search(r'CE|PE|BUY|SELL', message, re.I)):
                self.logger.info("[SKIP] Non-signal message. Skipping Claude fallback.")
                return None
            
            self.logger.info("[FALLBACK] Regex incomplete, using Claude API...")
            result = self._extract_with_claude(message)
        #####
        # Step 3: If regex fails, use Claude API
        if not result or not self._has_minimum_fields(result):
            self.logger.info("[FALLBACK] Regex incomplete, using Claude API...")
            result = self._extract_with_claude(message)
        
        # Step 4: If still no result, reject
        if not result:
            self.logger.warning("[REJECT] Could not parse signal")
            return None
        
        # Step 5: Enrich with instrument data
        result = self._enrich_with_instrument_data(result)
        
        # Step 6: Final validation
        if not self._validate_complete(result):
            self.logger.warning(f"[INCOMPLETE] Missing fields after enrichment")
            return None
        
        self.logger.info(f"[✓ VALID] {result['symbol']} {result['strike']} {result['option_type']} | " +
                        f"Action: {result['action']} | Entry: {result['entry_price']} | SL: {result['stop_loss']}")
        
        return result
    
    def _should_ignore(self, message):
        """Check if message should be ignored"""
        ignore_patterns = self.rules.get('ignoreable_patterns', {})
        
        # Check minimum length
        if len(message) < ignore_patterns.get('minimum_length', 10):
            return True
        
        # Check exact matches
        for pattern in ignore_patterns.get('exact_matches', []):
            if message.strip().upper() == pattern.upper():
                return True
        
        # Check contains keywords
        for keyword in ignore_patterns.get('contains_keywords', []):
            if keyword.upper() in message.upper():
                return True
        
        return False
    
    def _extract_with_regex(self, message):
        """Extract using regex patterns"""
        result = {}
        

        # 1. Action
        action_match = re.search(r'\b(BUY|SELL)\b', message, re.IGNORECASE)
        if action_match:
            result['action'] = action_match.group(1).upper()
        # 2. Normalized Symbol detection
        known_symbols = ['NIFTY', 'BANKNIFTY', 'SENSEX', 'FINNIFTY', 'GOLD', 'CRUDEOIL', 'SILVER', 'NATURALGAS']
        for symbol in known_symbols:
            if symbol in message.upper():
            # FORCING MINI CONTRACTS AS REQUESTED
            if symbol == 'GOLD': result['symbol'] = 'GOLDM'
            elif symbol == 'CRUDEOIL': result['symbol'] = 'CRUDEOILM'
            else: result['symbol'] = symbol
            break
        
        
        # If no known symbol, try stock pattern
        if not result.get('symbol'):
            stock_match = re.search(r'\b([A-Z]{2,20})\s*(\d{3,6})\s*(CE|PE)\b', message, re.IGNORECASE)
            if stock_match:
                result['symbol'] = stock_match.group(1).upper()
                result['is_stock_option'] = True
        # 3. Strike Price - FIXED to handle "5300. PE" or "5300-PE"
        # Strike
        # Old: strike_match = re.search(r'\b(\d{4,6})\s*(CE|PE)\b', message, re.IGNORECASE)
        # New: Handles 5300. PE or 5300PE or 5300-PE
        strike_match = re.search(r'\b(\d{4,6})[\.\-\s]*(CE|PE)\b', message, re.IGNORECASE)
        
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
        sl_match = re.search(r'(?:SL|STOPLOSS|STOP LOSS)\s*[-:]*\s*(\d+\.?\d*)', message, re.IGNORECASE)
        if sl_match:
            result['stop_loss'] = float(sl_match.group(1))
        
        # Targets
        target_match = re.search(r'(?:TARGET|TGT|T)\s*[-:]*\s*([\d/,.\s]+)', message, re.IGNORECASE)
        if target_match:
            targets_str = target_match.group(1)
            targets = re.findall(r'\d+\.?\d*', targets_str)
            result['targets'] = [float(t) for t in targets[:3]]  # Max 3 targets
        
        # Quantity
        qty_match = re.search(r'(\d+)\s*(?:LOT|LOTS|QTY|QUANTITY)', message, re.IGNORECASE)
        if qty_match:
            result['quantity'] = int(qty_match.group(1))
        
        return result if result else None
    
    def _extract_with_claude(self, message):
        """Use Claude API as fallback"""
        try:
            prompt = f"""Extract trading signal information from this message. Return ONLY a JSON object with these exact fields:

{{
  "action": "BUY or SELL",
  "symbol": "NIFTY, BANKNIFTY, GOLD, etc.",
  "strike": 25900 (number),
  "option_type": "CE or PE",
  "entry_price": 140 (number),
  "stop_loss": 130 (number),
  "targets": [150, 160, 170] (array of numbers),
  "quantity": 1 (number, if mentioned)
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
                    'messages': [{
                        'role': 'user',
                        'content': prompt
                    }]
                },
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                text = data['content'][0]['text']
                
                # Extract JSON from response
                json_match = re.search(r'\{.*\}', text, re.DOTALL)
                if json_match:
                    result = json.loads(json_match.group())
                    self.logger.info("[CLAUDE] Successfully parsed with API")
                    return result
            
            self.logger.warning(f"[CLAUDE] API call failed: {response.status_code}")
            return None
            
        except Exception as e:
            self.logger.error(f"[CLAUDE] Error: {e}")
            return None
    
    def _has_minimum_fields(self, result):
        """Check if has minimum fields for enrichment"""
        if not result:
            return False
        
        # Must have at least symbol, strike, option_type
        return all(result.get(field) for field in ['symbol', 'strike', 'option_type'])
    
    def _enrich_with_instrument_data(self, result):
        """Enrich signal with instrument data"""
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
            # Add full tradingsymbol
            result['tradingsymbol'] = instrument['symbol']
            
            # Add expiry if missing
            if not result.get('expiry_date'):
                result['expiry_date'] = instrument['expiry_date']
                result['expiry_auto_added'] = True
            
            # Add exchange
            result['exchange'] = instrument['exchange']
            
            # Add quantity if missing
            if not result.get('quantity'):
                result['quantity'] = self.instrument_finder.get_default_quantity(result['symbol'])
                result['quantity_auto_added'] = True
            
            self.logger.info(f"[ENRICH] Added: tradingsymbol={instrument['symbol']}, " +
                           f"expiry={instrument['expiry_date']}, qty={result.get('quantity')}")
        else:
            self.logger.warning(f"[WARN] Instrument not found: {result['symbol']} {result['strike']} {result['option_type']}")
        
        return result
    
    def _validate_complete(self, result):
        """Final validation - all required fields present"""
        required = [
            'symbol', 'strike', 'option_type', 'action', 
            'entry_price', 'stop_loss', 'expiry_date', 'quantity'
        ]
        
        missing = [f for f in required if not result.get(f)]
        
        if missing:
            self.logger.warning(f"[MISSING] Required fields: {missing}")
            return False
        
        # Type validation
        try:
            result['strike'] = int(float(result['strike']))
            result['entry_price'] = float(result['entry_price'])
            result['stop_loss'] = float(result['stop_loss'])
            result['quantity'] = int(result['quantity'])
            result['option_type'] = result['option_type'].upper()
            result['action'] = result['action'].upper()
            
            if result['option_type'] not in ['CE', 'PE']:
                return False
            
            if result['action'] not in ['BUY', 'SELL']:
                return False
            
            return True
            
        except (ValueError, TypeError) as e:
            self.logger.error(f"[ERROR] Type validation failed: {e}")
            return False
