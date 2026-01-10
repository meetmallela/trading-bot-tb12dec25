import re
import json
import logging
from datetime import datetime
import pandas as pd
import requests
from instrument_finder_df import InstrumentFinderDF

class SignalParserWithClaudeFallback:
    def __init__(self, claude_api_key, rules_file='parsing_rules_enhanced_v2.json'):
        self.logger = logging.getLogger('PARSER')
        self.claude_api_key = claude_api_key
        self.instrument_finder = InstrumentFinderDF('valid_instruments.csv')
        try:
            with open(rules_file, 'r') as f:
                self.rules = json.load(f)
        except:
            self.rules = {}

    def parse(self, message, channel_id=None):
        msg_clean = message.strip()
        self.logger.info(f"[RAW] {msg_clean[:60]}...")

        if self._should_ignore(msg_clean):
            return None

        # 1. Regex Extraction (Flexible Hinglish)
        result = self._extract_with_regex(msg_clean)

        # 2. Claude Fallback if Regex failed to find basics
        essentials = ['symbol', 'strike', 'option_type', 'entry_price']
        if not result or not all(result.get(k) for k in essentials):
            # Only call Claude if it's likely a trade
            if any(word in msg_clean.upper() for word in ['BUY', 'SELL', 'CE', 'PE', 'CALL', 'PUT']):
                self.logger.info("[FALLBACK] Asking Claude for help...")
                result = self._extract_with_claude(msg_clean)
            else:
                return None

        if not result: return None

        # 3. Enrich with CSV Data (Nearest Expiry + Lot Size)
        result = self._enrich_from_csv(result)
        
        # 4. Final Relaxed Validation
        if self._validate_relaxed(result):
            return result
        return None

    def _extract_with_regex(self, message):
        res = {}
        m = message.upper()
        
        # Dynamic Symbol Search
        symbols = sorted(self.instrument_finder.df['symbol'].unique(), key=len, reverse=True)
        for s in symbols:
            if re.search(r'\b' + re.escape(s) + r'\b', m):
                res['symbol'] = s
                break
        
        # Strike & Type
        st = re.search(r'(\d{4,6})[\.\-\s]*(CE|PE|CALL|PUT)', m)
        if st:
            res['strike'] = int(st.group(1))
            res['option_type'] = 'CE' if st.group(2) in ['CE', 'CALL'] else 'PE'
        
        # Price (ABOVE/NEAR)
        pr = re.search(r'(?:ABOVE|NEAR|LEVEL|AT|:-|BUY)\s*[-:]*\s*(\d+\.?\d*)', m)
        if pr: res['entry_price'] = float(pr.group(1))
        
        res['action'] = "BUY" if "BUY" in m or "CALL" in m or "PUT" in m else None
        return res

    def _enrich_from_csv(self, result):
        if not result.get('symbol'): return result
        df = self.instrument_finder.df
        mask = (df['symbol'] == result['symbol']) & (df['strike'] == result.get('strike', 0))
        matches = df[mask].copy()
        
        if not matches.empty:
            matches['expiry_date'] = pd.to_datetime(matches['expiry_date'])
            future = matches[matches['expiry_date'] >= pd.Timestamp.now().normalize()].sort_values('expiry_date')
            if not future.empty:
                nearest = future.iloc[0]
                result['expiry_date'] = nearest['expiry_date'].strftime('%Y-%m-%d')
                result['lot_size'] = int(nearest['lot_size'])
                result['quantity'] = result.get('quantity', result['lot_size'])
        return result

    def _validate_relaxed(self, result):
        required = ['symbol', 'strike', 'option_type', 'entry_price']
        if not all(result.get(k) for k in required): return False
        if not result.get('stop_loss'):
            result['stop_loss'] = result['entry_price'] * 0.80 # 20% Default
        return True

    def _should_ignore(self, msg):
        # Add your list of hype words to ignore
        ignore_words = ["PROFITS", "DONE FOR THE DAY", "SCREENSHOT", "++", "TARGET ACHIEVED"]
        return any(word in msg.upper() for word in ignore_words)

    def _extract_with_claude(self, message):
        url = "https://api.anthropic.com/v1/messages"
        headers = {
            "x-api-key": self.claude_api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        }
        data = {
            "model": "claude-3-haiku-20240307",
            "max_tokens": 500,
            "messages": [{"role": "user", "content": f"Extract trading signal as JSON (symbol, strike, option_type, action, entry_price). Today is {datetime.now().date()}. Msg: {message}"}]
        }
        try:
            response = requests.post(url, headers=headers, json=data)
            text = response.json()['content'][0]['text']
            return json.loads(re.search(r'\{.*\}', text, re.DOTALL).group())
        except:
            return None