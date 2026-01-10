"""
premium_channel_agent_v2.py
Enhanced Claude Agent with Knowledge Base & Learning
- Correct expiry rules per index
- Weekly instrument caching
- Human correction learning
- Pattern recognition
"""

import json
import logging
import pickle
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from anthropic import Anthropic
import pandas as pd

class PremiumChannelAgentV2:
    """Enhanced agent with KB and learning capabilities"""
    
    # Expiry rules for each index
    EXPIRY_RULES = {
        'NIFTY': {'weekday': 3, 'name': 'Thursday'},      # 0=Monday, 3=Thursday
        'BANKNIFTY': {'weekday': 2, 'name': 'Wednesday'},
        'FINNIFTY': {'weekday': 1, 'name': 'Tuesday'},
        'MIDCPNIFTY': {'weekday': 0, 'name': 'Monday'},
        'SENSEX': {'weekday': 4, 'name': 'Friday'},
        'BANKEX': {'weekday': None, 'name': 'Monthly'},
        'CRUDEOILM': {'weekday': None, 'name': 'Monthly'},
        'GOLDM': {'weekday': None, 'name': 'Monthly'},
    }
    
    def __init__(self, claude_api_key, instruments_csv='valid_instruments.csv', kb_db='agent_kb.db', rulebook='channel_rulebook.txt'):
        self.logger = logging.getLogger('PREMIUM_AGENT_V2')
        self.client = Anthropic(api_key=claude_api_key)
        self.kb_db_path = kb_db
        
        # Load rulebook
        self.rulebook = self._load_rulebook(rulebook)
        
        # Initialize KB database
        self._init_kb_database()
        
        # Load instruments (with caching)
        self.logger.info("[INIT] Loading instruments...")
        self.instruments = self._load_instruments_cached(instruments_csv)
        
        # Create context for Claude
        self.instrument_context = self._create_instrument_context()
        
        # Load human corrections (KB)
        self.corrections = self._load_corrections()
        
        self.logger.info(f"[OK] Agent V2 initialized")
        self.logger.info(f"     Rulebook: {'Loaded' if self.rulebook else 'Not found'}")
        self.logger.info(f"     Instruments: {len(self.instruments)}")
        self.logger.info(f"     Corrections KB: {len(self.corrections)}")
    
    def _init_kb_database(self):
        """Initialize knowledge base database"""
        conn = sqlite3.connect(self.kb_db_path)
        
        # Table for human corrections
        conn.execute('''
            CREATE TABLE IF NOT EXISTS parsing_corrections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_message TEXT,
                claude_parsed JSON,
                human_corrected JSON,
                timestamp TEXT,
                channel_id TEXT,
                notes TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        self.logger.info(f"[OK] KB database: {self.kb_db_path}")
    
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
        cache_file = 'instruments_cache.pkl'
        cache_path = Path(cache_file)
        
        # Check if cache exists and is < 7 days old
        if cache_path.exists():
            cache_age = datetime.now() - datetime.fromtimestamp(cache_path.stat().st_mtime)
            if cache_age.days < 7:
                self.logger.info(f"[CACHE] Loading from cache (age: {cache_age.days} days)")
                with open(cache_file, 'rb') as f:
                    return pickle.load(f)
        
        # Load fresh from CSV
        self.logger.info("[LOADING] Fresh load from CSV...")
        try:
            df = pd.read_csv(csv_path)
            
            # Filter for index options - try multiple formats
            if 'instrument_type' in df.columns:
                options = df[df['instrument_type'].isin(['OPTIDX', 'INDEX_OPTION', 'OPTSTK'])].copy()
            else:
                # If no instrument_type column, use all rows
                options = df.copy()
            
            self.logger.info(f"[LOAD] Found {len(options)} option instruments")
            
            instruments_by_symbol = {}
            for symbol in options['symbol'].unique():
                symbol_data = options[options['symbol'] == symbol]
                
                # Convert expiry dates to YYYY-MM-DD format
                expiry_dates = []
                for exp in symbol_data['expiry_date'].unique():
                    try:
                        # Try DD-MM-YYYY format first
                        if '-' in str(exp) and len(str(exp).split('-')[0]) <= 2:
                            dt = datetime.strptime(str(exp), '%d-%m-%Y')
                            expiry_dates.append(dt.strftime('%Y-%m-%d'))
                        # Try YYYY-MM-DD format
                        elif '-' in str(exp):
                            dt = datetime.strptime(str(exp), '%Y-%m-%d')
                            expiry_dates.append(dt.strftime('%Y-%m-%d'))
                        else:
                            expiry_dates.append(str(exp))
                    except Exception as e:
                        self.logger.warning(f"[WARN] Failed to parse date {exp}: {e}")
                        continue
                
                instruments_by_symbol[symbol] = {
                    'strikes': sorted(symbol_data['strike'].unique().tolist()),
                    'expiries': sorted(expiry_dates),
                    'lot_size': int(symbol_data.iloc[0]['lot_size']),
                    'exchange': symbol_data.iloc[0]['exchange']
                }
            
            # Save to cache
            with open(cache_file, 'wb') as f:
                pickle.dump(instruments_by_symbol, f)
            self.logger.info(f"[CACHE] Saved to {cache_file}")
            
            return instruments_by_symbol
            
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to load instruments: {e}")
            return {}
    
    def _create_instrument_context(self):
        """Create instrument context with expiry rules"""
        context = {
            "trading_date": datetime.now().strftime("%Y-%m-%d"),
            "expiry_rules": self.EXPIRY_RULES,
            "instruments": {}
        }
        
        for symbol, data in self.instruments.items():
            context["instruments"][symbol] = {
                "lot_size": data['lot_size'],
                "exchange": data['exchange'],
                "expiries": data['expiries'],  # ALL expiries
                "expiry_rule": self.EXPIRY_RULES.get(symbol, {'weekday': None, 'name': 'Monthly'})
            }
        
        return context
    
    def _load_corrections(self, limit=20):
        """Load recent human corrections for few-shot learning"""
        conn = sqlite3.connect(self.kb_db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT raw_message, human_corrected 
            FROM parsing_corrections 
            ORDER BY timestamp DESC 
            LIMIT ?
        """, (limit,))
        
        corrections = []
        for row in cursor.fetchall():
            corrections.append({
                'message': row[0],
                'correct_parsing': json.loads(row[1])
            })
        
        conn.close()
        return corrections
    
    def _get_next_expiry_for_symbol(self, symbol):
        """Get next available expiry from CSV for a symbol"""
        today = datetime.now().date()
        
        # Check if symbol exists in instruments
        if symbol not in self.instruments:
            self.logger.warning(f"[WARN] Symbol {symbol} not in instruments")
            return None
        
        expiries = self.instruments[symbol]['expiries']
        
        if not expiries:
            return None
        
        # Find first expiry >= today
        for exp_str in expiries:
            try:
                # Handle different date formats
                if 'T' in exp_str:
                    exp_date = datetime.fromisoformat(exp_str.replace('Z', '+00:00')).date()
                else:
                    exp_date = datetime.strptime(exp_str, '%Y-%m-%d').date()
                
                if exp_date >= today:
                    return exp_str if 'T' not in exp_str else exp_date.strftime('%Y-%m-%d')
            except Exception as e:
                self.logger.warning(f"[WARN] Failed to parse expiry {exp_str}: {e}")
                continue
        
        # If no future expiry found, return first available
        return expiries[0] if expiries else None
    
    def _get_system_prompt(self):
        """System prompt with KB, rulebook, and expiry rules"""
        today = datetime.now()
        
        # Calculate next expiry for each index
        expiry_info = []
        for symbol, rule in self.EXPIRY_RULES.items():
            if rule['weekday'] is not None:
                next_exp = self._get_next_expiry_for_symbol(symbol)
                expiry_info.append(f"- {symbol}: {rule['name']} ({next_exp})")
        
        # Format human corrections as examples
        correction_examples = ""
        if self.corrections:
            correction_examples = "\n\nLEARNING FROM CORRECTIONS (use these patterns):\n"
            for i, corr in enumerate(self.corrections[:5], 1):  # Top 5
                correction_examples += f"\nExample {i}:\n"
                correction_examples += f"Message: {corr['message']}\n"
                correction_examples += f"Correct parsing: {json.dumps(corr['correct_parsing'], indent=2)}\n"
        
        # Include rulebook if available
        rulebook_section = ""
        if self.rulebook:
            rulebook_section = f"\n\nCHANNEL-SPECIFIC RULEBOOK:\n{self.rulebook}\n"
        
        return f"""You are an expert trading signal parser for Indian markets.

Today: {today.strftime('%Y-%m-%d %A')}

{rulebook_section}

AVAILABLE EXPIRIES FROM CSV (USE THESE EXACT DATES):
{chr(10).join(expiry_info)}

Available Instruments with ALL Expiries:
{json.dumps(self.instrument_context['instruments'], indent=2)}

{correction_examples}

CRITICAL INSTRUCTIONS:
1. Follow the channel rulebook rules EXACTLY
2. For expiry: SELECT from the "expiries" list in instruments - DO NOT calculate dates!
3. Use FIRST available expiry from the list (earliest date >= today)
4. Identify missing symbols using strike price ranges from rulebook
5. Learn from correction examples above

EXAMPLE:
If expiries list is ["2025-12-30", "2026-01-06", "2026-01-13"],
use "2025-12-30" (first in list)

Return ONLY valid JSON, no explanation."""
    
    def parse_signal(self, message, channel_id=None, channel_name=None):
        """Parse signal with KB"""
        self.logger.info(f"[PARSE] {message[:80]}...")
        
        prompt = f"""Parse this trading signal:

Message: {message}

Required JSON:
{{
  "symbol": "NIFTY",
  "strike": 26150,
  "option_type": "CE|PE",
  "action": "BUY|SELL",
  "entry_price": 135.0,
  "stop_loss": 120.0,
  "targets": [150, 165],
  "expiry_date": "2026-01-02",
  "quantity": 75,
  "tradingsymbol": "NIFTY26JAN26150CE",
  "exchange": "NFO"
}}"""
        
        try:
            response = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=2000,
                system=self._get_system_prompt(),
                messages=[{"role": "user", "content": prompt}]
            )
            
            text = response.content[0].text
            
            import re
            json_match = re.search(r'\{.*\}', text, re.DOTALL)
            if json_match:
                signal = json.loads(json_match.group())
                signal = self._enrich_signal(signal)
                
                if self._validate_signal(signal):
                    self.logger.info(f"[SUCCESS] {signal['symbol']} {signal['strike']} {signal['option_type']}")
                    return signal
            
            return None
            
        except Exception as e:
            self.logger.error(f"[ERROR] {e}")
            return None
    
    def save_correction(self, raw_message, claude_parsed, human_corrected, channel_id=None, notes=None):
        """Save human correction to KB"""
        conn = sqlite3.connect(self.kb_db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO parsing_corrections 
            (raw_message, claude_parsed, human_corrected, timestamp, channel_id, notes)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            raw_message,
            json.dumps(claude_parsed) if claude_parsed else None,
            json.dumps(human_corrected),
            datetime.now().isoformat(),
            channel_id,
            notes
        ))
        
        conn.commit()
        conn.close()
        
        # Reload corrections
        self.corrections = self._load_corrections()
        
        self.logger.info(f"[KB] Correction saved. Total: {len(self.corrections)}")
    
    def _enrich_signal(self, signal):
        """Enrich with tradingsymbol"""
        symbol = signal.get('symbol')
        strike = signal.get('strike')
        option_type = signal.get('option_type')
        expiry = signal.get('expiry_date')
        
        if all([symbol, strike, option_type, expiry]):
            expiry_str = datetime.strptime(expiry, '%Y-%m-%d').strftime('%y%b').upper()
            signal['tradingsymbol'] = f"{symbol}{expiry_str}{strike}{option_type}"
        
        if not signal.get('exchange') and symbol in self.instruments:
            signal['exchange'] = self.instruments[symbol]['exchange']
        
        if not signal.get('quantity') and symbol in self.instruments:
            signal['quantity'] = self.instruments[symbol]['lot_size']
        
        return signal
    
    def _validate_signal(self, signal):
        """Validate completeness"""
        required = ['symbol', 'strike', 'option_type', 'action', 'entry_price', 
                   'stop_loss', 'expiry_date', 'quantity', 'tradingsymbol', 'exchange']
        
        missing = [f for f in required if not signal.get(f)]
        if missing:
            self.logger.error(f"[MISSING] {missing}")
            return False
        
        try:
            signal['strike'] = int(signal['strike'])
            signal['entry_price'] = float(signal['entry_price'])
            signal['stop_loss'] = float(signal['stop_loss'])
            signal['quantity'] = int(signal['quantity'])
            return True
        except:
            return False
    
    def get_statistics(self):
        """Get agent statistics"""
        return {
            'instruments_loaded': len(self.instruments),
            'corrections_kb': len(self.corrections),
            'cache_status': 'active',
            'expiry_rules': len(self.EXPIRY_RULES)
        }


if __name__ == "__main__":
    # Test
    logging.basicConfig(level=logging.INFO)
    
    agent = PremiumChannelAgentV2(
        claude_api_key="your-key",
        instruments_csv='../valid_instruments.csv'
    )
    
    # Test parsing
    signal = agent.parse_signal("NIFTY 26150PE BUY 135 SL 120")
    if signal:
        print(json.dumps(signal, indent=2))
    
    # Test correction
    # agent.save_correction(
    #     raw_message="NIFTY 26150PE BUY 135",
    #     claude_parsed=signal,
    #     human_corrected={...corrected version...}
    # )
