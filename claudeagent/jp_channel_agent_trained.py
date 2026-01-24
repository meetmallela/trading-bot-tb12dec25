"""
jp_channel_agent_trained.py
Enhanced JP parser using Knowledge Base for few-shot learning
Combines regex speed with Claude intelligence + human corrections
"""

import sqlite3
import json
import logging
import re
from anthropic import Anthropic
from jp_channel_parser import JPChannelParser

class JPChannelAgentTrained:
    """JP parser enhanced with training data from human corrections"""
    
    def __init__(self, claude_api_key, kb_db='jp_kb.db', instruments_csv='valid_instruments.csv', rulebook_path='jp_channel_rulebook.txt'):
        self.logger = logging.getLogger('JP_TRAINED')
        self.claude = Anthropic(api_key=claude_api_key)
        
        # Load channel rulebook
        self.rulebook = self._load_rulebook(rulebook_path)
        
        # Initialize base parser (with caching and rulebook)
        self.base_parser = JPChannelParser(
            instruments_csv=instruments_csv,
            rulebook_path=rulebook_path
        )
        
        # Load training data from KB
        self.training_examples = self._load_training_data(kb_db)
        
        self.logger.info(f"[INIT] Trained agent ready")
        self.logger.info(f"  Rulebook: {'Loaded' if self.rulebook else 'Not found'}")
        self.logger.info(f"  Training examples: {len(self.training_examples)}")
        self.logger.info(f"  Instruments: Cached")
    
    def _load_rulebook(self, rulebook_path):
        """Load channel rulebook"""
        try:
            with open(rulebook_path, 'r', encoding='utf-8') as f:
                content = f.read()
            self.logger.info(f"[OK] Rulebook loaded: {len(content)} characters")
            return content
        except Exception as e:
            self.logger.warning(f"[WARN] Rulebook not found: {e}")
            return None
    
    def _load_training_data(self, kb_db):
        """Load training examples from KB"""
        try:
            conn = sqlite3.connect(kb_db)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT raw_message, correct_data 
                FROM training_data 
                WHERE is_valid IN ('yes', 'y', '1', 'true')
                ORDER BY timestamp DESC
                LIMIT 250
            """)
            
            examples = []
            for row in cursor.fetchall():
                examples.append({
                    'message': row[0],
                    'correct': json.loads(row[1])
                })
            
            conn.close()
            self.logger.info(f"[KB] Loaded {len(examples)} training examples")
            return examples
        
        except Exception as e:
            self.logger.warning(f"[WARN] Could not load KB: {e}")
            return []
    
    def parse(self, message, message_date=None):
        """Parse with KB-enhanced Claude"""
        
        # Try base parser first (fast, no API cost)
        result = self.base_parser.parse(message, message_date=message_date)
        if result:
            self.logger.info(f"[REGEX] Parsed: {result['symbol']} {result['strike']} {result['option_type']}")
            return result
        
        # Use Claude with training examples and rulebook
        self.logger.info(f"[CLAUDE] Using trained model with {len(self.training_examples)} examples...")
        
        # Build rulebook section
        rulebook_section = ""
        if self.rulebook:
            rulebook_section = f"\n\nCHANNEL RULEBOOK:\n{self.rulebook}\n"
        
        # Find most similar examples (simple: first 10)
        examples_text = ""
        for i, ex in enumerate(self.training_examples[:10], 1):
            examples_text += f"\nExample {i}:\n"
            examples_text += f"Message: \"{ex['message']}\"\n"
            examples_text += f"Correct parsing: {json.dumps(ex['correct'])}\n"
        
        prompt = f"""You are a trading signal parser trained on JP channel data.

{rulebook_section}

TRAINING EXAMPLES (Learn the exact patterns):
{examples_text}

KEY LEARNINGS FROM CORRECTIONS:
1. Stock options: Use last Thursday of MESSAGE month for expiry
2. NIFTY: Use nearest Thursday from MESSAGE date (not today)
3. SENSEX: Use nearest Friday from MESSAGE date (not today)
4. If no stop loss provided: CE = entry * 0.85, PE = entry * 1.15
5. Action is always BUY for this channel
6. Message date context: {message_date if message_date else 'Not provided (use current date)'}

RULES:
- Follow the channel rulebook rules EXACTLY
- Extract: symbol, strike, option_type (CE/PE), entry_price, stop_loss (if present)
- Follow the EXACT pattern from training examples above
- Match the expiry date format from examples

MESSAGE TO PARSE:
{message}

Return ONLY JSON (no explanation):
{{
  "symbol": "...",
  "strike": 0,
  "option_type": "CE",
  "entry_price": 0.0,
  "stop_loss": 0.0,
  "expiry_date": "YYYY-MM-DD",
  "action": "BUY"
}}"""
        
        try:
            response = self.claude.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}]
            )
            
            text = response.content[0].text
            
            # Extract JSON
            json_match = re.search(r'\{.*\}', text, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group())
                
                # Check if it's a valid signal (must have required fields)
                required_fields = ['symbol', 'strike', 'option_type', 'entry_price', 'expiry_date']
                missing_fields = [f for f in required_fields if f not in data]
                
                if missing_fields:
                    self.logger.info(f"[SKIP] Incomplete signal - missing: {', '.join(missing_fields)}")
                    return None
                
                # Add quantity and tradingsymbol
                data['quantity'] = self.base_parser._get_lot_size(
                    data.get('symbol', ''),
                    data.get('strike', 0),
                    data.get('option_type', 'CE')
                )
                
                # Set exchange
                symbol = data.get('symbol', '')
                if symbol in ['SENSEX', 'BANKEX']:
                    data['exchange'] = 'BFO'
                elif symbol in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']:
                    data['exchange'] = 'NFO'
                else:
                    data['exchange'] = 'NSE'
                
                # Build tradingsymbol
                from datetime import datetime
                expiry_dt = datetime.strptime(data['expiry_date'], '%Y-%m-%d')
                
                # Different format for stocks vs indices
                if symbol in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX']:
                    # Index format: NIFTY26JAN26000CE (no day)
                    expiry_str = expiry_dt.strftime('%y%b').upper()
                    data['tradingsymbol'] = f"{data['symbol']}{expiry_str}{data['strike']}{data['option_type']}"
                else:
                    # Stock format: TATASTEEL30JAN25180CE (includes day "30")
                    expiry_str = expiry_dt.strftime('%d%b%y').upper()
                    data['tradingsymbol'] = f"{data['symbol']}{expiry_str}{data['strike']}{data['option_type']}"
                
                self.logger.info(f"[SUCCESS] {data['symbol']} {data['strike']} {data['option_type']}")
                return data
        
        except Exception as e:
            self.logger.error(f"[ERROR] {e}")
            import traceback
            traceback.print_exc()
        
        return None


# Test function
def test_trained_agent():
    """Test the trained agent"""
    import sys
    
    # Get API key
    try:
        with open('claude_api_key.txt', 'r') as f:
            api_key = f.read().strip()
    except FileNotFoundError:
        print("Error: claude_api_key.txt not found")
        sys.exit(1)
    except (IOError, OSError) as e:
        print(f"Error reading claude_api_key.txt: {e}")
        sys.exit(1)
    
    # Initialize
    print("Initializing trained agent...")
    agent = JPChannelAgentTrained(api_key)
    
    # Test messages
    test_cases = [
        "CDSL 1640 CE 51.5",
        "Muthootfin 3800 CE 101",
        "Nifty 26000 CE 173",
        "26200 CE 140",
        "Persistent 6400 CE 185 SL 180",
    ]
    
    print("\nTesting trained agent:")
    print("="*70)
    
    for msg in test_cases:
        print(f"\nMessage: {msg}")
        result = agent.parse(msg, message_date="2025-12-01T10:00:00+00:00")
        
        if result:
            print(f"  ✓ {result['symbol']} {result['strike']} {result['option_type']}")
            print(f"    Entry: {result['entry_price']} | SL: {result['stop_loss']}")
            print(f"    Expiry: {result['expiry_date']} | Tradingsymbol: {result['tradingsymbol']}")
        else:
            print(f"  ✗ Failed to parse")
    
    print("\n" + "="*70)
    print("Test complete!")


if __name__ == "__main__":
    test_trained_agent()
