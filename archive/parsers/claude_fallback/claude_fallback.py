"""
Claude API Fallback Parser - FIXED VERSION
Uses Claude API when regex parsing fails
"""

import logging
from anthropic import Anthropic
from datetime import datetime
from typing import Optional
import json
from .base_parser import ParsedSignal

logger = logging.getLogger(__name__)


class ClaudeFallbackParser:
    """Fallback parser using Claude API"""
    
    def __init__(self, api_key: str, model: str = "claude-sonnet-4-20250514", 
                 timeout: int = 3, instruments_list: list = None):
        # Initialize Anthropic client without proxies parameter
        self.client = Anthropic(api_key=api_key)
        self.model = model
        self.timeout = timeout
        self.instruments_list = instruments_list or []
    
    def parse(self, message: str, channel: str, timestamp: datetime = None) -> Optional[ParsedSignal]:
        """
        Parse message using Claude API
        
        Args:
            message: Raw telegram message
            channel: Channel name
            timestamp: Message timestamp
        
        Returns:
            ParsedSignal or None
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        try:
            logger.info(f"[ClaudeAPI] Attempting fallback parsing for {channel}")
            
            # Create prompt for Claude
            prompt = self._create_parsing_prompt(message)
            
            # Call Claude API with timeout
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1000,
                messages=[{
                    "role": "user",
                    "content": prompt
                }],
                timeout=float(self.timeout)
            )
            
            # Parse Claude's response
            content = response.content[0].text
            signal = self._parse_claude_response(content, message, channel, timestamp)
            
            if signal:
                logger.info(f"[ClaudeAPI] Successfully parsed: {signal.underlying} {signal.strike} {signal.option_type}")
            else:
                logger.warning(f"[ClaudeAPI] Could not extract valid signal from Claude response")
            
            return signal
            
        except Exception as e:
            logger.error(f"[ClaudeAPI] Error: {e}")
            return None
    
    def _create_parsing_prompt(self, message: str) -> str:
        """Create parsing prompt for Claude"""
        # Get unique instrument names for context
        instruments_str = ""
        if self.instruments_list and len(self.instruments_list) > 0:
            unique_instruments = set([inst.get('underlying', '') for inst in self.instruments_list[:100] if inst.get('underlying')])
            instruments_str = ", ".join(sorted(list(unique_instruments))[:50])
        
        prompt = f"""Extract trading signal from this Telegram message. Respond ONLY with a JSON object, no other text.

Message:
{message}

Valid instruments include: {instruments_str or 'NIFTY, SENSEX, BANKNIFTY, FINNIFTY, and various stocks'}

Extract:
1. underlying: Instrument name (NIFTY, SENSEX, INDIGO, etc)
2. strike: Strike price (number)
3. option_type: CE or PE
4. entry_price: Entry price (if range like 115/120, take lower value)
5. stop_loss: Stop loss price (null if not mentioned)
6. targets: Array of target prices (empty array if none)
7. expiry_date: Date in YYYY-MM-DD format (null if not mentioned)

Rules:
- "ABOVE 120" means entry at 120
- "NEAR LEVEL 115" means entry at 115
- "BUY :- 115/120" means entry at 115 (lower value)
- Strike prices are 4-6 digits
- Option type is CE or PE

JSON format only:
{{
  "underlying": "NIFTY",
  "strike": 26000,
  "option_type": "PE",
  "entry_price": 115,
  "stop_loss": 100,
  "targets": [160, 170, 200],
  "expiry_date": "2025-12-09"
}}

If you cannot extract a valid signal, respond with: {{"error": "Cannot parse"}}"""
        
        return prompt
    
    def _parse_claude_response(self, response: str, raw_message: str, 
                               channel: str, timestamp: datetime) -> Optional[ParsedSignal]:
        """Parse Claude's JSON response into ParsedSignal"""
        try:
            # Remove markdown code blocks if present
            response = response.strip()
            if response.startswith('```'):
                response = response.split('```')[1]
                if response.startswith('json'):
                    response = response[4:]
            response = response.strip()
            
            # Parse JSON
            data = json.loads(response)
            
            # Check for error
            if 'error' in data:
                return None
            
            # Validate required fields
            required_fields = ['underlying', 'strike', 'option_type', 'entry_price']
            if not all(field in data for field in required_fields):
                logger.warning("[ClaudeAPI] Missing required fields in response")
                return None
            
            # Parse expiry date
            expiry_date = None
            if data.get('expiry_date'):
                try:
                    expiry_date = datetime.strptime(data['expiry_date'], '%Y-%m-%d')
                except:
                    pass
            
            # Create signal
            signal = ParsedSignal(
                underlying=data['underlying'].upper(),
                strike=float(data['strike']),
                option_type=data['option_type'].upper(),
                entry_price=float(data['entry_price']),
                stop_loss=float(data['stop_loss']) if data.get('stop_loss') else None,
                targets=[float(t) for t in data.get('targets', [])],
                expiry_date=expiry_date,
                raw_message=raw_message,
                channel=channel,
                timestamp=timestamp
            )
            
            return signal
            
        except json.JSONDecodeError as e:
            logger.error(f"[ClaudeAPI] JSON parse error: {e}")
            return None
        except Exception as e:
            logger.error(f"[ClaudeAPI] Response parse error: {e}")
            return None
