"""
STEP 3: Test Parsers
Test the parsers with your actual messages
"""

import sys
import os
from datetime import datetime

# Sample messages from your channels
TEST_MESSAGES = {
    'MCX_PREMIUM': [
        """BUY - NIFTY  26000 PE
NEAR LEVEL --   115
TARGET -  160 / 170/200
STOPLOSS -- 100
EXPIRY -  09 DECEMBER""",
        
        """[2025-12-08 06:27:58]
INDIGO    4700 PE
ABOVE  120
TARGET 150,170
SL 102
#STOCK_OPTIONS""",
    ],
    
    'VIP_RJ': [
        """[2025-12-08 08:43:15]
**SENSEX 85200CE BUY :- 342 ABOVE 
SL :- 330 TARGET :- 370/450/600++**""",
        
        """[2025-12-08 06:46:11]
**NIFTY 26200PE BUY :- 115/120
SL :- 105 TARGET :- 138/158/178++**""",
    ],
    
    'RJ_STUDENT': [
        """**INDIGO DEC 4700PE BUY :- 135 ABOVE 
SL :- 122 TARGET :- 148/178/198++**""",
        
        """**HEROMOTOCO 6200PE BUY :- 112 ABOVE 
SL :- 100 TARGET :- 122/138/158++**""",
    ]
}

def test_parsers():
    """Test all parsers"""
    
    print("\n" + "="*80)
    print("PARSER TESTER")
    print("="*80)
    
    # Add parsers to path
    sys.path.insert(0, os.getcwd())
    
    try:
        from parsers.base_parser import BaseParser
        from parsers.parser_channel1 import Channel1Parser
        from parsers.parser_channel2 import Channel2Parser
        from parsers.parser_channel3 import Channel3Parser
    except ImportError as e:
        print(f"ERROR: Could not import parsers: {e}")
        print("Make sure all parser files are in the parsers/ folder")
        return
    
    # Expiry rules
    expiry_rules = {
        'NIFTY': {'type': 'weekly', 'day': 'Thursday'},
        'SENSEX': {'type': 'weekly', 'day': 'Friday'},
        'default': {'type': 'monthly'}
    }
    
    # Create parsers
    parsers = {
        'MCX_PREMIUM': Channel1Parser(expiry_rules),
        'VIP_RJ': Channel2Parser(expiry_rules),
        'RJ_STUDENT': Channel3Parser(expiry_rules)
    }
    
    print("\nTesting parsers with sample messages...\n")
    
    total_tests = 0
    passed_tests = 0
    
    for channel, messages in TEST_MESSAGES.items():
        print(f"\n{'='*80}")
        print(f"CHANNEL: {channel}")
        print('='*80)
        
        parser = parsers[channel]
        
        for i, message in enumerate(messages, 1):
            total_tests += 1
            print(f"\nTest {i}:")
            print(f"Message: {message[:80]}...")
            
            # Parse
            signal = parser.parse(message, datetime.now())
            
            if signal:
                passed_tests += 1
                print(f"✅ PASS - Parsed successfully")
                print(f"   Underlying: {signal.underlying}")
                print(f"   Strike: {signal.strike}")
                print(f"   Type: {signal.option_type}")
                print(f"   Entry: {signal.entry_price}")
                print(f"   SL: {signal.stop_loss}")
                print(f"   Targets: {signal.targets}")
            else:
                print(f"❌ FAIL - Could not parse")
    
    print(f"\n{'='*80}")
    print(f"RESULTS: {passed_tests}/{total_tests} tests passed ({100*passed_tests/total_tests:.0f}%)")
    print('='*80)
    
    if passed_tests == total_tests:
        print("\n✅ All parsers working! You can now run the main system.")
    else:
        print(f"\n⚠️  {total_tests - passed_tests} tests failed. Check the parsers.")

if __name__ == "__main__":
    test_parsers()
