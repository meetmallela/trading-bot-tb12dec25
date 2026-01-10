"""
test_stock_options.py - Test stock option parsing
"""

from signal_parser_enhanced_v2 import EnhancedSignalParser
import json
import logging

logging.basicConfig(level=logging.INFO)

parser = EnhancedSignalParser()

# Test messages
test_cases = [
    "EICHERMOT 7300CE BUY :- 140 ABOVE\nSL :- 130 TARGET :- 155/178/198++",
    "DIXON 14000 CE\nABOVE  145\nTARGET 170,190\nSL 135",
    "RELIANCE 2500CE BUY @ 45\nSL 40\nTARGET 50/55/60",
    "TCS 3500PE SELL :- 120\nSL :- 130\nTARGET :- 100/95/90",
]

print("="*80)
print("TESTING STOCK OPTION PARSING")
print("="*80)

for i, msg in enumerate(test_cases, 1):
    print(f"\n{i}. Message:\n{msg}\n")
    
    parsed = parser.parse(msg)
    
    if parsed:
        print("✓ PARSED:")
        print(json.dumps(parsed, indent=2))
    else:
        print("✗ FAILED TO PARSE")
    
    print("-"*80)
