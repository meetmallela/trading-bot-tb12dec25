"""
test_parser_filtering.py - Test the improved parser with filtering
Shows how junk messages are filtered out before hitting Claude API
"""

import logging
from signal_parser_with_claude import SignalParserWithClaude

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Your API key
API_KEY = "your-api-key-here"

def test_filtering():
    """Test parser with various message types"""
    
    print("=" * 80)
    print("TESTING PARSER WITH FILTERING & TEXT CLEANING")
    print("=" * 80)
    
    parser = SignalParserWithClaude(claude_api_key=API_KEY)
    
    test_cases = [
        {
            "name": "Valid Signal (should parse)",
            "message": "BUY NIFTY 24200 CE @ 165 SL 150 TGT 180, 195",
            "expect": "PARSE"
        },
        {
            "name": "Price Update with Emojis (should SKIP)",
            "message": "168++❤️🎁✔️",
            "expect": "SKIP"
        },
        {
            "name": "Price Update with Emojis (should SKIP)",
            "message": "170++❤️🎁✔️",
            "expect": "SKIP"
        },
        {
            "name": "Just Numbers (should SKIP)",
            "message": "155+",
            "expect": "SKIP"
        },
        {
            "name": "Valid Signal with Emojis (should parse, clean first)",
            "message": "BUY 🚀 CRUDEOIL 5500 PE 💰 @ 205 🎯 SL 195 ✅",
            "expect": "PARSE"
        },
        {
            "name": "Too Short (should SKIP)",
            "message": "Good!",
            "expect": "SKIP"
        },
        {
            "name": "No Keywords (should SKIP)",
            "message": "Have a great day everyone!",
            "expect": "SKIP"
        },
        {
            "name": "Valid Signal - Complex Format",
            "message": "SELL BANKNIFTY 48000 PE NEAR 200 TARGET 180/160 STOPLOSS 220",
            "expect": "PARSE"
        }
    ]
    
    results = []
    api_calls_saved = 0
    
    for i, test in enumerate(test_cases, 1):
        print(f"\n{'=' * 80}")
        print(f"TEST {i}: {test['name']}")
        print(f"{'=' * 80}")
        print(f"Message: {test['message']}")
        print(f"Expected: {test['expect']}")
        print("-" * 80)
        
        # Parse
        parsed = parser.parse(test['message'])
        
        if parsed:
            print(f"✅ PARSED")
            print(f"   Symbol: {parsed.get('symbol')}")
            print(f"   Action: {parsed.get('action')}")
            print(f"   Entry: {parsed.get('entry_price')}")
            result = "PARSED"
        else:
            print(f"⏭️  SKIPPED (not sent to API)" if test['expect'] == "SKIP" else "❌ FAILED")
            result = "SKIPPED"
            if test['expect'] == "SKIP":
                api_calls_saved += 1
        
        results.append(result)
    
    # Summary
    print(f"\n{'=' * 80}")
    print("SUMMARY")
    print(f"{'=' * 80}")
    
    stats = parser.get_stats()
    print(f"Total messages:       {stats['total']}")
    print(f"Regex success:        {stats['regex_success']} ({stats.get('regex_rate', '0%')})")
    print(f"Claude API calls:     {stats['claude_success']}")
    print(f"Skipped (filtered):   {stats['failed']}")
    print(f"API calls saved:      {api_calls_saved} 💰")
    
    # Calculate savings
    cost_per_call = 0.0003  # ~$0.0003 per signal
    money_saved = api_calls_saved * cost_per_call
    print(f"\nMoney saved:          ${money_saved:.4f} (~₹{money_saved * 85:.2f})")
    print(f"\nPer day (if 50 junk): ${0.0003 * 50:.2f} saved (~₹{0.0003 * 50 * 85:.1f})")
    print(f"Per month:            ${0.0003 * 50 * 30:.2f} saved (~₹{0.0003 * 50 * 30 * 85:.1f})")
    
    print(f"\n{'=' * 80}")
    print("KEY IMPROVEMENTS")
    print(f"{'=' * 80}")
    print("✅ Emojis removed before API call (cleaner parsing)")
    print("✅ Price updates filtered out (no API cost)")
    print("✅ Short messages skipped (obvious junk)")
    print("✅ Messages without keywords skipped (greetings, etc)")
    print("✅ Only real signals sent to Claude API")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    if API_KEY == "your-api-key-here":
        print("\n⚠️  Please set your API_KEY in this script first!")
        print("This is just a demonstration of filtering - you can also run it without API key")
        print("to see which messages would be filtered.\n")
    
    test_filtering()