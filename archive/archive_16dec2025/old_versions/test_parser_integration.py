"""
test_parser_integration.py - Quick test of Claude-enhanced parser
Run this to verify everything works before integrating into your bot
"""

import logging
from signal_parser_with_claude import SignalParserWithClaude

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ⚠️ REPLACE WITH YOUR API KEY
API_KEY = "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"

def test_parser():
    """Test the parser with various signal formats"""
    
    print("=" * 80)
    print("TESTING CLAUDE-ENHANCED SIGNAL PARSER")
    print("=" * 80)
    
    # Initialize parser
    parser = SignalParserWithClaude(
        claude_api_key=API_KEY,
        instruments_csv_path=None  # No instruments file for now
    )
    
    # Test cases covering different formats
    test_cases = [
        # Standard format (should use regex)
        {
            "name": "Standard NIFTY call",
            "message": "BUY NIFTY 24200 CE @ 165 SL 150 TGT 180, 195",
            "expect_regex": True
        },
        
        # Complex format (should fallback to Claude)
        {
            "name": "Complex CRUDEOIL format",
            "message": "BUY CRUDEOIL 5500 PE NEAR LEVEL - 205 TARGET - 255/270 STOPLOSS - 195 EXPIRY - DECEMBER",
            "expect_regex": False
        },
        
        # Unusual format (should fallback to Claude)
        {
            "name": "Conversational format",
            "message": "Buy Nifty 24500 call option around 150 rupees, keep stop loss at 140, targets are 170 and 190",
            "expect_regex": False
        },
        
        # BANKNIFTY (should use regex)
        {
            "name": "BANKNIFTY put",
            "message": "SELL BANKNIFTY 48000 PE @ 200 SL 220 TGT 180, 160",
            "expect_regex": True
        },
        
        # Missing some info (Claude should handle better)
        {
            "name": "Partial information",
            "message": "NIFTY 24300 CE looks good at 155, target 180",
            "expect_regex": False
        }
    ]
    
    results = []
    
    for i, test in enumerate(test_cases, 1):
        print(f"\n{'=' * 80}")
        print(f"TEST {i}: {test['name']}")
        print(f"{'=' * 80}")
        print(f"Message: {test['message']}")
        print(f"Expected parser: {'Regex' if test['expect_regex'] else 'Claude API'}")
        print("-" * 80)
        
        # Parse the signal
        parsed = parser.parse(test['message'])
        
        if parsed:
            print("✅ SUCCESS")
            print(f"Action: {parsed.get('action')}")
            print(f"Symbol: {parsed.get('symbol')}")
            print(f"Strike: {parsed.get('strike')}")
            print(f"Option Type: {parsed.get('option_type')}")
            print(f"Entry Price: {parsed.get('entry_price')}")
            print(f"Stop Loss: {parsed.get('stop_loss')}")
            print(f"Targets: {parsed.get('targets')}")
            results.append("✅")
        else:
            print("❌ FAILED - Could not parse signal")
            results.append("❌")
    
    # Summary
    print(f"\n{'=' * 80}")
    print("SUMMARY")
    print(f"{'=' * 80}")
    
    stats = parser.get_stats()
    print(f"Total signals: {stats['total']}")
    print(f"Regex success: {stats['regex_success']} ({stats.get('regex_rate', '0%')})")
    print(f"Claude success: {stats['claude_success']} ({stats.get('claude_rate', '0%')})")
    print(f"Failed: {stats['failed']}")
    print(f"Overall success rate: {stats.get('success_rate', '0%')}")
    
    print(f"\nResults: {' '.join(results)}")
    
    # Pass/fail determination
    success_count = results.count("✅")
    if success_count == len(test_cases):
        print("\n🎉 ALL TESTS PASSED!")
        print("✅ Parser is working correctly")
        print("✅ Ready to integrate into your trading bot")
    elif success_count >= len(test_cases) * 0.8:
        print(f"\n⚠️  MOSTLY PASSED ({success_count}/{len(test_cases)})")
        print("Parser is working but may need tuning")
    else:
        print(f"\n❌ TESTS FAILED ({success_count}/{len(test_cases)})")
        print("Please check API key and configuration")
    
    print("=" * 80)


if __name__ == "__main__":
    if API_KEY == "your-api-key-here":
        print("❌ ERROR: Please set your API_KEY in this script first!")
        print("Get your key from: https://console.anthropic.com/settings/keys")
        exit(1)
    
    test_parser()
