"""
Check why signals are failing to parse
Shows recent messages from telegram_reader.log
"""

import re

print("="*80)
print("RECENT MESSAGES THAT FAILED TO PARSE")
print("="*80)

try:
    with open('telegram_reader.log', 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Find recent messages with PREVIEW
    messages = []
    i = 0
    while i < len(lines):
        line = lines[i]
        
        if '[PREVIEW]' in line:
            # Extract preview
            preview = line.split('[PREVIEW]')[1].strip()
            
            # Check if it was skipped
            if i + 2 < len(lines):
                next_line = lines[i+2]
                if '[✗ SKIP]' in next_line or '[INCOMPLETE]' in next_line or '[REJECT]' in next_line:
                    messages.append(preview)
        
        i += 1
    
    # Show last 10 failed messages
    print(f"\nFound {len(messages)} failed messages")
    print("\nLast 10 messages that failed:")
    print("-"*80)
    
    for i, msg in enumerate(messages[-10:], 1):
        print(f"\n{i}. {msg[:150]}")
    
    print("\n" + "="*80)
    print("ANALYSIS")
    print("="*80)
    
    # Check patterns
    trading_signals = 0
    noise = 0
    
    for msg in messages[-50:]:  # Check last 50
        msg_upper = msg.upper()
        
        # Check if looks like trading signal
        has_ce_pe = 'CE' in msg_upper or 'PE' in msg_upper
        has_buy_sell = 'BUY' in msg_upper or 'SELL' in msg_upper
        has_sl = 'SL' in msg_upper or 'STOP' in msg_upper
        has_target = 'TARGET' in msg_upper or 'TGT' in msg_upper
        
        if (has_ce_pe or has_buy_sell) and (has_sl or has_target):
            trading_signals += 1
        else:
            noise += 1
    
    print(f"\nOut of last 50 messages:")
    print(f"  Looks like trading signals: {trading_signals}")
    print(f"  Looks like noise/spam: {noise}")
    
    if trading_signals > 20:
        print("\n⚠️  PROBLEM: Many trading signals are being rejected!")
        print("   → Parser or enrichment is failing")
        print("   → Check instruments CSV")
    elif noise > 40:
        print("\n✅ EXPECTED: Most messages are just spam/noise")
        print("   → Parser is working correctly")
        print("   → Just waiting for real signals")

except FileNotFoundError:
    print("❌ telegram_reader.log not found")
except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "="*80)
print("NEXT STEPS")
print("="*80)
print("""
1. If many real signals are failing:
   → Fix instrument_finder_df.py (download updated version)
   → Restart telegram reader

2. If mostly noise:
   → System is working correctly
   → Just waiting for real trading signals

3. Check a specific message:
   → Copy a failed message here
   → I'll tell you exactly why it failed
""")
