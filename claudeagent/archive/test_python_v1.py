from premium_channel_agent_v2 import PremiumChannelAgentV2

# Load API key
with open('claude_api_key.txt', 'r') as f:
    api_key = f.read().strip()

# Initialize agent
print("Initializing agent...")
agent = PremiumChannelAgentV2(
    claude_api_key=api_key,
    instruments_csv='valid_instruments.csv',
    rulebook='channel_rulebook.txt'
)

print("\nAgent ready!")
print(agent.get_statistics())

# Test parsing
print("\nTest 1: NIFTY without symbol name")
signal = agent.parse_signal("26150PE BUY 135 SL 120")
if signal:
    print(f"  Symbol: {signal['symbol']}")
    print(f"  Expiry: {signal['expiry_date']}")
    print(f"  Tradingsymbol: {signal['tradingsymbol']}")
else:
    print("  Failed to parse")

print("\nTest 2: BANKNIFTY without symbol name")
signal = agent.parse_signal("58200CE BUY 180 SL 160")
if signal:
    print(f"  Symbol: {signal['symbol']}")
    print(f"  Expiry: {signal['expiry_date']}")
    print(f"  Tradingsymbol: {signal['tradingsymbol']}")
else:
    print("  Failed to parse")

print("\nTest 3: SENSEX without symbol name")
signal = agent.parse_signal("84900PE BUY 312 SL 278")
if signal:
    print(f"  Symbol: {signal['symbol']}")
    print(f"  Expiry: {signal['expiry_date']}")
    print(f"  Tradingsymbol: {signal['tradingsymbol']}")
else:
    print("  Failed to parse")

print("\nAll tests complete!")