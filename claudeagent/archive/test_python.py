from premium_channel_agent import PremiumChannelAgent
agent = PremiumChannelAgent(
        claude_api_key="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",
        instruments_csv="valid_instruments.csv" )
signal = agent.parse_signal("NIFTY 26150PE BUY 135 SL 120")
print(signal)