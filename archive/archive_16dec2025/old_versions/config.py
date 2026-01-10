"""
config.py - Configuration file for trading bot
This is a simple Python config file (alternative to config.yml)
"""

# ==================== KITE API ====================
# Your Zerodha Kite API credentials (if needed)
#API_KEY = "your-kite-api-key-here"
API_KEY = "olcwzq15suerys7u"
API_SECRET = "5ep60dw8pf4kq4u83un461ugq0lt4qnc"

# ==================== CLAUDE API ====================
# Your Anthropic Claude API key
CLAUDE_API_KEY = "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"

# ==================== TELEGRAM ====================
# Your Telegram API credentials
TELEGRAM_API_ID = 25677420
TELEGRAM_API_HASH = "3fe3d6d76fdffd005104a5df5db5ba6f"
TELEGRAM_PHONE = "+919833459174"

# Channels to monitor
MONITORED_CHANNELS = [
    -1002498088029,  # RJ - STUDENT PRACTICE CALLS
    -1002770917134,  # MCX PREMIUM
    -1002842854743,  # VIP RJ Paid Education Purpose
    -1003089362819,  # Paid Premium group
    -1001903138387,  # COPY MY TRADES BANKNIFTY
    -1002380215256,  # PREMIUM_GROUP
    -1002568252699,  # TARGET HIT CLUB
    -1002201480769,  # Trader ayushi
    -1001294857397,  # Mcx Trading King Official Group
    -1002431924245,  # MCX JACKPOT TRADING
    -1001389090145,  # Stockpro Online
    -1001456128948,  # Ashish Kyal Trading Gurukul
    -1003282204738,  # JP Paper trade - Nov-25
]

# ==================== TRADING SETTINGS ====================
# Order placement settings
ORDER_PRODUCT_TYPE = "NRML"  # or "MIS" for intraday
DEFAULT_LOT_SIZE = 50

# Stop loss settings
INITIAL_SL_PERCENT = 2.0  # Initial stop loss percentage
TRAIL_TRIGGER = 5.0       # Trail when profit reaches this %
TRAIL_STEP = 2.0          # Move SL by this % when trailing
