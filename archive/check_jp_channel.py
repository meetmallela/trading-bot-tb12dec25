"""
Check if we're receiving messages from JP Paper trade channel today
"""

import sqlite3
from datetime import datetime

# Channel ID
JP_CHANNEL_ID = "-1003282204738"
JP_CHANNEL_NAME = "JP Paper trade - Dec-25"

# Connect to database
conn = sqlite3.connect('trading.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

print("="*80)
print(f"JP PAPER TRADE CHANNEL DIAGNOSTIC")
print("="*80)
print(f"Channel ID: {JP_CHANNEL_ID}")
print(f"Channel Name: {JP_CHANNEL_NAME}")
print()

# Check all messages from this channel
cursor.execute("""
    SELECT id, timestamp, raw_text, processed
    FROM signals
    WHERE channel_id = ? OR channel_name LIKE '%JP Paper%'
    ORDER BY timestamp DESC
    LIMIT 20
""", (JP_CHANNEL_ID,))

messages = cursor.fetchall()

if not messages:
    print("❌ NO MESSAGES found from JP Paper trade channel!")
    print()
    print("Possible reasons:")
    print("1. Telegram reader is not running")
    print("2. Channel is not sending messages")
    print("3. We're not a member of this channel")
    print("4. Channel ID changed")
else:
    print(f"✅ Found {len(messages)} messages from JP Paper trade")
    print()
    print("Recent messages:")
    print("-"*80)
    
    today = datetime.now().strftime('%Y-%m-%d')
    today_count = 0
    
    for msg in messages:
        timestamp = msg['timestamp'][:19]  # YYYY-MM-DD HH:MM:SS
        date = msg['timestamp'][:10]
        
        if date == today:
            today_count += 1
            marker = "🔴 TODAY"
        else:
            marker = ""
        
        preview = msg['raw_text'][:60].replace('\n', ' ')
        status = "✓ Processed" if msg['processed'] else "⏳ Pending"
        
        print(f"[{msg['id']}] {timestamp} {marker}")
        print(f"   {preview}...")
        print(f"   Status: {status}")
        print()
    
    print("-"*80)
    print(f"Messages TODAY: {today_count}")
    print(f"Messages TOTAL: {len(messages)}")

# Check all channels we're receiving from today
print()
print("="*80)
print("ALL CHANNELS - TODAY'S ACTIVITY")
print("="*80)

cursor.execute("""
    SELECT channel_name, COUNT(*) as count
    FROM signals
    WHERE DATE(timestamp) = DATE('now')
    GROUP BY channel_name
    ORDER BY count DESC
""")

channels_today = cursor.fetchall()

if not channels_today:
    print("❌ NO MESSAGES from ANY channel today!")
    print()
    print("⚠️  Telegram reader might not be running!")
else:
    print(f"✅ Receiving from {len(channels_today)} channels today:")
    print()
    for ch in channels_today:
        print(f"   {ch['channel_name']:<50} : {ch['count']:>3} messages")
    
    # Check if JP is in the list
    jp_found = any('JP Paper' in ch['channel_name'] for ch in channels_today)
    
    print()
    if jp_found:
        print("✅ JP Paper trade IS sending messages today")
    else:
        print("❌ JP Paper trade NOT sending messages today")
        print("   → Either no messages sent, or telegram_reader not receiving")

conn.close()

print()
print("="*80)
print("RECOMMENDATION")
print("="*80)

if not messages:
    print("""
⚠️  NO MESSAGES from JP Paper trade channel EVER!

Action required:
1. Check if telegram_reader_production.py is running
2. Verify you're a member of "JP Paper trade - Dec-25"
3. Check if channel ID is correct: -1003282204738
4. Run: python get_channel_ids.py to verify
""")
elif today_count == 0:
    print(f"""
⚠️  LAST MESSAGE was on {messages[0]['timestamp'][:10]}

Possible reasons:
1. Channel hasn't posted today yet
2. Telegram reader restarted and missing today's messages
3. Check if telegram_reader_production.py is currently running

Action: Check telegram_reader.log for today's activity
""")
else:
    print(f"""
✅ Everything looks good! Received {today_count} messages today.

If signals aren't being placed as orders, check:
1. Are they being parsed correctly?
2. Run: python fix_all_signals.py to fix incomplete signals
""")