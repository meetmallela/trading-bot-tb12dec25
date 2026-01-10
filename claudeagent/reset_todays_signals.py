"""
reset_todays_signals.py
Resets today's signals to unprocessed so they can be retried
Only resets signals from today that are < 30 minutes old
"""

import sqlite3
from datetime import datetime, timedelta

db_path = 'jp_signals_trained.db'

# Connect to database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get today's date
today = datetime.now().date()
today_start = datetime(today.year, today.month, today.day, 0, 0, 0)

# Get current time for freshness check
now = datetime.now()
cutoff_time = now - timedelta(minutes=30)

print("="*70)
print("RESET TODAY'S SIGNALS")
print("="*70)
print(f"Today: {today}")
print(f"Cutoff: Last 30 minutes (after {cutoff_time.strftime('%H:%M:%S')})")
print("")

# Get all today's signals
cursor.execute("""
    SELECT id, timestamp, raw_text, processed 
    FROM signals 
    WHERE date(timestamp) = date('now', 'localtime')
    ORDER BY id ASC
""")

signals = cursor.fetchall()

if not signals:
    print("[INFO] No signals found from today")
    conn.close()
    exit()

print(f"[FOUND] {len(signals)} signals from today")
print("")

# Process each signal
reset_count = 0
stale_count = 0

for signal in signals:
    signal_id, timestamp_str, raw_text, processed = signal
    
    # Parse timestamp
    try:
        if 'T' in timestamp_str:
            signal_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            # Remove timezone for comparison
            signal_time = signal_time.replace(tzinfo=None)
        else:
            signal_time = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"[ERROR] Could not parse timestamp for signal {signal_id}: {e}")
        continue
    
    # Calculate age
    age_minutes = (now - signal_time).total_seconds() / 60
    
    # Check if fresh (< 30 min)
    if signal_time >= cutoff_time:
        # FRESH - Reset to unprocessed
        cursor.execute("""
            UPDATE signals 
            SET processed = 0 
            WHERE id = ?
        """, (signal_id,))
        
        reset_count += 1
        status = "RESET" if processed == 1 else "Already unprocessed"
        print(f"[{status}] Signal {signal_id}: {int(age_minutes)} min old")
        print(f"  {raw_text[:50]}...")
    else:
        # STALE - Keep as processed
        stale_count += 1
        print(f"[STALE] Signal {signal_id}: {int(age_minutes)} min old (too old)")

conn.commit()
conn.close()

print("")
print("="*70)
print("SUMMARY")
print("="*70)
print(f"Total signals today: {len(signals)}")
print(f"Reset to unprocessed: {reset_count}")
print(f"Kept as stale: {stale_count}")
print("")

if reset_count > 0:
    print(f"✓ {reset_count} signals reset!")
    print("  Restart order_placer_jp_trained.py to reprocess them")
else:
    print("[INFO] No fresh signals to reset")

print("="*70)
