import sqlite3
import json
from datetime import datetime

db = sqlite3.connect('trading.db')
cursor = db.cursor()

# Get all signals
cursor.execute("""
    SELECT id, channel_name, parsed_data, timestamp, processed 
    FROM signals 
    ORDER BY timestamp DESC 
    LIMIT 20
""")

signals = cursor.fetchall()

print("="*80)
print(f"📊 SIGNALS DATABASE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

if not signals:
    print("No signals in database yet.")
else:
    print(f"Total signals: {len(signals)} (showing last 20)")
    print()
    
    for sig in signals:
        sig_id, channel, parsed_str, ts, processed = sig
        parsed = json.loads(parsed_str)
        
        status = "✓ PROCESSED" if processed == 1 else "⏳ PENDING" if processed == 0 else "✗ ERROR"
        
        print(f"[{sig_id}] {status} | {channel}")
        print(f"    {parsed.get('symbol')} {parsed.get('strike')} {parsed.get('option_type')}")
        print(f"    Action: {parsed.get('action')} | Entry: {parsed.get('entry_price')} | SL: {parsed.get('stop_loss')}")
        print(f"    Expiry: {parsed.get('expiry_date')} | Qty: {parsed.get('quantity')}")
        print(f"    Time: {ts}")
        print()

print("="*80)

# Summary stats
cursor.execute("SELECT processed, COUNT(*) FROM signals GROUP BY processed")
stats = cursor.fetchall()

if stats:
    print("\nSummary:")
    for status, count in stats:
        status_name = "Processed" if status == 1 else "Pending" if status == 0 else "Error"
        print(f"  {status_name}: {count}")

db.close()
