"""
Diagnose why signal 311 is not being processed
"""

import sqlite3
import json

conn = sqlite3.connect('trading.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

print("="*80)
print("SIGNAL 311 DIAGNOSTIC")
print("="*80)

# Get signal 311
cursor.execute("SELECT * FROM signals WHERE id = 311")
signal = cursor.fetchone()

if signal:
    print("\n✅ Signal 311 exists in database")
    print(f"\nSignal details:")
    print(f"  ID: {signal['id']}")
    print(f"  Channel: {signal['channel_name']}")
    print(f"  Raw text: {signal['raw_text']}")
    print(f"  Timestamp: {signal['timestamp']}")
    print(f"  Processed: {signal['processed']}")
    
    # Check parsed_data
    try:
        parsed = json.loads(signal['parsed_data'])
        print(f"\n✅ Parsed data is valid JSON")
        print(f"\nParsed fields:")
        for key, value in parsed.items():
            print(f"  {key}: {value}")
        
        # Check required fields
        required = ['symbol', 'strike', 'option_type', 'action', 'entry_price', 'stop_loss', 'expiry_date', 'quantity']
        missing = [f for f in required if f not in parsed or parsed[f] is None]
        
        if missing:
            print(f"\n❌ Missing required fields: {missing}")
        else:
            print(f"\n✅ All required fields present")
            
    except Exception as e:
        print(f"\n❌ Error parsing JSON: {e}")
    
    # Check if processed flag is correct
    if signal['processed'] == 0:
        print(f"\n✅ Signal is marked as unprocessed (processed=0)")
    else:
        print(f"\n❌ Signal is marked as processed (processed={signal['processed']})")
        print(f"   Order placer only picks up signals with processed=0")
else:
    print("\n❌ Signal 311 not found in database")

# Check all unprocessed signals
print("\n" + "="*80)
print("ALL UNPROCESSED SIGNALS")
print("="*80)

cursor.execute("""
    SELECT id, channel_name, json_extract(parsed_data, '$.symbol'), 
           json_extract(parsed_data, '$.strike'), processed
    FROM signals 
    WHERE processed = 0
    ORDER BY id
""")

unprocessed = cursor.fetchall()

if unprocessed:
    print(f"\nFound {len(unprocessed)} unprocessed signals:")
    for row in unprocessed:
        print(f"  Signal {row[0]}: {row[2]} {row[3]} | Channel: {row[1]} | Processed: {row[4]}")
else:
    print("\n❌ No unprocessed signals found!")
    print("   This is why order_placer shows 'No pending signals'")

# Check column names
print("\n" + "="*80)
print("SIGNALS TABLE SCHEMA")
print("="*80)

cursor.execute("PRAGMA table_info(signals)")
columns = cursor.fetchall()

print("\nColumns in signals table:")
for col in columns:
    print(f"  {col[1]} ({col[2]})")

conn.close()

print("\n" + "="*80)
print("SOLUTION")
print("="*80)

print("""
If signal 311 has processed=1 instead of 0, run:
  UPDATE signals SET processed = 0 WHERE id = 311;

If signal 311 doesn't exist, re-insert it with correct SQL.

If signal 311 exists with processed=0 but order_placer doesn't see it,
restart order_placer:
  Ctrl+C (stop it)
  python order_placer_db_production.py --continuous
""")
