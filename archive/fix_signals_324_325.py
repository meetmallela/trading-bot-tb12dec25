"""
Fix date format in signals 324 and 325 specifically
Also resets them to processed=0
"""

import sqlite3
import json
from datetime import datetime

conn = sqlite3.connect('trading.db')
cursor = conn.cursor()

print("="*80)
print("FIXING SIGNALS 324 & 325")
print("="*80)

# Get signals 324 and 325
cursor.execute("""
    SELECT id, channel_name, raw_text, parsed_data, processed
    FROM signals
    WHERE id IN (324, 325)
""")

signals = cursor.fetchall()

if not signals:
    print("\n❌ Signals 324 and 325 not found!")
else:
    print(f"\n✅ Found {len(signals)} signals\n")
    
    for signal_id, channel_name, raw_text, parsed_json, processed in signals:
        print("="*80)
        print(f"SIGNAL #{signal_id}")
        print("="*80)
        print(f"Channel: {channel_name}")
        print(f"Status: {'Processed' if processed else 'Unprocessed'}")
        print(f"Raw: {raw_text[:80]}...")
        print()
        
        try:
            parsed = json.loads(parsed_json)
            
            print("BEFORE:")
            print(f"  expiry_date: {parsed.get('expiry_date')}")
            print(f"  tradingsymbol: {parsed.get('tradingsymbol')}")
            
            # Fix expiry date format
            expiry = parsed.get('expiry_date')
            if expiry:
                # Check if it's DD-MM-YYYY format
                if '-' in expiry:
                    parts = expiry.split('-')
                    if len(parts[0]) <= 2:  # Day is first
                        day, month, year = parts
                        correct_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                        parsed['expiry_date'] = correct_date
                        print(f"\n✅ Fixed date: {expiry} → {correct_date}")
                    else:
                        correct_date = expiry
                        print(f"\n✅ Date already correct: {correct_date}")
                else:
                    print(f"\n⚠️  Date format unexpected: {expiry}")
                    correct_date = expiry
            
            # Fix tradingsymbol
            symbol = parsed.get('symbol', '')
            strike = parsed.get('strike', 0)
            option_type = parsed.get('option_type', '')
            
            if symbol and strike and option_type and correct_date:
                try:
                    date_obj = datetime.strptime(correct_date, '%Y-%m-%d')
                    month_code = date_obj.strftime('%b').upper()  # DEC
                    year_code = date_obj.strftime('%y')  # 25
                    
                    correct_tradingsymbol = f"{symbol}{year_code}{month_code}{int(strike)}{option_type}"
                    parsed['tradingsymbol'] = correct_tradingsymbol
                    
                    print(f"✅ Fixed tradingsymbol: {correct_tradingsymbol}")
                except Exception as e:
                    print(f"⚠️  Could not fix tradingsymbol: {e}")
            
            print("\nAFTER:")
            print(f"  expiry_date: {parsed.get('expiry_date')}")
            print(f"  tradingsymbol: {parsed.get('tradingsymbol')}")
            
            # Update database and reset to unprocessed
            cursor.execute("""
                UPDATE signals
                SET parsed_data = ?, processed = 0
                WHERE id = ?
            """, (json.dumps(parsed), signal_id))
            
            print(f"\n✅ Updated signal {signal_id} and reset to processed=0")
            print()
            
        except Exception as e:
            print(f"❌ Error: {e}")
            print()
    
    conn.commit()
    
    print("="*80)
    print("SUMMARY")
    print("="*80)
    print(f"✅ Fixed signals 324 and 325")
    print(f"✅ Reset to processed=0")
    print(f"✅ Order placer will pick them up in next cycle (30 seconds)")
    print("="*80)

# Verify the fix
print("\n" + "="*80)
print("VERIFICATION")
print("="*80)

cursor.execute("""
    SELECT id, 
           json_extract(parsed_data, '$.expiry_date') as expiry,
           json_extract(parsed_data, '$.tradingsymbol') as symbol,
           processed
    FROM signals
    WHERE id IN (324, 325)
""")

for row in cursor.fetchall():
    print(f"Signal {row[0]}: expiry={row[1]}, symbol={row[2]}, processed={row[3]}")

conn.close()

print("\n" + "="*80)
print("NEXT STEP")
print("="*80)
print("Monitor order_placer.log to see these signals being processed:")
print("  Get-Content order_placer.log -Wait -Tail 20")
print("="*80)
