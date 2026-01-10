"""
Fix date format in signals - Convert DD-MM-YYYY to YYYY-MM-DD
"""

import sqlite3
import json
from datetime import datetime

conn = sqlite3.connect('trading.db')
cursor = conn.cursor()

print("="*80)
print("FIXING DATE FORMAT IN SIGNALS")
print("="*80)

# Get all unprocessed signals with wrong date format
cursor.execute("""
    SELECT id, parsed_data
    FROM signals
    WHERE processed = 0
    AND parsed_data LIKE '%"expiry_date": "%-%-%'
""")

signals = cursor.fetchall()

if not signals:
    print("\n✅ No signals with wrong date format found")
else:
    print(f"\n⚠️  Found {len(signals)} signals with wrong date format\n")
    
    fixed_count = 0
    
    for signal_id, parsed_json in signals:
        try:
            parsed = json.loads(parsed_json)
            expiry = parsed.get('expiry_date')
            
            if expiry and '-' in expiry:
                # Check if it's DD-MM-YYYY format
                parts = expiry.split('-')
                if len(parts) == 3 and len(parts[0]) <= 2:  # Day is first
                    # Convert DD-MM-YYYY to YYYY-MM-DD
                    day, month, year = parts
                    correct_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    
                    print(f"[{signal_id}] Fixing date: {expiry} → {correct_date}")
                    
                    parsed['expiry_date'] = correct_date
                    
                    # Also fix tradingsymbol if it exists and looks wrong
                    tradingsymbol = parsed.get('tradingsymbol', '')
                    if 'D16' in tradingsymbol or 'D30' in tradingsymbol:
                        # Fix malformed tradingsymbol
                        symbol = parsed.get('symbol', '')
                        strike = parsed.get('strike', '')
                        option_type = parsed.get('option_type', '')
                        
                        # Parse the correct date
                        date_obj = datetime.strptime(correct_date, '%Y-%m-%d')
                        month_code = date_obj.strftime('%b').upper()  # DEC
                        year_code = date_obj.strftime('%y')  # 25
                        
                        correct_tradingsymbol = f"{symbol}{year_code}{month_code}{strike}{option_type}"
                        parsed['tradingsymbol'] = correct_tradingsymbol
                        
                        print(f"       Also fixed tradingsymbol: {tradingsymbol} → {correct_tradingsymbol}")
                    
                    # Update database
                    cursor.execute("""
                        UPDATE signals
                        SET parsed_data = ?, processed = 0
                        WHERE id = ?
                    """, (json.dumps(parsed), signal_id))
                    
                    fixed_count += 1
                    print(f"       ✅ Fixed!")
                    print()
        
        except Exception as e:
            print(f"[{signal_id}] ❌ Error: {e}")
    
    conn.commit()
    
    print("="*80)
    print("SUMMARY")
    print("="*80)
    print(f"✅ Fixed: {fixed_count} signals")
    print("="*80)
    
    if fixed_count > 0:
        print("\n✅ Fixed signals will be picked up by order_placer in next cycle!")
        print("   (within 30 seconds)")

conn.close()

print("\n" + "="*80)
print("ALSO CHECK: Why is the parser adding wrong date format?")
print("="*80)
print("""
The parser should add dates in YYYY-MM-DD format, not DD-MM-YYYY.

Temporary fix: This script fixes existing signals.
Permanent fix: Update the parser to use correct date format.

To prevent this in future:
1. Check signal_parser_with_claude_fallback.py
2. Look for date enrichment logic
3. Ensure it formats as: YYYY-MM-DD
""")
