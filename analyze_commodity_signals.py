"""
Analyze commodity signals from database to find why they're failing
"""

import sqlite3
import json

print("="*80)
print("ANALYZING COMMODITY SIGNALS")
print("="*80)

try:
    conn = sqlite3.connect('trading.db')
    cursor = conn.cursor()
    
    # Get all signals with commodity-related keywords
    cursor.execute("""
        SELECT id, channel_name, raw_text, parsed_data, processed, timestamp
        FROM signals
        WHERE raw_text LIKE '%GOLD%' 
           OR raw_text LIKE '%SILVER%'
           OR raw_text LIKE '%CRUDE%'
           OR raw_text LIKE '%NATURAL%'
           OR raw_text LIKE '%MCX%'
        ORDER BY id DESC
        LIMIT 20
    """)
    
    signals = cursor.fetchall()
    
    if not signals:
        print("\n❌ No commodity signals found in database")
        print("Searching for any signals...")
        
        cursor.execute("SELECT id, raw_text FROM signals ORDER BY id DESC LIMIT 5")
        recent = cursor.fetchall()
        
        print(f"\nLast 5 signals:")
        for sig in recent:
            print(f"  ID {sig[0]}: {sig[1][:60]}...")
    else:
        print(f"\n✅ Found {len(signals)} commodity-related signals\n")
        
        for sig in signals:
            sig_id, channel, raw_text, parsed_json, processed, timestamp = sig
            
            print("-"*80)
            print(f"Signal ID: {sig_id}")
            print(f"Channel: {channel}")
            print(f"Timestamp: {timestamp}")
            print(f"Processed: {processed} (0=pending, 1=success, -1=failed)")
            print(f"\nRaw Text:")
            print(f"  {raw_text[:150]}")
            
            if parsed_json:
                try:
                    parsed = json.loads(parsed_json)
                    print(f"\nParsed Data:")
                    print(f"  Symbol: {parsed.get('symbol')}")
                    print(f"  Strike: {parsed.get('strike')}")
                    print(f"  Option Type: {parsed.get('option_type')}")
                    print(f"  Instrument Type: {parsed.get('instrument_type', 'NOT SET')}")
                    print(f"  Action: {parsed.get('action')}")
                    print(f"  Entry: {parsed.get('entry_price')}")
                    print(f"  SL: {parsed.get('stop_loss')}")
                    print(f"  Tradingsymbol: {parsed.get('tradingsymbol', 'NOT SET')}")
                    print(f"  Exchange: {parsed.get('exchange', 'NOT SET')}")
                    print(f"  Quantity: {parsed.get('quantity')}")
                    
                    # Identify issues
                    issues = []
                    if not parsed.get('tradingsymbol'):
                        issues.append("Missing tradingsymbol")
                    if not parsed.get('exchange'):
                        issues.append("Missing exchange")
                    if not parsed.get('quantity'):
                        issues.append("Missing quantity")
                    if not parsed.get('instrument_type'):
                        issues.append("Missing instrument_type")
                    
                    if issues:
                        print(f"\n⚠️  ISSUES FOUND:")
                        for issue in issues:
                            print(f"    - {issue}")
                    else:
                        print(f"\n✅ All required fields present")
                        
                except Exception as e:
                    print(f"\n❌ Failed to parse JSON: {e}")
            else:
                print("\n❌ No parsed data")
            
            print()
    
    conn.close()
    
    print("="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)
    
except sqlite3.OperationalError as e:
    print(f"❌ Database error: {e}")
    print("Make sure trading.db exists in the current directory")
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
