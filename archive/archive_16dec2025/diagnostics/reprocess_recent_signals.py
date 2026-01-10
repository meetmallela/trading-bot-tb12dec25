"""
reprocess_recent_signals.py - Mark recent signals as unprocessed for retry
"""

import sqlite3
import json
from datetime import datetime, timedelta

def reprocess_recent_signals(hours=1):
    """
    Mark signals from last N hours as unprocessed
    Only marks signals that have complete data
    """
    conn = sqlite3.connect('trading.db')
    cursor = conn.cursor()
    
    # Calculate cutoff time
    cutoff_time = (datetime.now() - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')
    
    print(f"\n{'='*80}")
    print(f"REPROCESSING SIGNALS FROM LAST {hours} HOUR(S)")
    print("="*80)
    print(f"Cutoff time: {cutoff_time}")
    print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Find signals from last N hours that are processed=1
    cursor.execute("""
        SELECT id, parsed_data, timestamp
        FROM signals 
        WHERE timestamp > ?
        AND processed = 1
        AND parsed_data IS NOT NULL
        ORDER BY id DESC
    """, (cutoff_time,))
    
    signals = cursor.fetchall()
    
    if not signals:
        print(f"\nNo signals found from last {hours} hour(s)")
        conn.close()
        return
    
    print(f"\nFound {len(signals)} signals to review")
    print("-"*80)
    
    # Filter for complete signals only
    complete_signals = []
    incomplete_signals = []
    
    for signal_id, parsed_data_str, timestamp in signals:
        try:
            parsed = json.loads(parsed_data_str)
            
            # Check if it has required fields
            has_action = parsed.get('action') is not None
            has_symbol = parsed.get('symbol') is not None
            has_strike = parsed.get('strike') is not None
            has_option_type = parsed.get('option_type') is not None
            has_entry = parsed.get('entry_price') is not None
            has_sl = parsed.get('stop_loss') is not None
            has_expiry = parsed.get('expiry_date') is not None
            
            is_complete = has_action and has_symbol and has_strike and has_option_type and has_entry and has_sl
            
            if is_complete:
                complete_signals.append({
                    'id': signal_id,
                    'symbol': parsed.get('symbol'),
                    'strike': parsed.get('strike'),
                    'option_type': parsed.get('option_type'),
                    'has_expiry': has_expiry,
                    'timestamp': timestamp
                })
            else:
                incomplete_signals.append({
                    'id': signal_id,
                    'symbol': parsed.get('symbol'),
                    'reason': 'Missing fields'
                })
        
        except Exception as e:
            incomplete_signals.append({
                'id': signal_id,
                'symbol': 'Unknown',
                'reason': f'Parse error: {e}'
            })
    
    print(f"\nComplete signals: {len(complete_signals)}")
    print(f"Incomplete signals: {len(incomplete_signals)} (will be skipped)")
    
    if not complete_signals:
        print("\nNo complete signals to reprocess!")
        conn.close()
        return
    
    # Show what will be reprocessed
    print(f"\n{'='*80}")
    print("SIGNALS TO BE REPROCESSED:")
    print("="*80)
    
    for sig in complete_signals:
        expiry_status = "✓ Has expiry" if sig['has_expiry'] else "✗ No expiry"
        print(f"  Signal #{sig['id']}: {sig['symbol']} {sig['strike']} {sig['option_type']} - {expiry_status} - {sig['timestamp']}")
    
    if incomplete_signals:
        print(f"\n{'='*80}")
        print("INCOMPLETE SIGNALS (WILL BE SKIPPED):")
        print("="*80)
        for sig in incomplete_signals:
            print(f"  Signal #{sig['id']}: {sig['symbol']} - {sig['reason']}")
    
    # Confirm
    print(f"\n{'='*80}")
    response = input(f"Mark {len(complete_signals)} signals as unprocessed? (yes/no): ")
    
    if response.lower() != 'yes':
        print("Cancelled.")
        conn.close()
        return
    
    # Mark as unprocessed
    signal_ids = [sig['id'] for sig in complete_signals]
    
    cursor.executemany(
        "UPDATE signals SET processed = 0 WHERE id = ?",
        [(sid,) for sid in signal_ids]
    )
    
    conn.commit()
    conn.close()
    
    print(f"\n{'='*80}")
    print(f"✓ MARKED {len(signal_ids)} SIGNALS AS UNPROCESSED")
    print("="*80)
    print(f"\nThese signals will be processed in the next order_placer cycle!")
    print(f"\nMonitor with:")
    print(f"  Get-Content order_placer.log -Wait -Tail 30")
    print(f"\nExpected:")
    print(f"  - order_placer will pick up these signals in ~30 seconds")
    print(f"  - Check for 'Found X pending signals to process'")
    print(f"  - Watch for order placement confirmations")
    print(f"\nVerify orders placed:")
    print(f"  sqlite3 trading.db \"SELECT COUNT(*) FROM orders WHERE signal_id IN ({','.join(map(str, signal_ids[:5]))});\"")
    print("="*80 + "\n")

if __name__ == "__main__":
    import sys
    
    hours = 1
    if len(sys.argv) > 1:
        try:
            hours = float(sys.argv[1])
        except:
            print("Usage: python reprocess_recent_signals.py [hours]")
            print("Example: python reprocess_recent_signals.py 2  # Last 2 hours")
            sys.exit(1)
    
    reprocess_recent_signals(hours)