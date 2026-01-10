"""
check_failed_signals.py - Check signals 208, 209, 210
"""

import sqlite3
import json

def check_signals():
    conn = sqlite3.connect('trading.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    signal_ids = [208, 209, 210]
    
    print("\n" + "="*80)
    print("CHECKING FAILED SIGNALS")
    print("="*80)
    
    for signal_id in signal_ids:
        cursor.execute("SELECT * FROM signals WHERE id = ?", (signal_id,))
        signal = cursor.fetchone()
        
        if not signal:
            print(f"\nSignal #{signal_id}: NOT FOUND")
            continue
        
        print(f"\n{'='*80}")
        print(f"SIGNAL #{signal_id}")
        print("="*80)
        
        # Safe access
        def get_val(row, key, default='N/A'):
            try:
                return row[key] if row[key] is not None else default
            except:
                return default
        
        print(f"Processed: {get_val(signal, 'processed')}")
        print(f"Timestamp: {get_val(signal, 'timestamp')}")
        
        print(f"\nRaw Text:")
        print("-"*80)
        print(get_val(signal, 'raw_text'))
        
        print(f"\nParsed Data:")
        print("-"*80)
        parsed_data = get_val(signal, 'parsed_data', None)
        if parsed_data:
            try:
                parsed = json.loads(parsed_data)
                print(json.dumps(parsed, indent=2))
            except:
                print(parsed_data)
        else:
            print("[NULL] - Not parsed or parsing failed")
        
        # Check order
        cursor.execute("SELECT COUNT(*) FROM orders WHERE signal_id = ?", (signal_id,))
        order_count = cursor.fetchone()[0]
        print(f"\nOrder created: {'YES' if order_count > 0 else 'NO'}")
    
    print("\n" + "="*80)
    
    # Summary
    print("\nSUMMARY")
    print("="*80)
    
    cursor.execute("""
        SELECT 
            s.id,
            CASE 
                WHEN s.parsed_data IS NULL THEN 'NOT_PARSED'
                WHEN s.parsed_data LIKE '%null%' THEN 'INCOMPLETE_PARSE'
                ELSE 'PARSED_OK'
            END as parse_status,
            CASE 
                WHEN o.id IS NULL THEN 'NO_ORDER'
                ELSE 'ORDER_CREATED'
            END as order_status
        FROM signals s
        LEFT JOIN orders o ON s.id = o.signal_id
        WHERE s.id IN (208, 209, 210)
        ORDER BY s.id
    """)
    
    for row in cursor.fetchall():
        print(f"Signal #{row[0]}: {row[1]} -> {row[2]}")
    
    conn.close()

if __name__ == "__main__":
    check_signals()
