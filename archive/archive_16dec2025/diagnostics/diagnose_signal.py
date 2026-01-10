"""
diagnose_signal.py - Check why a signal wasn't converted to an order
Usage: python diagnose_signal.py 205
"""

import sqlite3
import json
import sys

def diagnose_signal(signal_id, db_path='trading.db'):
    """Diagnose why a signal wasn't processed"""
    
    print("\n" + "="*80)
    print(f"DIAGNOSTIC REPORT FOR SIGNAL #{signal_id}")
    print("="*80)
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Get signal details
    cursor.execute("SELECT * FROM signals WHERE id = ?", (signal_id,))
    signal = cursor.fetchone()
    
    if not signal:
        print(f"\n[ERROR] Signal #{signal_id} not found in database")
        conn.close()
        return
    
    # Display signal details
    print(f"\n{'='*80}")
    print("SIGNAL DETAILS")
    print("="*80)
    
    # Helper function to safely get row values
    def get_val(row, key, default='N/A'):
        try:
            return row[key] if row[key] is not None else default
        except:
            return default
    
    print(f"ID: {get_val(signal, 'id')}")
    print(f"Channel ID: {get_val(signal, 'channel_id')}")
    print(f"Message ID: {get_val(signal, 'message_id')}")
    print(f"Timestamp: {get_val(signal, 'timestamp')}")
    print(f"Processed: {get_val(signal, 'processed')}")
    
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
        print("[NULL] - Signal was not parsed")
    
    # Check if order exists
    print(f"\n{'='*80}")
    print("ORDER STATUS")
    print("="*80)
    
    cursor.execute("SELECT * FROM orders WHERE signal_id = ?", (signal_id,))
    order = cursor.fetchone()
    
    if order:
        print("\n[OK] Order was placed!")
        print("-"*80)
        order_keys = order.keys()
        
        if 'entry_order_id' in order_keys:
            print(f"Entry Order ID: {get_val(order, 'entry_order_id')}")
        elif 'order_id' in order_keys:
            print(f"Order ID: {get_val(order, 'order_id')}")
        
        # Try different column names
        symbol = get_val(order, 'tradingsymbol', get_val(order, 'symbol'))
        print(f"Tradingsymbol: {symbol}")
        print(f"Action: {get_val(order, 'action')}")
        print(f"Quantity: {get_val(order, 'quantity')}")
        print(f"Entry Price: {get_val(order, 'entry_price')}")
        print(f"Stop Loss: {get_val(order, 'stop_loss')}")
        
        # Try different status column names
        status = get_val(order, 'entry_status', get_val(order, 'status'))
        print(f"Status: {status}")
        print(f"SL Flag: {get_val(order, 'sl_flag')}")
    else:
        print("\n[WARNING] No order placed for this signal")
    
    # Diagnosis
    print(f"\n{'='*80}")
    print("DIAGNOSIS")
    print("="*80)
    
    issues = []
    
    # Check 1: Was signal parsed?
    if not parsed_data:
        issues.append("❌ PARSING FAILED - Signal was not parsed (parsed_data is NULL)")
        issues.append("   Reason: Parsing failed or message was ignoreable")
        issues.append("   Fix: Check telegram_reader logs for parsing errors")
    
    # Check 2: Was signal processed?
    processed = get_val(signal, 'processed', 0)
    if processed == 0:
        if parsed_data:
            issues.append("⚠️  NOT PROCESSED YET - Signal parsed but not yet processed")
            issues.append("   Reason: order_placer hasn't picked it up yet")
            issues.append("   Fix: Wait for next cycle or check if order_placer is running")
        else:
            issues.append("❌ NOT PARSED - Signal marked as unprocessed because parsing failed")
    
    # Check 3: Validate parsed data
    if parsed_data:
        try:
            parsed = json.loads(parsed_data)
            
            required_fields = ['action', 'symbol', 'strike', 'option_type', 'entry_price']
            missing_fields = [f for f in required_fields if not parsed.get(f)]
            
            if missing_fields:
                issues.append(f"❌ INCOMPLETE DATA - Missing fields: {', '.join(missing_fields)}")
                issues.append("   Reason: Parser couldn't extract all required information")
                issues.append("   Fix: Check if message format is supported")
            
            # Check if symbol is valid
            symbol = parsed.get('symbol', '')
            if symbol and not symbol.isupper():
                issues.append(f"⚠️  SYMBOL ISSUE - Symbol '{symbol}' might not be valid")
            
            # Check if prices are valid
            entry_price = parsed.get('entry_price', 0)
            stop_loss = parsed.get('stop_loss', 0)
            
            if entry_price <= 0:
                issues.append("❌ INVALID ENTRY PRICE - Entry price must be > 0")
            
            if stop_loss <= 0:
                issues.append("❌ INVALID STOP LOSS - Stop loss must be > 0")
            
        except Exception as e:
            issues.append(f"❌ JSON ERROR - Could not parse JSON: {e}")
    
    # Check 4: Check if order_placer had errors
    if processed == 1 and not order:
        issues.append("❌ ORDER PLACEMENT FAILED - Signal marked as processed but no order created")
        issues.append("   Reason: Order placement failed (check order_placer logs)")
        issues.append("   Possible causes: Invalid instrument, API error, Kite rejection")
    
    # Display issues
    if issues:
        print("\nISSUES FOUND:")
        print("-"*80)
        for issue in issues:
            print(issue)
    else:
        print("\n✅ No issues found - Signal appears to be processed correctly")
    
    # Recommendations
    print(f"\n{'='*80}")
    print("RECOMMENDATIONS")
    print("="*80)
    
    processed = get_val(signal, 'processed', 0)
    
    if not parsed_data:
        print("\n1. Check telegram_reader logs:")
        print("   Look for: [FAILED] Could not parse")
        print("   Look for: Message matches ignoreable pattern")
        print("\n2. Check if message format is supported:")
        print("   Re-send message or check parsing rules")
    
    elif processed == 0:
        print("\n1. Check if order_placer is running:")
        print("   python order_placer_db_enhanced.py --continuous")
        print("\n2. Wait for next processing cycle (30 seconds)")
        print("\n3. Check order_placer logs for errors")
    
    elif not order:
        print("\n1. Check order_placer logs:")
        print("   Look for: [ERROR] or [WARNING] messages")
        print("\n2. Check if instrument is valid:")
        print("   Verify tradingsymbol format")
        print("\n3. Check Kite API status:")
        print("   Verify access token is valid")
        print("\n4. Run order_placer in test mode:")
        print("   python order_placer_db_enhanced.py --test")
    
    print("\n" + "="*80)
    
    conn.close()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        signal_id = int(sys.argv[1])
    else:
        signal_id = int(input("Enter signal ID to diagnose: "))
    
    diagnose_signal(signal_id)
