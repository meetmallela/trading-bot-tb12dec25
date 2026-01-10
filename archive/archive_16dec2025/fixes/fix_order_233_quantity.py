"""
fix_order_233_quantity.py - Fix quantity for NATURALGAS order
"""

import sqlite3

def fix_order():
    conn = sqlite3.connect('trading.db')
    cursor = conn.cursor()
    
    # Update quantity to 1 for the NATURALGAS order
    cursor.execute("""
        UPDATE orders 
        SET quantity = 1
        WHERE entry_order_id = '1999524499623780352'
        AND tradingsymbol = 'NATURALGAS25DEC360PE'
    """)
    
    affected = cursor.rowcount
    conn.commit()
    
    print("\n" + "="*60)
    print("FIXED ORDER QUANTITY")
    print("="*60)
    print(f"Updated {affected} order(s)")
    print(f"Entry Order ID: 1999524499623780352")
    print(f"Tradingsymbol: NATURALGAS25DEC360PE")
    print(f"Old Quantity: 50")
    print(f"New Quantity: 1")
    print("\n" + "="*60)
    print("NEXT: Restart sl_monitor.py")
    print("="*60)
    print("The SL order will now place with correct quantity = 1")
    print("="*60 + "\n")
    
    conn.close()

if __name__ == "__main__":
    fix_order()
