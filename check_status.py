"""
Trading System Status Dashboard
Shows real-time status of all components
"""

import sqlite3
from datetime import datetime
import os

def check_status():
    """Display system status"""
    
    print("="*80)
    print(" "*20 + "TRADING SYSTEM STATUS")
    print("="*80)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    conn = sqlite3.connect('trading.db')
    cursor = conn.cursor()
    
    # Signals today
    cursor.execute("SELECT COUNT(*) FROM signals WHERE DATE(timestamp) = DATE('now')")
    signals_today = cursor.fetchone()[0]
    
    # Pending signals
    cursor.execute("SELECT COUNT(*) FROM signals WHERE processed = 0")
    pending = cursor.fetchone()[0]
    
    # Orders today
    cursor.execute("SELECT COUNT(*) FROM orders WHERE DATE(created_at) = DATE('now')")
    orders_today = cursor.fetchone()[0]
    
    # Open orders
    cursor.execute("SELECT COUNT(*) FROM orders WHERE entry_status IN ('PENDING', 'OPEN')")
    open_orders = cursor.fetchone()[0]
    
    print(f"\n📊 SIGNALS")
    print(f"   Today: {signals_today} signals")
    print(f"   Pending: {pending} waiting for processing")
    
    print(f"\n📈 ORDERS")
    print(f"   Today: {orders_today} orders placed")
    print(f"   Open: {open_orders} orders active")
    
    # Recent signals
    cursor.execute("""
        SELECT id, 
               substr(timestamp, 12, 8) as time,
               json_extract(parsed_data, '$.symbol') as symbol,
               json_extract(parsed_data, '$.strike') as strike,
               json_extract(parsed_data, '$.option_type') as type,
               processed
        FROM signals 
        WHERE DATE(timestamp) = DATE('now')
        ORDER BY id DESC 
        LIMIT 5
    """)
    recent = cursor.fetchall()
    
    if recent:
        print(f"\n🔔 RECENT SIGNALS (Last 5)")
        print(f"{'ID':<6} {'Time':<10} {'Symbol':<12} {'Strike':<8} {'Type':<4} {'Status':<10}")
        print("-"*60)
        for r in recent:
            status = "✅ Done" if r[5] == 1 else "⏳ Pending"
            print(f"{r[0]:<6} {r[1]:<10} {r[2]:<12} {r[3]:<8} {r[4]:<4} {status:<10}")
    
    # Recent orders
    cursor.execute("""
        SELECT id, tradingsymbol, entry_status, sl_flag,
               substr(created_at, 12, 8) as time
        FROM orders 
        WHERE DATE(created_at) = DATE('now')
        ORDER BY id DESC 
        LIMIT 5
    """)
    orders = cursor.fetchall()
    
    if orders:
        print(f"\n💼 RECENT ORDERS (Last 5)")
        print(f"{'ID':<6} {'Time':<10} {'Symbol':<20} {'Entry':<12} {'SL':<15}")
        print("-"*70)
        for o in orders:
            print(f"{o[0]:<6} {o[4]:<10} {o[1]:<20} {o[2]:<12} {o[3]:<15}")
    
    conn.close()
    
    # Check if processes are running
    print(f"\n⚙️  PROCESSES")
    
    # Check log files
    logs = {
        'telegram_reader.log': 'Telegram Watcher',
        'order_placer.log': 'Order Placer',
        'sl_monitor.log': 'SL Monitor'
    }
    
    for log_file, name in logs.items():
        if os.path.exists(log_file):
            # Get last modified time
            mtime = os.path.getmtime(log_file)
            last_update = datetime.fromtimestamp(mtime)
            age_seconds = (datetime.now() - last_update).total_seconds()
            
            if age_seconds < 60:
                status = "✅ Running"
                color = ""
            elif age_seconds < 300:
                status = "⚠️  Idle"
                color = ""
            else:
                status = "❌ Stopped"
                color = ""
            
            print(f"   {name:<20} {status} (last activity: {int(age_seconds)}s ago)")
        else:
            print(f"   {name:<20} ❓ No log file")
    
    print("\n" + "="*80)
    print("Press Ctrl+C to exit")
    print("="*80)

if __name__ == "__main__":
    try:
        import time
        while True:
            check_status()
            time.sleep(30)
            print("\n" * 2)
    except KeyboardInterrupt:
        print("\n\nExiting...")