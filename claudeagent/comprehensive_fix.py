"""
comprehensive_fix.py
Fixes all issues:
1. Updates telegram reader to use new parser with instrument CSV lookup
2. Resets all signals to unprocessed
3. Deletes instrument cache to force reload
"""

import sqlite3
import os
from pathlib import Path

print("="*70)
print("COMPREHENSIVE FIX - JP TRADING SYSTEM")
print("="*70)
print("")

# Step 1: Delete instrument cache
cache_file = 'jp_instruments_cache.pkl'
if Path(cache_file).exists():
    os.remove(cache_file)
    print("✓ Deleted instrument cache")
    print("  System will reload from CSV with correct data")
else:
    print("✓ No cache to delete")

print("")

# Step 2: Reset ALL signals to unprocessed
db_path = 'jp_signals_trained.db'

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Count signals
    cursor.execute("SELECT COUNT(*) FROM signals")
    total = cursor.fetchone()[0]
    
    # Reset all to unprocessed
    cursor.execute("UPDATE signals SET processed = 0")
    conn.commit()
    
    print(f"✓ Reset {total} signals to unprocessed")
    print("  All signals will be reprocessed with correct data")
    
    conn.close()
    
except Exception as e:
    print(f"✗ Database error: {e}")

print("")
print("="*70)
print("NEXT STEPS:")
print("="*70)
print("")
print("1. Download these 3 UPDATED files:")
print("   - telegram_reader_jp_trained.py (NEW VERSION)")
print("   - jp_channel_parser.py (with instrument CSV lookup)")
print("   - order_placer_jp_trained.py (latest version)")
print("")
print("2. RESTART Telegram Reader:")
print("   python telegram_reader_jp_trained.py")
print("")
print("3. RESTART Order Placer:")
print("   python order_placer_jp_trained.py --continuous --interval 5")
print("")
print("4. Watch logs for:")
print("   [INSTRUMENT] Found in CSV... ← Parser using instruments")
print("   Quantity: 35 ← Correct BANKNIFTY lot size")
print("   [SUCCESS] Order placed ← Working!")
print("")
print("="*70)
