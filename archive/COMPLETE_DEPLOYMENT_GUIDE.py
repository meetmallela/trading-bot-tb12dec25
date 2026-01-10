"""
COMPLETE FILES READY - FUTURES SUPPORT DEPLOYMENT
==================================================

You now have 3 files ready to deploy:

1. signal_parser_with_futures.py       - Parser with OPTIONS + FUTURES
2. telegram_reader_production_FUTURES.py - Telegram reader (complete)
3. order_placer_db_production_FUTURES.py - Order placer (complete)

"""

print("="*80)
print("COMPLETE DEPLOYMENT INSTRUCTIONS")
print("="*80)

print("""
STEP 1: BACKUP YOUR CURRENT FILES
==================================
copy signal_parser_with_claude_fallback.py signal_parser_with_claude_fallback.py.OLD
copy telegram_reader_production.py telegram_reader_production.py.OLD  
copy order_placer_db_production.py order_placer_db_production.py.OLD

STEP 2: REPLACE WITH NEW FILES
================================
Download these 3 files:
✅ signal_parser_with_futures.py
✅ telegram_reader_production_FUTURES.py
✅ order_placer_db_production_FUTURES.py

Rename them:
copy signal_parser_with_futures.py signal_parser_with_claude_fallback.py
copy telegram_reader_production_FUTURES.py telegram_reader_production.py
copy order_placer_db_production_FUTURES.py order_placer_db_production.py

OR just use the new names - your choice!

STEP 3: UPDATE DATABASE (OPTIONAL)
===================================
sqlite3 trading.db

-- Add instrument_type column if not exists
ALTER TABLE signals ADD COLUMN instrument_type TEXT DEFAULT 'OPTIONS';
ALTER TABLE orders ADD COLUMN instrument_type TEXT DEFAULT 'OPTIONS';

.exit

This is optional - code works without it.

STEP 4: RESTART EVERYTHING
===========================
# Stop current processes (Ctrl+C on each terminal)

# Terminal 1: Telegram Reader
python telegram_reader_production.py

# Terminal 2: Order Placer
python order_placer_db_production.py --continuous

# Terminal 3: SL Monitor (no changes needed)
python sl_monitor_with_trailing.py --interval 30

STEP 5: VERIFY IT'S WORKING
============================
Check logs for:

telegram_reader.log:
[OK] Using SignalParserWithFutures (OPTIONS + FUTURES support)
[START] Monitoring 13 channels
[MODE] futures_enabled - OPTIONS + FUTURES

When a signal comes in:
[FUTURES] Detected futures signal  ← FUTURES signal
OR
[OPTIONS] Detected options signal  ← OPTIONS signal

order_placer.log:
[FUTURES ORDER] GOLD25FEBFUT      ← FUTURES order
OR
[OPTIONS ORDER] NIFTY25DEC25900CE ← OPTIONS order

""")

print("="*80)
print("WHAT'S CHANGED - SUMMARY")
print("="*80)

print("""
TELEGRAM READER:
✅ Imports signal_parser_with_futures.py instead
✅ Detects both OPTIONS and FUTURES automatically
✅ Logs show signal type (OPTIONS/FUTURES)
✅ Tracks statistics separately for each type
✅ Saves instrument_type to database

ORDER PLACER:
✅ Has place_futures_order() method
✅ Has place_options_order() method (refactored)
✅ process_signal() checks instrument_type
✅ Routes to correct handler automatically
✅ Handles MCX exchange for futures
✅ Uses MARKET orders for both types
✅ Retry logic for connection errors

PARSER:
✅ Detects if signal is FUTURES or OPTIONS
✅ Parses FUTURES: "BUY GOLD FEB CMP 136830 SL 136500"
✅ Parses OPTIONS: "NIFTY 25900 CE BUY 140 SL 130"
✅ Auto-adds expiry dates for futures (last day of month)
✅ Auto-adds lot sizes (GOLD=100, SILVER=30000, etc.)
✅ Sets strike=None, option_type=None for futures
✅ Adds instrument_type='FUTURES' or 'OPTIONS'
✅ Builds tradingsymbol (GOLD25FEBFUT)
✅ Claude API fallback for both types

""")

print("="*80)
print("TESTING")
print("="*80)

print("""
TEST 1: OPTIONS SIGNAL
======================
Wait for: "NIFTY 25900 CE BUY 140 SL 130"

Expected telegram_reader.log:
[OPTIONS] Detected options signal
[✓ PARSED OPTIONS] NIFTY 25900 CE
[✓ COMPLETE] All required fields present
[✓ STORED] Signal ID: XXX | Type: OPTIONS

Expected order_placer.log:
[PROCESSING] Signal #XXX | Type: OPTIONS
[OPTIONS] NIFTY 25900 CE | BUY @ 140 | SL: 130
[OPTIONS ORDER] NIFTY25DEC25900CE
[✓ OK] Options order placed! Order ID: 123456
[✓ SUCCESS] Options order completed

TEST 2: FUTURES SIGNAL
======================
Wait for: "BUY GOLD FEB CMP 136830 WITH SL 136500"

Expected telegram_reader.log:
[FUTURES] Detected futures signal
[✓ PARSED FUTURES] GOLD FEB
[✓ COMPLETE] All required fields present
[✓ STORED] Signal ID: XXX | Type: FUTURES

Expected order_placer.log:
[PROCESSING] Signal #XXX | Type: FUTURES
[FUTURES] GOLD | BUY @ 136830.0 | SL: 136500.0
[FUTURES ORDER] GOLD25FEBFUT
[✓ OK] Futures order placed! Order ID: 789012
[✓ SUCCESS] Futures order completed

""")

print("="*80)
print("SUPPORTED FUTURES")
print("="*80)

print("""
MCX COMMODITIES:
- GOLD       (100 units)
- GOLDM      (10 units)
- SILVER     (30,000 grams)
- SILVERM    (5,000 grams)
- CRUDEOIL   (100 barrels)
- NATURALGAS (1,250 mmBtu)
- COPPER     (1,000 kg)
- ZINC       (5,000 kg)
- LEAD       (5,000 kg)
- NICKEL     (250 kg)
- ALUMINIUM  (5,000 kg)

MONTHS:
JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC

EXAMPLES:
✅ BUY GOLD FEB CMP 136830 WITH SL 136500
✅ SELL SILVER MAR 70000 SL 71000
✅ BUY CRUDEOIL APR 5300 SL 5250
✅ NATURALGAS DEC 350 BUY SL 340
""")

print("="*80)
print("TROUBLESHOOTING")
print("="*80)

print("""
PROBLEM: Parser not found
ERROR: signal_parser_with_futures.py not found!

SOLUTION:
Make sure signal_parser_with_futures.py is in the same directory
OR
Update the import in telegram_reader_production.py

---

PROBLEM: Futures order rejected
ERROR: Order rejected by exchange

POSSIBLE REASONS:
1. Wrong tradingsymbol format
2. Futures contract doesn't exist for that month
3. Market closed (MCX hours: 9:00 AM - 11:30 PM)
4. Insufficient margin
5. Trading holiday

CHECK:
- Is tradingsymbol correct? (GOLD25FEBFUT)
- Is exchange correct? (MCX for commodities)
- Are you trading during market hours?

---

PROBLEM: Still parsing as OPTIONS
[OPTIONS] Detected options signal (should be FUTURES)

SOLUTION:
Parser detection logic:
- Looks for month codes (FEB, MAR, etc.)
- Combined with commodity names (GOLD, SILVER, etc.)
- No CE/PE = Futures

Check if message has these keywords.

""")

print("="*80)
print("YOU'RE DONE!")
print("="*80)

print("""
Your trading bot now supports:
✅ NFO OPTIONS (NIFTY, BANKNIFTY, SENSEX, stocks)
✅ MCX FUTURES (GOLD, SILVER, CRUDE, etc.)

Both are:
✅ Auto-detected
✅ Auto-parsed
✅ Auto-enriched (expiry, quantity)
✅ Auto-traded

All in one system! 🚀📈

Questions? Just ask! 😊
""")
