"""
COMPLETE FUTURES SUPPORT DEPLOYMENT GUIDE
==========================================

Your signal "BUY GOLD FEB CMP 136830 WITH SL" will now be parsed and traded!

STEP-BY-STEP DEPLOYMENT:
"""

print("="*80)
print("ADDING FUTURES SUPPORT TO YOUR TRADING BOT")
print("="*80)

print("""
WHAT YOU'LL GET:
✅ Parse both OPTIONS (CE/PE) and FUTURES signals
✅ Auto-detect signal type (Options vs Futures)
✅ Place futures orders via Zerodha
✅ Proper lot sizes for MCX commodities
✅ Expiry date enrichment for futures
✅ Stop loss support for futures

EXAMPLE SIGNALS THAT WILL NOW WORK:
- "BUY GOLD FEB CMP 136830 WITH SL 136500"  ← FUTURES
- "SELL SILVER MAR 70000 SL 71000"          ← FUTURES  
- "NIFTY 25900 CE BUY 140 SL 130"           ← OPTIONS (still works)

""")

print("="*80)
print("STEP 1: REPLACE PARSER")
print("="*80)

print("""
1.1 Backup your current parser:
    copy signal_parser_with_claude_fallback.py signal_parser_with_claude_fallback.py.backup

1.2 Download: signal_parser_with_futures.py

1.3 Rename it to replace the old one:
    copy signal_parser_with_futures.py signal_parser_with_claude_fallback.py
    
    (OR just use the new name and update your imports)

WHAT THIS DOES:
- Detects if signal is FUTURES or OPTIONS
- Parses futures signals (GOLD FEB, SILVER MAR, etc.)
- Adds expiry dates for futures (last day of month)
- Adds proper lot sizes (GOLD=100, SILVER=30000, etc.)
- Sets strike=None and option_type=None for futures
- Adds instrument_type field ('FUTURES' or 'OPTIONS')
""")

print("="*80)
print("STEP 2: UPDATE ORDER PLACER")
print("="*80)

print("""
2.1 Open: order_placer_db_production.py

2.2 Add the place_futures_order() method from order_placer_futures_patch.py
    (Copy the entire function into your OrderPlacer class)

2.3 Modify your process_signal() method:

    OLD CODE:
    --------
    def process_signal(self, signal_record):
        signal_data = json.loads(parsed_json)
        # ... process as options ...
    
    NEW CODE:
    --------
    def process_signal(self, signal_record):
        signal_data = json.loads(parsed_json)
        
        # Check instrument type
        instrument_type = signal_data.get('instrument_type', 'OPTIONS')
        
        if instrument_type == 'FUTURES':
            # Handle futures
            order_result = self.place_futures_order(signal_data)
        else:
            # Handle options (existing logic)
            # ... your existing code ...

WHAT THIS DOES:
- Checks if signal is FUTURES or OPTIONS
- Places MARKET order for futures on MCX exchange
- Uses proper quantity (lot size)
- Saves order to database with stop_loss for SL monitor
""")

print("="*80)
print("STEP 3: UPDATE TELEGRAM READER (OPTIONAL)")
print("="*80)

print("""
3.1 Open: telegram_reader_production.py

3.2 Update the import if you renamed the parser:

    OLD:
    from signal_parser_with_claude_fallback import SignalParserWithClaudeFallback
    
    NEW (if renamed):
    from signal_parser_with_futures import SignalParserWithFutures
    
    # Update initialization
    parser = SignalParserWithFutures(
        claude_api_key=claude_api_key,
        rules_file='parsing_rules_enhanced_v2.json'
    )

WHAT THIS DOES:
- Uses the new futures-enabled parser
- No other changes needed!
""")

print("="*80)
print("STEP 4: UPDATE DATABASE (IF NEEDED)")
print("="*80)

print("""
Your database should already work, but you can add an index for faster queries:

sqlite3 trading.db

ALTER TABLE signals ADD COLUMN instrument_type TEXT DEFAULT 'OPTIONS';
ALTER TABLE orders ADD COLUMN instrument_type TEXT DEFAULT 'OPTIONS';

.exit

This is optional - the system works without it.
""")

print("="*80)
print("STEP 5: TEST IT!")
print("="*80)

print("""
5.1 Restart telegram reader:
    python telegram_reader_production.py

5.2 Restart order placer:
    python order_placer_db_production.py --continuous

5.3 Wait for a futures signal like:
    "BUY GOLD FEB CMP 136830 WITH SL 136500"

5.4 Check logs - you should see:
    [FUTURES] Detected futures signal
    [✓ VALID] GOLD FUTURES | Action: BUY | Entry: 136830.0 | SL: 136500.0
    [FUTURES TASK] Processing Signal #XXX
    [OK] Futures order placed! Order ID: 123456789
""")

print("="*80)
print("EXAMPLE: WHAT WILL HAPPEN")
print("="*80)

print("""
SIGNAL RECEIVED:
"NEW CALLL BUY GOLD FEB CMP 136830 136700 WITH SL 136500"

PARSER OUTPUT:
{
  "instrument_type": "FUTURES",
  "symbol": "GOLD",
  "action": "BUY",
  "entry_price": 136830.0,
  "stop_loss": 136500.0,
  "expiry_date": "2025-02-28",
  "expiry_month": "FEB",
  "quantity": 100,  ← Auto-added (GOLD lot size)
  "exchange": "MCX",
  "tradingsymbol": "GOLD25FEBFUT",
  "strike": null,
  "option_type": null
}

ORDER PLACED:
- Symbol: GOLD25FEBFUT
- Exchange: MCX
- Action: BUY
- Quantity: 100
- Type: MARKET
- Product: MIS (Intraday)

SAVED TO DATABASE:
- signal_id: 123
- order_id: 987654321
- stop_loss: 136500  ← SL monitor will use this
""")

print("="*80)
print("SUPPORTED FUTURES")
print("="*80)

print("""
MCX COMMODITIES:
- GOLD      (Lot: 100)
- GOLDM     (Lot: 10)
- SILVER    (Lot: 30000)
- SILVERM   (Lot: 5000)
- CRUDEOIL  (Lot: 100)
- NATURALGAS (Lot: 1250)
- COPPER    (Lot: 1000)
- ZINC      (Lot: 5000)
- LEAD      (Lot: 5000)
- NICKEL    (Lot: 250)

EXPIRY MONTHS:
JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC
""")

print("="*80)
print("THAT'S IT!")
print("="*80)

print("""
Your bot will now handle:
✅ OPTIONS: NIFTY 25900 CE BUY 140 SL 130
✅ FUTURES: BUY GOLD FEB CMP 136830 SL 136500

Both will be parsed, validated, and traded automatically!

Questions? Just ask! 🚀
""")
