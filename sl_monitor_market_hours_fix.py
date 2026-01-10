"""
Fix SL Monitor - Skip checks after market close (3:30 PM IST)
Add market hours check to avoid AMO errors
"""

# Add this to your SL monitor at the top after imports:

from datetime import datetime
import pytz

def is_market_open():
    """
    Check if market is open for regular orders
    NSE/NFO: 9:15 AM to 3:30 PM IST (Mon-Fri)
    """
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    
    # Check if weekend
    if now.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return False
    
    # Market hours: 9:15 AM to 3:30 PM
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    
    return market_open <= now <= market_close

# ============================================================================
# In your monitor_positions() method, add this at the very beginning:
# ============================================================================

def monitor_positions(self):
    """Monitor all open positions and manage SL orders"""
    
    # CHECK MARKET HOURS FIRST
    if not is_market_open():
        ist = pytz.timezone('Asia/Kolkata')
        current_time = datetime.now(ist).strftime('%H:%M:%S')
        logging.info(f"[SKIP] Market closed at {current_time} IST - No SL monitoring")
        logging.info(f"[INFO] Market hours: 9:15 AM - 3:30 PM IST (Mon-Fri)")
        return  # Exit early - don't check positions
    
    # REST OF YOUR EXISTING CODE...
    try:
        positions = self.kite.positions()
        # ... etc

# ============================================================================
# ALTERNATIVE: Add to continuous_monitor() method:
# ============================================================================

def continuous_monitor(self, interval=30):
    """Continuously monitor positions"""
    logging.info(f"[START] SL Monitor started (checks every {interval}s)")
    logging.info(f"[INFO] Will auto-pause outside market hours (9:15 AM - 3:30 PM IST)")
    
    while True:
        try:
            # Check market hours before monitoring
            if is_market_open():
                self.monitor_positions()
            else:
                # Market closed - just log once per minute to avoid spam
                if not hasattr(self, '_last_closed_log') or \
                   (datetime.now() - self._last_closed_log).seconds >= 60:
                    ist = pytz.timezone('Asia/Kolkata')
                    current_time = datetime.now(ist).strftime('%H:%M:%S')
                    logging.info(f"[PAUSED] Market closed ({current_time} IST) - Waiting for 9:15 AM")
                    self._last_closed_log = datetime.now()
            
            time.sleep(interval)
            
        except KeyboardInterrupt:
            logging.info("\n[STOP] SL Monitor stopped by user")
            break
        except Exception as e:
            logging.error(f"[ERROR] {e}")
            time.sleep(interval)

print("""
================================================================================
DEPLOYMENT INSTRUCTIONS
================================================================================

1. Add 'import pytz' at the top of sl_monitor_with_trailing.py
   (Install if needed: pip install pytz)

2. Add the is_market_open() function near the top of your file

3. Add market hours check at the beginning of monitor_positions() method:
   
   if not is_market_open():
       logging.info("[SKIP] Market closed - No SL monitoring")
       return

4. Restart SL monitor

WHAT THIS FIXES:
✅ No AMO errors after 3:30 PM
✅ Auto-pauses monitoring outside market hours
✅ Auto-resumes at 9:15 AM next trading day
✅ Skips weekends automatically

MARKET HOURS:
- Regular orders: 9:15 AM - 3:30 PM IST (Mon-Fri)
- AMO orders: 3:45 PM - 8:57 AM (next day)
- Our fix: Monitor only during 9:15 AM - 3:30 PM

After 3:30 PM, SL monitor will:
1. Stop checking positions
2. Log "Market closed" message
3. Wait until 9:15 AM next day
4. Auto-resume monitoring

================================================================================
""")
