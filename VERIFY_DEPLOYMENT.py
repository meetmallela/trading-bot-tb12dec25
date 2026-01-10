"""
VERIFICATION SCRIPT - Check if MCX Hours + Re-Entry Prevention is Deployed
===========================================================================
"""

import os
import sys

print("="*80)
print("DEPLOYMENT VERIFICATION SCRIPT")
print("="*80)
print()

# Colors for output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
RESET = '\033[0m'

def check_pass(msg):
    print(f"{GREEN}✅ PASS{RESET} - {msg}")

def check_fail(msg):
    print(f"{RED}❌ FAIL{RESET} - {msg}")

def check_warn(msg):
    print(f"{YELLOW}⚠️  WARN{RESET} - {msg}")

# ============================================================================
# CHECK 1: Files Exist
# ============================================================================
print("CHECK 1: Required Files")
print("-" * 80)

files_to_check = [
    'sl_monitor_with_trailing.py',
    'order_placer_db_production.py'
]

for filename in files_to_check:
    if os.path.exists(filename):
        check_pass(f"{filename} exists")
    else:
        check_fail(f"{filename} NOT FOUND")

print()

# ============================================================================
# CHECK 2: MCX Hours in SL Monitor
# ============================================================================
print("CHECK 2: MCX Hours Support (9 AM - 11:55 PM)")
print("-" * 80)

try:
    with open('sl_monitor_with_trailing.py', 'r', encoding='utf-8') as f:
        sl_monitor_code = f.read()
    
    # Check for MCX hours
    if 'mcx_open' in sl_monitor_code.lower() or 'MCX:' in sl_monitor_code:
        check_pass("MCX hours code found in SL monitor")
    else:
        check_fail("MCX hours code NOT FOUND in SL monitor")
    
    # Check for 23:55 or 11:55 PM
    if '23' in sl_monitor_code and '55' in sl_monitor_code:
        check_pass("MCX closing time (11:55 PM) configured")
    else:
        check_fail("MCX closing time NOT configured")
    
    # Check for NSE and MCX separation
    if 'nse_open' in sl_monitor_code.lower() or 'NSE/NFO' in sl_monitor_code:
        check_pass("NSE hours separated from MCX")
    else:
        check_warn("NSE hours separation not clear")

except FileNotFoundError:
    check_fail("sl_monitor_with_trailing.py not found")
except Exception as e:
    check_fail(f"Error reading SL monitor: {e}")

print()

# ============================================================================
# CHECK 3: Re-Entry Prevention in SL Monitor
# ============================================================================
print("CHECK 3: Re-Entry Prevention (Anti-Revenge Trading)")
print("-" * 80)

try:
    with open('sl_monitor_with_trailing.py', 'r', encoding='utf-8') as f:
        sl_monitor_code = f.read()
    
    # Check for sl_exits tracking
    if 'sl_exits_today' in sl_monitor_code:
        check_pass("SL exits tracking found")
    else:
        check_fail("SL exits tracking NOT FOUND")
    
    # Check for record_sl_exit function
    if 'record_sl_exit' in sl_monitor_code or 'BLACKLIST' in sl_monitor_code:
        check_pass("Blacklist recording function found")
    else:
        check_fail("Blacklist recording function NOT FOUND")
    
    # Check for sl_exits.json
    if 'sl_exits.json' in sl_monitor_code:
        check_pass("sl_exits.json persistence configured")
    else:
        check_fail("sl_exits.json persistence NOT configured")

except Exception as e:
    check_fail(f"Error checking re-entry prevention: {e}")

print()

# ============================================================================
# CHECK 4: Re-Entry Prevention in Order Placer
# ============================================================================
print("CHECK 4: Order Placer Blocks Re-Entry")
print("-" * 80)

try:
    with open('order_placer_db_production.py', 'r', encoding='utf-8') as f:
        order_placer_code = f.read()
    
    # Check for is_blocked_from_reentry
    if 'is_blocked_from_reentry' in order_placer_code or 'blocked_from_reentry' in order_placer_code.lower():
        check_pass("Re-entry blocking function found")
    else:
        check_fail("Re-entry blocking function NOT FOUND")
    
    # Check for sl_exits loading
    if '_load_sl_exits' in order_placer_code or 'sl_exits.json' in order_placer_code:
        check_pass("SL exits blacklist loading configured")
    else:
        check_fail("SL exits blacklist loading NOT configured")
    
    # Check for BLOCKED message
    if 'BLOCKED' in order_placer_code and 'RE-ENTRY' in order_placer_code:
        check_pass("Re-entry blocking messages found")
    else:
        check_warn("Re-entry blocking messages not clear")

except Exception as e:
    check_fail(f"Error checking order placer: {e}")

print()

# ============================================================================
# CHECK 5: No Averaging Down in Order Placer
# ============================================================================
print("CHECK 5: No Averaging Down Protection")
print("-" * 80)

try:
    with open('order_placer_db_production.py', 'r', encoding='utf-8') as f:
        order_placer_code = f.read()
    
    # Check for existing position check
    if 'check_existing_position' in order_placer_code or 'existing position' in order_placer_code.lower():
        check_pass("Existing position check found")
    else:
        check_fail("Existing position check NOT FOUND")
    
    # Check for averaging down blocking
    if 'AVERAGING DOWN' in order_placer_code or 'averaging down' in order_placer_code.lower():
        check_pass("Averaging down blocking found")
    else:
        check_fail("Averaging down blocking NOT FOUND")
    
    # Check for positions API call
    if 'kite.positions()' in order_placer_code:
        check_pass("Kite positions check configured")
    else:
        check_fail("Kite positions check NOT configured")

except Exception as e:
    check_fail(f"Error checking averaging down: {e}")

print()

# ============================================================================
# CHECK 6: Runtime Files
# ============================================================================
print("CHECK 6: Runtime Files")
print("-" * 80)

# Check if sl_exits.json exists
if os.path.exists('sl_exits.json'):
    check_pass("sl_exits.json exists (blacklist active)")
    try:
        import json
        with open('sl_exits.json', 'r') as f:
            data = json.load(f)
            if data:
                check_warn(f"Blacklist has {len(data)} instruments:")
                for symbol, date in data.items():
                    print(f"         ⛔ {symbol} (blocked on {date})")
            else:
                check_pass("Blacklist is empty (no SL exits yet)")
    except Exception as e:
        check_warn(f"Could not read sl_exits.json: {e}")
else:
    check_warn("sl_exits.json not found (will be created on first SL exit)")

print()

# ============================================================================
# CHECK 7: Log Files (if available)
# ============================================================================
print("CHECK 7: Recent Logs")
print("-" * 80)

log_files = [
    'sl_monitor.log',
    'order_placer.log',
    'order_placer_production.log'
]

for log_file in log_files:
    if os.path.exists(log_file):
        try:
            with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                # Read last 50 lines
                lines = f.readlines()[-50:]
                content = ''.join(lines)
                
                # Check for new features in logs
                if 'MCX 9:00-23:55' in content or 'MCX' in content:
                    check_pass(f"{log_file}: MCX hours mentioned")
                
                if 'BLACKLIST' in content or 'NO RE-ENTRY' in content:
                    check_pass(f"{log_file}: Re-entry prevention active")
                
                if 'AVERAGING DOWN' in content or 'Already have position' in content:
                    check_pass(f"{log_file}: Averaging down check active")
                
                if 'BLOCKED' in content:
                    check_pass(f"{log_file}: Blocking logic triggered")
        except Exception as e:
            check_warn(f"Could not read {log_file}: {e}")

print()

# ============================================================================
# SUMMARY
# ============================================================================
print("="*80)
print("SUMMARY")
print("="*80)
print()
print("To be FULLY deployed, you should see:")
print()
print("  ✅ MCX hours code (9 AM - 11:55 PM)")
print("  ✅ Re-entry prevention (sl_exits tracking)")
print("  ✅ Blacklist persistence (sl_exits.json)")
print("  ✅ Order placer blocks re-entry")
print("  ✅ Existing position check (no averaging down)")
print()
print("If you see ❌ FAIL messages above:")
print("  → You need to deploy the new files")
print("  → Copy from outputs folder:")
print("     • sl_monitor_MCX_HOURS_REENTRY_BLOCK.py")
print("     • order_placer_REENTRY_BLOCK_AVERAGING.py")
print()
print("="*80)
