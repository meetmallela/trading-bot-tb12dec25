"""
find_gold_instruments.py - Search for GOLD instruments in your CSV files
"""

import csv
import sys
from datetime import datetime

def search_gold_instruments(strike=136000, option_type='CE'):
    """Search for GOLD instruments with given strike and type"""
    
    files_to_check = [
        'instruments_cache.csv',
        'config/valid_instruments.csv'
    ]
    
    print("\n" + "="*80)
    print(f"SEARCHING FOR: GOLD {strike} {option_type}")
    print("="*80)
    
    all_gold_instruments = []
    
    for filepath in files_to_check:
        try:
            print(f"\nChecking: {filepath}")
            print("-"*80)
            
            with open(filepath, 'r') as f:
                # Check if it's just a list of instruments (one per line)
                first_line = f.readline().strip()
                f.seek(0)
                
                if ',' in first_line:
                    # CSV with columns
                    reader = csv.reader(f)
                    for row in reader:
                        instrument = row[0] if row else ''
                        if instrument.startswith('GOLD'):
                            all_gold_instruments.append(instrument)
                else:
                    # Simple list, one per line
                    for line in f:
                        instrument = line.strip()
                        if instrument.startswith('GOLD'):
                            all_gold_instruments.append(instrument)
            
            print(f"Found {len([i for i in all_gold_instruments if filepath in str(i)])} GOLD instruments")
            
        except FileNotFoundError:
            print(f"File not found: {filepath}")
        except Exception as e:
            print(f"Error reading {filepath}: {e}")
    
    if not all_gold_instruments:
        print("\n[ERROR] No GOLD instruments found!")
        print("\nPlease check:")
        print("1. Files exist: instruments_cache.csv, config/valid_instruments.csv")
        print("2. Files contain instrument data")
        return
    
    # Remove duplicates
    all_gold_instruments = list(set(all_gold_instruments))
    
    print(f"\n{'='*80}")
    print(f"TOTAL GOLD INSTRUMENTS FOUND: {len(all_gold_instruments)}")
    print("="*80)
    
    # Parse instruments to find different expiries and strikes
    expiries = set()
    strikes = set()
    
    for inst in all_gold_instruments:
        # Format: GOLD25DEC88600CE or GOLD26JAN88600CE
        try:
            # Extract expiry (25DEC, 26JAN, etc.)
            if len(inst) > 10:
                # Find where numbers start after GOLD
                year_part = inst[4:6]  # 25, 26
                month_part = inst[6:9]  # DEC, JAN
                expiry = f"{year_part}{month_part}"
                expiries.add(expiry)
                
                # Extract strike (find where CE or PE is)
                if 'CE' in inst:
                    strike_str = inst[9:inst.index('CE')]
                elif 'PE' in inst:
                    strike_str = inst[9:inst.index('PE')]
                else:
                    continue
                
                if strike_str.isdigit():
                    strikes.add(int(strike_str))
        except:
            pass
    
    print(f"\nAvailable Expiries:")
    print("-"*80)
    for exp in sorted(expiries):
        print(f"  {exp}")
    
    print(f"\nStrike Price Range:")
    print("-"*80)
    if strikes:
        print(f"  Min: {min(strikes)}")
        print(f"  Max: {max(strikes)}")
        print(f"  Count: {len(strikes)} different strikes")
    
    # Check for exact match
    print(f"\n{'='*80}")
    print(f"SEARCHING FOR EXACT MATCH: {strike} {option_type}")
    print("="*80)
    
    matches = []
    for inst in all_gold_instruments:
        if str(strike) in inst and option_type in inst:
            matches.append(inst)
    
    if matches:
        print(f"\n[OK] Found {len(matches)} matching instruments:")
        for match in sorted(matches):
            print(f"  {match}")
        
        # Recommend which one to use
        print(f"\n{'='*80}")
        print("RECOMMENDATION")
        print("="*80)
        
        # Find nearest expiry
        today = datetime.now()
        
        nearest = None
        nearest_date = None
        
        for match in matches:
            try:
                # Parse expiry from instrument name
                year_part = match[4:6]
                month_part = match[6:9]
                
                # Convert to full year
                year = 2000 + int(year_part)
                
                # Convert month name to number
                months = {
                    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4,
                    'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8,
                    'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                }
                month = months.get(month_part)
                
                if month:
                    # GOLD expires on 5th of month
                    expiry_date = datetime(year, month, 5)
                    
                    if expiry_date > today:
                        if nearest_date is None or expiry_date < nearest_date:
                            nearest_date = expiry_date
                            nearest = match
            except:
                pass
        
        if nearest:
            print(f"\nUse this instrument: {nearest}")
            print(f"Expiry date: {nearest_date.strftime('%Y-%m-%d')}")
            print(f"\nSQL to fix signal #209:")
            print("-"*80)
            print(f"""sqlite3 trading.db "
UPDATE signals 
SET 
    parsed_data = json_set(parsed_data, '$.expiry_date', '{nearest_date.strftime('%Y-%m-%d')}'),
    processed = 0
WHERE id = 209;
" """)
    else:
        print(f"\n[WARNING] No instruments found for GOLD {strike} {option_type}")
        print(f"\nClosest strikes available:")
        
        # Find closest strikes
        if strikes:
            sorted_strikes = sorted(strikes)
            closest_lower = max([s for s in sorted_strikes if s < strike], default=None)
            closest_higher = min([s for s in sorted_strikes if s > strike], default=None)
            
            if closest_lower:
                print(f"  Lower: {closest_lower}")
            if closest_higher:
                print(f"  Higher: {closest_higher}")
    
    # Show some sample instruments
    print(f"\n{'='*80}")
    print("SAMPLE GOLD INSTRUMENTS (First 20)")
    print("="*80)
    for inst in sorted(all_gold_instruments)[:20]:
        print(f"  {inst}")
    
    if len(all_gold_instruments) > 20:
        print(f"  ... and {len(all_gold_instruments) - 20} more")


if __name__ == "__main__":
    # Default search for signal #209
    strike = 136000
    option_type = 'CE'
    
    if len(sys.argv) > 1:
        strike = int(sys.argv[1])
    if len(sys.argv) > 2:
        option_type = sys.argv[2]
    
    search_gold_instruments(strike, option_type)
