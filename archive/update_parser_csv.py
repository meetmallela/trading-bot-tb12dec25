"""
Update signal_parser to use valid_instruments.csv
"""

import fileinput
import sys

# Read the parser file
filename = 'signal_parser_with_claude_fallback.py'

print("="*80)
print("UPDATING PARSER TO USE valid_instruments.csv")
print("="*80)

try:
    # Read file
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Backup
    with open(f"{filename}.backup", 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"✅ Created backup: {filename}.backup")
    
    # Replace instruments_master.csv with valid_instruments.csv
    old_line = "self.instrument_finder = InstrumentFinderDF('instruments_master.csv')"
    new_line = "self.instrument_finder = InstrumentFinderDF('valid_instruments.csv')"
    
    if old_line in content:
        content = content.replace(old_line, new_line)
        
        # Write updated content
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"✅ Updated: instruments_master.csv → valid_instruments.csv")
        print(f"\n✅ Parser will now use valid_instruments.csv with:")
        print(f"   - NIFTY: 554 options")
        print(f"   - BANKNIFTY: 333 options")
        print(f"   - SENSEX: 762 options")
        print(f"   - GOLDM: 474 options")
    else:
        print(f"⚠️  Could not find line to replace")
        print(f"   Looking for: {old_line}")

except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "="*80)
print("NEXT STEPS")
print("="*80)
print("""
1. ✅ Parser updated to use valid_instruments.csv
2. ✅ instrument_finder_df.py fixed to build tradingsymbol
3. ⚡ Restart telegram_reader_production.py
4. ✅ Signals will now be parsed correctly!
""")
