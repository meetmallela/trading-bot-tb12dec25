"""
fix_training_limit.py
Automatically update jp_channel_agent_trained.py to use 250 examples
"""

import os
import shutil

FILE = 'jp_channel_agent_trained.py'
BACKUP = 'jp_channel_agent_trained.py.backup'

print("="*80)
print("AUTO-FIX: Updating Training Examples Limit")
print("="*80)
print()

# Check if file exists
if not os.path.exists(FILE):
    print(f"❌ {FILE} not found!")
    print("   Make sure you're in the correct directory")
    exit(1)

print(f"✓ Found {FILE}")

# Create backup
print(f"Creating backup: {BACKUP}")
shutil.copy2(FILE, BACKUP)
print(f"✓ Backup created")

# Read file
with open(FILE, 'r', encoding='utf-8') as f:
    content = f.read()

# Check current state
if 'LIMIT 100' in content:
    print("\n✓ Found 'LIMIT 100' - will update to 'LIMIT 250'")
    new_content = content.replace('LIMIT 100', 'LIMIT 250')
    changes = 1
elif 'LIMIT 250' in content:
    print("\n✓ Already using 'LIMIT 250' - no changes needed!")
    exit(0)
else:
    print("\n⚠️  Could not find 'LIMIT 100' in file")
    print("   Searching for any LIMIT statement...")
    
    import re
    limits = re.findall(r'LIMIT\s+(\d+)', content)
    if limits:
        print(f"   Found: {limits}")
        print("   Please update manually or check the file")
    else:
        print("   No LIMIT statement found!")
    exit(1)

# Write updated file
with open(FILE, 'w', encoding='utf-8') as f:
    f.write(new_content)

print(f"✓ Updated {FILE}")
print()

print("="*80)
print("SUCCESS!")
print("="*80)
print()
print("Changes made:")
print("  LIMIT 100 → LIMIT 250")
print()
print("Next steps:")
print("  1. Restart telegram reader:")
print("     python telegram_reader_jp_trained.py")
print()
print("  2. Verify in logs:")
print("     [KB] Loaded 184 training examples  ← Should be 184, not 20!")
print()
print(f"Backup saved as: {BACKUP}")
print("="*80)
