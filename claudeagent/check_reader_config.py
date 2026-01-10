"""
check_reader_config.py
Check what KB files the telegram reader is using
"""

import re

print("="*80)
print("CHECKING TELEGRAM READER CONFIGURATION")
print("="*80)
print()

try:
    with open('telegram_reader_jp_trained.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Look for KB file references
    kb_patterns = [
        r'kb_file\s*=\s*["\']([^"\']+)["\']',
        r'kb_db\s*=\s*["\']([^"\']+)["\']',
        r'JPChannelAgentTrained\([^)]*kb[^)]*\)',
    ]
    
    print("KB file references in telegram_reader_jp_trained.py:")
    print("-"*80)
    
    for pattern in kb_patterns:
        matches = re.findall(pattern, content, re.IGNORECASE)
        for match in matches:
            print(f"  Found: {match}")
    
    print()
    
    # Look for the actual initialization
    init_match = re.search(r'JPChannelAgentTrained\((.*?)\)', content, re.DOTALL)
    if init_match:
        print("Agent initialization:")
        print("-"*80)
        print(init_match.group(0)[:200])
    
    print()
    print("="*80)
    
    # Check if jp_channel_training_kb.json exists
    import os
    json_kb = 'jp_channel_training_kb.json'
    db_kb = 'jp_kb.db'
    
    print("\nKB Files in directory:")
    print("-"*80)
    
    if os.path.exists(json_kb):
        size = os.path.getsize(json_kb) / 1024
        print(f"✓ {json_kb} ({size:.1f} KB) - EXISTS")
        
        # Count examples
        import json
        with open(json_kb, 'r') as f:
            data = json.load(f)
            examples = data.get('training_examples', [])
            print(f"  Contains {len(examples)} examples")
    else:
        print(f"✗ {json_kb} - NOT FOUND")
    
    if os.path.exists(db_kb):
        size = os.path.getsize(db_kb) / 1024
        print(f"✓ {db_kb} ({size:.1f} KB) - EXISTS")
        
        # Count examples
        import sqlite3
        conn = sqlite3.connect(db_kb)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM training_data")
        count = cursor.fetchone()[0]
        conn.close()
        print(f"  Contains {count} examples")
    else:
        print(f"✗ {db_kb} - NOT FOUND")
    
    print()
    print("="*80)
    print("DIAGNOSIS:")
    print("="*80)
    
    if os.path.exists(json_kb):
        print(f"\n⚠️  System is using {json_kb} (20 examples)")
        print("   This overrides the database!")
        print()
        print("SOLUTION:")
        print(f"  1. Rename/delete {json_kb}")
        print(f"  2. System will then use {db_kb} (184 examples)")
        print("  3. Restart telegram reader")
    else:
        print("\n✓ No JSON KB found")
        print("  System should be using database")
        print("  Check jp_channel_agent_trained.py initialization")

except FileNotFoundError:
    print("❌ telegram_reader_jp_trained.py not found")
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
