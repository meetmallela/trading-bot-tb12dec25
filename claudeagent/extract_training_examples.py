"""
extract_training_examples_final.py
Extract training examples using correct column names
"""

import sqlite3
import json
from datetime import datetime

DB_SIGNALS = 'jp_signals_trained.db'  # 70 production signals
DB_KB = 'jp_kb.db'  # 184 KB examples
OUTPUT_FILE = 'training_examples_production.json'

print("="*80)
print("EXTRACTING TRAINING EXAMPLES")
print("="*80)
print()

all_examples = []
seen_inputs = set()

# ============================================================================
# EXTRACT FROM jp_signals_trained.db (Production signals)
# ============================================================================

print("[1] Extracting from jp_signals_trained.db...")
print("-"*80)

conn = sqlite3.connect(DB_SIGNALS)
cursor = conn.cursor()

# Get all signals with parsed_data
query = """
SELECT 
    raw_text,
    parsed_data,
    timestamp
FROM signals
WHERE 
    parsed_data IS NOT NULL
    AND processed = 1
ORDER BY timestamp DESC
"""

cursor.execute(query)
results = cursor.fetchall()
conn.close()

print(f"✓ Found {len(results)} production signals")

for row in results:
    raw_text, parsed_data_json, timestamp = row
    
    # Clean input
    input_clean = raw_text.strip().lower() if raw_text else ""
    
    if not input_clean or input_clean in seen_inputs:
        continue
    
    # Parse JSON
    try:
        parsed_data = json.loads(parsed_data_json)
    except:
        continue
    
    # Create training example
    example = {
        "input": raw_text.strip(),
        "expected_output": {
            "symbol": parsed_data.get("symbol"),
            "strike": int(parsed_data.get("strike", 0)),
            "option_type": parsed_data.get("option_type"),
            "entry_price": float(parsed_data.get("entry_price", 0)) if parsed_data.get("entry_price") else None,
            "stop_loss": float(parsed_data.get("stop_loss", 0)) if parsed_data.get("stop_loss") else None,
            "tradingsymbol": parsed_data.get("tradingsymbol"),
            "exchange": parsed_data.get("exchange", "NFO"),
            "expiry_date": parsed_data.get("expiry_date"),
            "quantity": int(parsed_data.get("quantity", 1))
        },
        "source": "production_signals",
        "date": timestamp
    }
    
    all_examples.append(example)
    seen_inputs.add(input_clean)

print(f"✓ Added {len(all_examples)} examples from production")
print()

# ============================================================================
# EXTRACT FROM jp_kb.db (Knowledge base with corrections)
# ============================================================================

print("[2] Extracting from jp_kb.db...")
print("-"*80)

conn = sqlite3.connect(DB_KB)
cursor = conn.cursor()

# Get all training data (prefer correct_data over parsed_data)
query = """
SELECT 
    raw_message,
    parsed_data,
    correct_data,
    is_valid,
    timestamp
FROM training_data
ORDER BY timestamp DESC
"""

cursor.execute(query)
kb_results = cursor.fetchall()
conn.close()

print(f"✓ Found {len(kb_results)} KB examples")

kb_added = 0
for row in kb_results:
    raw_message, parsed_data_json, correct_data_json, is_valid, timestamp = row
    
    # Clean input
    input_clean = raw_message.strip().lower() if raw_message else ""
    
    if not input_clean or input_clean in seen_inputs:
        continue
    
    # Use correct_data if available, else parsed_data
    data_json = correct_data_json if correct_data_json else parsed_data_json
    
    if not data_json:
        continue
    
    # Parse JSON
    try:
        data = json.loads(data_json)
    except:
        continue
    
    # Create training example
    example = {
        "input": raw_message.strip(),
        "expected_output": {
            "symbol": data.get("symbol"),
            "strike": int(data.get("strike", 0)),
            "option_type": data.get("option_type"),
            "entry_price": float(data.get("entry_price", 0)) if data.get("entry_price") else None,
            "stop_loss": float(data.get("stop_loss", 0)) if data.get("stop_loss") else None,
            "tradingsymbol": data.get("tradingsymbol"),
            "exchange": data.get("exchange", "NFO"),
            "expiry_date": data.get("expiry_date"),
            "quantity": int(data.get("quantity", 1))
        },
        "source": "kb_corrected" if correct_data_json else "kb_parsed",
        "is_valid": is_valid,
        "date": timestamp
    }
    
    all_examples.append(example)
    seen_inputs.add(input_clean)
    kb_added += 1

print(f"✓ Added {kb_added} examples from KB")
print()

# ============================================================================
# ANALYSIS
# ============================================================================

print("="*80)
print("TRAINING SET SUMMARY")
print("="*80)
print(f"Total unique examples: {len(all_examples)}")
print()

# Count by source
source_counts = {}
for ex in all_examples:
    source = ex.get('source', 'unknown')
    source_counts[source] = source_counts.get(source, 0) + 1

print("By source:")
for source, count in sorted(source_counts.items()):
    print(f"  {source}: {count}")

print()

# Symbol breakdown
print("By symbol (top 20):")
symbol_counts = {}
for ex in all_examples:
    symbol = ex.get('expected_output', {}).get('symbol', 'UNKNOWN')
    symbol_counts[symbol] = symbol_counts.get(symbol, 0) + 1

for symbol, count in sorted(symbol_counts.items(), key=lambda x: -x[1])[:20]:
    print(f"  {symbol}: {count} examples")

print()

# Index vs Stock options
INDEX_SYMBOLS = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX']
index_count = sum(1 for ex in all_examples if ex.get('expected_output', {}).get('symbol') in INDEX_SYMBOLS)
stock_count = len(all_examples) - index_count

print("By type:")
print(f"  Index options: {index_count}")
print(f"  Stock options: {stock_count}")

# Save to JSON
with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
    json.dump(all_examples, f, indent=2, ensure_ascii=False)

print()
print(f"✓ Saved {len(all_examples)} examples to {OUTPUT_FILE}")

# Show samples
print()
print("="*80)
print("SAMPLE TRAINING EXAMPLES")
print("="*80)

for i, example in enumerate(all_examples[:5], 1):
    print(f"\nExample {i}:")
    print(f"  Input: {example['input'][:60]}...")
    output = example.get('expected_output', {})
    print(f"  Symbol: {output.get('symbol')}")
    print(f"  Strike: {output.get('strike')}")
    print(f"  Type: {output.get('option_type')}")
    print(f"  Tradingsymbol: {output.get('tradingsymbol')}")
    print(f"  Source: {example.get('source')}")

print()
print("="*80)
print("NEXT STEP: python retrain_agent.py")
print("="*80)
