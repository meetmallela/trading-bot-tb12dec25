"""
check_schema.py
Check exact column names in both databases
"""

import sqlite3

print("="*80)
print("DATABASE SCHEMA CHECK")
print("="*80)
print()

# Check jp_signals_trained.db
print("[1] jp_signals_trained.db - signals table")
print("-"*80)

conn = sqlite3.connect('jp_signals_trained.db')
cursor = conn.cursor()

cursor.execute("PRAGMA table_info(signals);")
columns = cursor.fetchall()

print("Columns:")
for col in columns:
    col_id, col_name, col_type, not_null, default, pk = col
    print(f"  {col_name:20s} {col_type:15s}")

# Get sample row
cursor.execute("SELECT * FROM signals LIMIT 1;")
sample = cursor.fetchone()

print(f"\nSample row (first 10 values):")
if sample:
    for i, val in enumerate(sample[:10]):
        col_name = columns[i][1] if i < len(columns) else f"col_{i}"
        print(f"  {col_name}: {val}")

conn.close()

print()
print("="*80)
print()

# Check jp_kb.db
print("[2] jp_kb.db - training_data table")
print("-"*80)

conn = sqlite3.connect('jp_kb.db')
cursor = conn.cursor()

cursor.execute("PRAGMA table_info(training_data);")
columns = cursor.fetchall()

print("Columns:")
for col in columns:
    col_id, col_name, col_type, not_null, default, pk = col
    print(f"  {col_name:20s} {col_type:15s}")

# Get sample row
cursor.execute("SELECT * FROM training_data LIMIT 1;")
sample = cursor.fetchone()

print(f"\nSample row:")
if sample:
    for i, val in enumerate(sample):
        col_name = columns[i][1] if i < len(columns) else f"col_{i}"
        val_str = str(val)[:100] if val else "NULL"
        print(f"  {col_name}: {val_str}")

conn.close()

print()
print("="*80)
print("Copy these column names to extract_training_examples.py")
print("="*80)