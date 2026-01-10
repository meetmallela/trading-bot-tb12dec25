import sqlite3
conn = sqlite3.connect('premium_signals.db')
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM signals")
print(f"Messages: {cursor.fetchone()[0]}")