"""
correction_interface.py
Interactive tool for correcting Claude's parsing and building KB
"""

import json
import sqlite3
from datetime import datetime

class CorrectionInterface:
    """Interface for reviewing and correcting Claude's parsing"""
    
    def __init__(self, signals_db='premium_signals.db', kb_db='agent_kb.db'):
        self.signals_db = signals_db
        self.kb_db = kb_db
    
    def get_recent_signals(self, limit=10):
        """Get recent signals for review"""
        conn = sqlite3.connect(self.signals_db)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM signals 
            WHERE parser_type = 'CLAUDE_AGENT'
            ORDER BY timestamp DESC 
            LIMIT ?
        """, (limit,))
        
        signals = []
        for row in cursor.fetchall():
            signals.append(dict(row))
        
        conn.close()
        return signals
    
    def review_signal(self, signal_id):
        """Review a specific signal"""
        conn = sqlite3.connect(self.signals_db)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM signals WHERE id = ?", (signal_id,))
        signal = cursor.fetchone()
        conn.close()
        
        if not signal:
            print(f"Signal {signal_id} not found")
            return
        
        print("\n" + "="*70)
        print(f"SIGNAL #{signal['id']} - {signal['channel_name']}")
        print("="*70)
        print(f"\nRAW MESSAGE:")
        print(signal['raw_text'])
        print(f"\nCLAUDE'S PARSING:")
        parsed = json.loads(signal['parsed_data'])
        print(json.dumps(parsed, indent=2))
        print("\n" + "="*70)
        
        return signal, parsed
    
    def save_correction(self, signal_id, corrected_data, notes=None):
        """Save correction to KB"""
        # Get original signal
        conn = sqlite3.connect(self.signals_db)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM signals WHERE id = ?", (signal_id,))
        signal = cursor.fetchone()
        conn.close()
        
        if not signal:
            print(f"Signal {signal_id} not found")
            return
        
        # Save to KB
        kb_conn = sqlite3.connect(self.kb_db)
        kb_cursor = kb_conn.cursor()
        
        kb_cursor.execute("""
            INSERT INTO parsing_corrections 
            (raw_message, claude_parsed, human_corrected, timestamp, channel_id, notes)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            signal['raw_text'],
            signal['parsed_data'],
            json.dumps(corrected_data),
            datetime.now().isoformat(),
            signal['channel_id'],
            notes
        ))
        
        kb_conn.commit()
        kb_conn.close()
        
        print(f"\n[OK] Correction saved to KB!")
        print(f"Claude will learn from this example in future parsings.\n")
    
    def interactive_review(self):
        """Interactive correction interface"""
        print("\n" + "="*70)
        print("CORRECTION INTERFACE - Build Your Knowledge Base")
        print("="*70)
        print("\nThis tool helps Claude learn from your corrections.")
        print("Review recent signals and fix any parsing errors.\n")
        
        signals = self.get_recent_signals(20)
        
        if not signals:
            print("No signals to review!")
            return
        
        print(f"Found {len(signals)} recent signals\n")
        
        for i, sig in enumerate(signals, 1):
            print(f"{i}. [{sig['timestamp'][:16]}] {sig['channel_name']}")
            preview = sig['raw_text'][:60].replace('\n', ' ')
            print(f"   {preview}...")
        
        print("\nCommands:")
        print("  [number] - Review signal")
        print("  'q' - Quit")
        print("  'stats' - Show KB statistics")
        
        while True:
            try:
                cmd = input("\n> ").strip()
                
                if cmd.lower() == 'q':
                    break
                
                if cmd.lower() == 'stats':
                    self.show_stats()
                    continue
                
                try:
                    idx = int(cmd) - 1
                    if 0 <= idx < len(signals):
                        self.review_and_correct(signals[idx])
                    else:
                        print("Invalid signal number")
                except ValueError:
                    print("Invalid command")
                    
            except KeyboardInterrupt:
                print("\n\nExiting...")
                break
    
    def review_and_correct(self, signal):
        """Review and optionally correct a signal"""
        signal_obj, parsed = self.review_signal(signal['id'])
        
        print("\nIs this parsing CORRECT? (y/n/skip)")
        response = input("> ").strip().lower()
        
        if response == 'skip' or response == 's':
            return
        
        if response == 'y':
            print("Great! No correction needed.")
            return
        
        if response == 'n':
            print("\nProvide corrected JSON:")
            print("(You can copy Claude's output above and modify it)")
            print("Enter JSON (end with empty line):\n")
            
            json_lines = []
            while True:
                line = input()
                if line.strip() == '':
                    break
                json_lines.append(line)
            
            try:
                corrected = json.loads('\n'.join(json_lines))
                
                print("\nCorrected parsing:")
                print(json.dumps(corrected, indent=2))
                print("\nSave this correction? (y/n)")
                
                if input("> ").strip().lower() == 'y':
                    notes = input("Notes (optional): ").strip()
                    self.save_correction(signal['id'], corrected, notes or None)
                    
            except json.JSONDecodeError as e:
                print(f"Invalid JSON: {e}")
    
    def show_stats(self):
        """Show KB statistics"""
        conn = sqlite3.connect(self.kb_db)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM parsing_corrections")
        total = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT channel_id, COUNT(*) 
            FROM parsing_corrections 
            GROUP BY channel_id
        """)
        by_channel = cursor.fetchall()
        
        conn.close()
        
        print("\n" + "="*70)
        print("KNOWLEDGE BASE STATISTICS")
        print("="*70)
        print(f"Total corrections: {total}")
        print(f"\nBy channel:")
        for channel_id, count in by_channel:
            print(f"  {channel_id}: {count} corrections")
        print("="*70)


def main():
    """Main function"""
    interface = CorrectionInterface()
    interface.interactive_review()


if __name__ == "__main__":
    main()
