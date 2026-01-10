"""
replay_messages_simulator.py - Test new parser with historical messages
Simulates telegram_reader behavior using messages from telegram_messages/ folder
"""

import re
import json
import logging
from datetime import datetime
from signal_parser_enhanced_v2 import EnhancedSignalParser
import sqlite3

class MessageReplaySimulator:
    """Replay historical messages through enhanced parser"""
    
    def __init__(self, messages_file, output_db='simulation_results.db'):
        self.messages_file = messages_file
        self.output_db = output_db
        self.parser = EnhancedSignalParser(
            rules_file='parsing_rules_enhanced_v2.json',
            instruments_cache='instruments_cache.csv'
        )
        self.setup_logging()
        self.setup_database()
        
        # Statistics
        self.stats = {
            'total_messages': 0,
            'ignored': 0,
            'parsed': 0,
            'failed': 0,
            'auto_symbol_detected': 0,
            'auto_expiry_calculated': 0,
            'commodities': 0,
            'ignore_reasons': {}
        }
    
    def setup_logging(self):
        """Setup logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('simulation.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('SIMULATOR')
    
    def setup_database(self):
        """Create simulation database"""
        conn = sqlite3.connect(self.output_db)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS simulation_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_text TEXT,
                channel_id TEXT,
                channel_name TEXT,
                message_id TEXT,
                timestamp TEXT,
                status TEXT,
                ignore_reason TEXT,
                parsed_data TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()
        self.logger.info(f"Database ready: {self.output_db}")
    
    def parse_message_file(self):
        """Parse the telegram messages text file"""
        messages = []
        
        with open(self.messages_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Split by separator
        message_blocks = content.split('='*80)
        
        for block in message_blocks:
            if 'MESSAGE RECEIVED' not in block and 'MESSAGE CONTENT' not in block:
                continue
            
            # Extract message details
            message_data = {
                'channel_id': None,
                'channel_name': None,
                'message_id': None,
                'timestamp': None,
                'content': None
            }
            
            lines = block.split('\n')
            capture_content = False
            content_lines = []
            
            for line in lines:
                line = line.strip()
                
                if line.startswith('Channel ID:'):
                    message_data['channel_id'] = line.replace('Channel ID:', '').strip()
                elif line.startswith('Channel Name:'):
                    message_data['channel_name'] = line.replace('Channel Name:', '').strip()
                elif line.startswith('Message ID:'):
                    message_data['message_id'] = line.replace('Message ID:', '').strip()
                elif line.startswith('Timestamp:'):
                    message_data['timestamp'] = line.replace('Timestamp:', '').strip()
                elif '--- MESSAGE CONTENT ---' in line:
                    capture_content = True
                    continue
                elif capture_content and line:
                    content_lines.append(line)
            
            if content_lines:
                message_data['content'] = '\n'.join(content_lines)
                messages.append(message_data)
        
        self.logger.info(f"Loaded {len(messages)} messages from {self.messages_file}")
        return messages
    
    def save_result(self, message_data, status, ignore_reason=None, parsed_data=None):
        """Save simulation result to database"""
        conn = sqlite3.connect(self.output_db)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO simulation_results 
            (message_text, channel_id, channel_name, message_id, timestamp, 
             status, ignore_reason, parsed_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            message_data.get('content'),
            message_data.get('channel_id'),
            message_data.get('channel_name'),
            message_data.get('message_id'),
            message_data.get('timestamp'),
            status,
            ignore_reason,
            json.dumps(parsed_data) if parsed_data else None
        ))
        
        conn.commit()
        conn.close()
    
    def simulate(self):
        """Run simulation on all messages"""
        
        print("\n" + "="*80)
        print("STARTING MESSAGE REPLAY SIMULATION")
        print("="*80)
        print(f"Input File: {self.messages_file}")
        print(f"Output DB: {self.output_db}")
        print(f"Parser: EnhancedSignalParser v2.0")
        print("="*80 + "\n")
        
        # Load messages
        messages = self.parse_message_file()
        self.stats['total_messages'] = len(messages)
        
        # Process each message
        for i, msg_data in enumerate(messages, 1):
            content = msg_data.get('content')
            channel_id = msg_data.get('channel_id')
            
            if not content:
                continue
            
            # Check if should ignore
            should_ignore, reason = self.parser.should_ignore(content, channel_id)
            
            if should_ignore:
                # Ignored
                self.stats['ignored'] += 1
                self.stats['ignore_reasons'][reason] = self.stats['ignore_reasons'].get(reason, 0) + 1
                
                self.save_result(msg_data, 'IGNORED', ignore_reason=reason)
                
                self.logger.info(f"[{i}/{len(messages)}] ✗ IGNORED: {reason}")
                self.logger.debug(f"    Message: {content[:60]}")
            
            else:
                # Try to parse
                parsed = self.parser.parse(content, channel_id)
                
                if parsed:
                    # Successfully parsed
                    self.stats['parsed'] += 1
                    
                    # Track special cases
                    if parsed.get('symbol_auto_detected'):
                        self.stats['auto_symbol_detected'] += 1
                    if parsed.get('expiry_auto_calculated'):
                        self.stats['auto_expiry_calculated'] += 1
                    if parsed.get('commodity'):
                        self.stats['commodities'] += 1
                    
                    self.save_result(msg_data, 'PARSED', parsed_data=parsed)
                    
                    self.logger.info(f"[{i}/{len(messages)}] ✓ PARSED: {parsed.get('symbol', 'N/A')} {parsed.get('strike', 'N/A')}")
                    if parsed.get('symbol_auto_detected'):
                        self.logger.info(f"    [AUTO-DETECT] Symbol: {parsed['symbol']}")
                    if parsed.get('expiry_auto_calculated'):
                        self.logger.info(f"    [AUTO-CALC] Expiry: {parsed['expiry_date']}")
                
                else:
                    # Failed to parse
                    self.stats['failed'] += 1
                    self.save_result(msg_data, 'FAILED')
                    
                    self.logger.warning(f"[{i}/{len(messages)}] ✗ FAILED: Could not parse")
                    self.logger.debug(f"    Message: {content[:60]}")
            
            # Progress update every 50 messages
            if i % 50 == 0:
                self.print_progress()
        
        # Final report
        self.print_final_report()
    
    def print_progress(self):
        """Print progress statistics"""
        total = self.stats['total_messages']
        processed = self.stats['ignored'] + self.stats['parsed'] + self.stats['failed']
        
        print(f"\nProgress: {processed}/{total} ({processed/total*100:.1f}%)")
        print(f"  Ignored: {self.stats['ignored']}")
        print(f"  Parsed: {self.stats['parsed']}")
        print(f"  Failed: {self.stats['failed']}")
    
    def print_final_report(self):
        """Print final simulation report"""
        
        print("\n" + "="*80)
        print("SIMULATION COMPLETE - FINAL REPORT")
        print("="*80)
        
        total = self.stats['total_messages']
        
        print(f"\nTOTAL MESSAGES: {total}")
        print("-"*80)
        print(f"✓ Parsed Successfully:  {self.stats['parsed']:4} ({self.stats['parsed']/total*100:5.1f}%)")
        print(f"✗ Ignored:              {self.stats['ignored']:4} ({self.stats['ignored']/total*100:5.1f}%)")
        print(f"✗ Failed to Parse:      {self.stats['failed']:4} ({self.stats['failed']/total*100:5.1f}%)")
        
        print(f"\nSPECIAL FEATURES USED:")
        print("-"*80)
        print(f"Auto Symbol Detection:  {self.stats['auto_symbol_detected']:4} signals")
        print(f"Auto Expiry Calculated: {self.stats['auto_expiry_calculated']:4} signals")
        print(f"Commodity Trades:       {self.stats['commodities']:4} signals")
        
        print(f"\nTOP IGNORE REASONS:")
        print("-"*80)
        sorted_reasons = sorted(self.stats['ignore_reasons'].items(), 
                               key=lambda x: x[1], reverse=True)
        for reason, count in sorted_reasons[:10]:
            print(f"  {count:4} - {reason}")
        
        print(f"\n" + "="*80)
        print("SIMULATION RESULTS SAVED TO:")
        print("="*80)
        print(f"  Database: {self.output_db}")
        print(f"  Log File: simulation.log")
        
        print(f"\n" + "="*80)
        print("VIEW RESULTS:")
        print("="*80)
        print(f"\n# All parsed signals:")
        print(f'sqlite3 {self.output_db} "SELECT * FROM simulation_results WHERE status=\'PARSED\' LIMIT 10;"')
        
        print(f"\n# Ignored messages:")
        print(f'sqlite3 {self.output_db} "SELECT ignore_reason, COUNT(*) FROM simulation_results WHERE status=\'IGNORED\' GROUP BY ignore_reason;"')
        
        print(f"\n# Auto-detected symbols:")
        print(f'sqlite3 {self.output_db} "SELECT * FROM simulation_results WHERE parsed_data LIKE \'%symbol_auto_detected%\';"')
        
        print("\n" + "="*80 + "\n")


def main():
    """Run the simulator"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python replay_messages_simulator.py <messages_file>")
        print("Example: python replay_messages_simulator.py telegram_messages/messages_2025-12-12.txt")
        sys.exit(1)
    
    messages_file = sys.argv[1]
    
    # Run simulation
    simulator = MessageReplaySimulator(messages_file)
    simulator.simulate()


if __name__ == "__main__":
    main()