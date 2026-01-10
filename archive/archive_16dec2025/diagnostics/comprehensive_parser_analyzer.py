"""
comprehensive_parser_analyzer.py - Analyze all messages and create detailed CSV report
Shows what's parsed, what's ignored, what failed - for manual review
"""

import re
import json
import csv
import os
from datetime import datetime
from signal_parser_enhanced_v2 import EnhancedSignalParser
import logging

class ComprehensiveAnalyzer:
    """Analyze messages and create detailed CSV report"""
    
    def __init__(self, messages_dir='telegram_messages'):
        self.messages_dir = messages_dir
        self.parser = EnhancedSignalParser(
            rules_file='parsing_rules_enhanced_v2.json',
            instruments_cache='instruments_cache.csv'
        )
        self.setup_logging()
        self.all_results = []
        
    def setup_logging(self):
        """Setup logging"""
        logging.basicConfig(
            level=logging.WARNING,  # Reduce log noise
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('ANALYZER')
    
    def parse_message_file(self, filepath):
        """Parse a telegram messages text file"""
        messages = []
        
        print(f"\nLoading: {filepath}")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Split by separator
        message_blocks = content.split('='*80)
        
        for block in message_blocks:
            if 'MESSAGE RECEIVED' not in block and 'MESSAGE CONTENT' not in block:
                continue
            
            # Extract message details
            message_data = {
                'file': os.path.basename(filepath),
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
        
        print(f"  Found {len(messages)} messages")
        return messages
    
    def analyze_message(self, msg_data):
        """Analyze a single message and return detailed result"""
        content = msg_data.get('content', '')
        channel_id = msg_data.get('channel_id')
        
        result = {
            'file': msg_data.get('file'),
            'channel_name': msg_data.get('channel_name'),
            'channel_id': msg_data.get('channel_id'),
            'message_id': msg_data.get('message_id'),
            'timestamp': msg_data.get('timestamp'),
            'message_text': content[:200],  # First 200 chars
            'message_length': len(content),
            'status': None,
            'ignore_reason': None,
            'parsed_action': None,
            'parsed_symbol': None,
            'parsed_strike': None,
            'parsed_option_type': None,
            'parsed_entry_price': None,
            'parsed_stop_loss': None,
            'parsed_targets': None,
            'is_commodity': None,
            'auto_symbol_detected': None,
            'auto_expiry_calculated': None,
            'full_parsed_data': None
        }
        
        # Check if should ignore
        should_ignore, reason = self.parser.should_ignore(content, channel_id)
        
        if should_ignore:
            result['status'] = 'IGNORED'
            result['ignore_reason'] = reason
            return result
        
        # Try to parse
        parsed = self.parser.parse(content, channel_id)
        
        if parsed:
            result['status'] = 'PARSED'
            result['parsed_action'] = parsed.get('action')
            result['parsed_symbol'] = parsed.get('symbol')
            result['parsed_strike'] = parsed.get('strike')
            result['parsed_option_type'] = parsed.get('option_type')
            result['parsed_entry_price'] = parsed.get('entry_price')
            result['parsed_stop_loss'] = parsed.get('stop_loss')
            result['parsed_targets'] = str(parsed.get('targets', []))
            result['is_commodity'] = 'YES' if parsed.get('commodity') else 'NO'
            result['auto_symbol_detected'] = 'YES' if parsed.get('symbol_auto_detected') else 'NO'
            result['auto_expiry_calculated'] = 'YES' if parsed.get('expiry_auto_calculated') else 'NO'
            result['full_parsed_data'] = json.dumps(parsed)
        else:
            result['status'] = 'FAILED'
            result['ignore_reason'] = 'Could not parse - complex format or missing data'
        
        return result
    
    def analyze_all_files(self):
        """Analyze all message files in the directory"""
        
        print("\n" + "="*80)
        print("COMPREHENSIVE PARSER ANALYSIS")
        print("="*80)
        
        # Get all .txt files
        files = []
        for filename in os.listdir(self.messages_dir):
            if filename.endswith('.txt'):
                filepath = os.path.join(self.messages_dir, filename)
                files.append(filepath)
        
        files.sort()
        
        if not files:
            print(f"No .txt files found in {self.messages_dir}")
            return
        
        print(f"\nFound {len(files)} message files:")
        for f in files:
            print(f"  - {os.path.basename(f)}")
        
        # Process each file
        for filepath in files:
            messages = self.parse_message_file(filepath)
            
            print(f"  Analyzing {len(messages)} messages...")
            
            for msg_data in messages:
                result = self.analyze_message(msg_data)
                self.all_results.append(result)
        
        print(f"\n✓ Analyzed {len(self.all_results)} total messages")
    
    def generate_statistics(self):
        """Generate statistics from results"""
        
        stats = {
            'total': len(self.all_results),
            'parsed': 0,
            'ignored': 0,
            'failed': 0,
            'nifty': 0,
            'banknifty': 0,
            'sensex': 0,
            'gold': 0,
            'silver': 0,
            'other': 0,
            'commodities': 0,
            'auto_symbol': 0,
            'auto_expiry': 0
        }
        
        for result in self.all_results:
            if result['status'] == 'PARSED':
                stats['parsed'] += 1
                
                symbol = result['parsed_symbol']
                if symbol == 'NIFTY':
                    stats['nifty'] += 1
                elif symbol == 'BANKNIFTY':
                    stats['banknifty'] += 1
                elif symbol == 'SENSEX':
                    stats['sensex'] += 1
                elif symbol == 'GOLD':
                    stats['gold'] += 1
                elif symbol == 'SILVER':
                    stats['silver'] += 1
                elif symbol:
                    stats['other'] += 1
                
                if result['is_commodity'] == 'YES':
                    stats['commodities'] += 1
                
                if result['auto_symbol_detected'] == 'YES':
                    stats['auto_symbol'] += 1
                
                if result['auto_expiry_calculated'] == 'YES':
                    stats['auto_expiry'] += 1
            
            elif result['status'] == 'IGNORED':
                stats['ignored'] += 1
            else:
                stats['failed'] += 1
        
        return stats
    
    def export_to_csv(self, output_file='telegram_messages_analysis.csv'):
        """Export results to CSV"""
        
        print(f"\n{'='*80}")
        print("EXPORTING TO CSV")
        print("="*80)
        
        fieldnames = [
            'file',
            'channel_name',
            'channel_id',
            'message_id',
            'timestamp',
            'status',
            'ignore_reason',
            'message_length',
            'message_text',
            'parsed_symbol',
            'parsed_strike',
            'parsed_option_type',
            'parsed_action',
            'parsed_entry_price',
            'parsed_stop_loss',
            'parsed_targets',
            'is_commodity',
            'auto_symbol_detected',
            'auto_expiry_calculated',
            'full_parsed_data'
        ]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.all_results)
        
        print(f"\n✓ Exported {len(self.all_results)} records to: {output_file}")
    
    def print_summary(self):
        """Print summary statistics"""
        
        stats = self.generate_statistics()
        
        print("\n" + "="*80)
        print("ANALYSIS SUMMARY")
        print("="*80)
        
        print(f"\nTOTAL MESSAGES: {stats['total']}")
        print("-"*80)
        print(f"✓ Parsed Successfully:  {stats['parsed']:4} ({stats['parsed']/stats['total']*100:5.1f}%)")
        print(f"✗ Ignored (Filtered):   {stats['ignored']:4} ({stats['ignored']/stats['total']*100:5.1f}%)")
        print(f"✗ Failed to Parse:      {stats['failed']:4} ({stats['failed']/stats['total']*100:5.1f}%)")
        
        print(f"\nPARSED BY SYMBOL:")
        print("-"*80)
        print(f"NIFTY:       {stats['nifty']:4} signals")
        print(f"BANKNIFTY:   {stats['banknifty']:4} signals")
        print(f"SENSEX:      {stats['sensex']:4} signals")
        print(f"GOLD:        {stats['gold']:4} signals")
        print(f"SILVER:      {stats['silver']:4} signals")
        print(f"OTHER:       {stats['other']:4} signals")
        
        print(f"\nSPECIAL FEATURES:")
        print("-"*80)
        print(f"Commodities:         {stats['commodities']:4}")
        print(f"Auto Symbol Detect:  {stats['auto_symbol']:4}")
        print(f"Auto Expiry Calc:    {stats['auto_expiry']:4}")
        
        print("\n" + "="*80)
        print("NEXT STEPS")
        print("="*80)
        print("1. Open: telegram_messages_analysis.csv")
        print("2. Filter by 'status' column:")
        print("   - PARSED: See what's working")
        print("   - FAILED: See what needs fixing")
        print("   - IGNORED: See what's being filtered")
        print("3. Sort by 'parsed_symbol' to see NIFTY/BANKNIFTY/SENSEX")
        print("4. Review 'message_text' for failed messages")
        print("5. Check 'ignore_reason' for incorrectly ignored messages")
        print("="*80 + "\n")


def main():
    """Run the comprehensive analysis"""
    
    analyzer = ComprehensiveAnalyzer(messages_dir='telegram_messages')
    
    # Analyze all files
    analyzer.analyze_all_files()
    
    # Print summary
    analyzer.print_summary()
    
    # Export to CSV
    analyzer.export_to_csv('telegram_messages_analysis.csv')
    
    print("\n✅ ANALYSIS COMPLETE")
    print("\nOpen telegram_messages_analysis.csv in Excel to review results!\n")


if __name__ == "__main__":
    main()
