"""
analyze_telegram_session.py - Analyze telegram reader session statistics
Shows messages received, parsed, sent to Claude API, and costs
"""

import sqlite3
import json
from datetime import datetime
import os

def analyze_database():
    """Analyze signals stored in database"""
    try:
        conn = sqlite3.connect('trading.db')
        cursor = conn.cursor()
        
        print("\n" + "="*80)
        print("DATABASE ANALYSIS - STORED SIGNALS")
        print("="*80)
        
        # Total signals stored
        cursor.execute("SELECT COUNT(*) FROM signals")
        total_signals = cursor.fetchone()[0]
        
        # Processed vs unprocessed
        cursor.execute("SELECT COUNT(*) FROM signals WHERE processed = 1")
        processed = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM signals WHERE processed = 0")
        unprocessed = cursor.fetchone()[0]
        
        # Today's signals
        cursor.execute("SELECT COUNT(*) FROM signals WHERE DATE(timestamp) = DATE('now')")
        today_signals = cursor.fetchone()[0]
        
        # Signals by channel
        cursor.execute("""
            SELECT channel_id, COUNT(*) as count 
            FROM signals 
            GROUP BY channel_id 
            ORDER BY count DESC
        """)
        by_channel = cursor.fetchall()
        
        # Recent signals
        cursor.execute("""
            SELECT id, parsed_data, timestamp 
            FROM signals 
            ORDER BY timestamp DESC 
            LIMIT 5
        """)
        recent = cursor.fetchall()
        
        print(f"\nTotal Signals Stored:     {total_signals}")
        print(f"  - Processed:            {processed}")
        print(f"  - Pending:              {unprocessed}")
        print(f"  - Today:                {today_signals}")
        
        print(f"\nSignals by Channel:")
        for channel_id, count in by_channel[:5]:
            print(f"  {channel_id}: {count} signals")
        
        print(f"\nRecent 5 Signals:")
        for signal_id, parsed_data, timestamp in recent:
            try:
                data = json.loads(parsed_data)
                symbol = f"{data.get('symbol')} {data.get('strike')} {data.get('option_type')}"
                action = data.get('action')
                print(f"  #{signal_id} - {timestamp} - {action} {symbol}")
            except:
                print(f"  #{signal_id} - {timestamp} - [parse error]")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Database error: {e}")


def analyze_log_file():
    """Analyze telegram_reader.log for detailed statistics"""
    
    log_file = 'telegram_reader.log'
    
    if not os.path.exists(log_file):
        print(f"\n⚠️  Log file not found: {log_file}")
        return
    
    print("\n" + "="*80)
    print("LOG FILE ANALYSIS - PARSER STATISTICS")
    print("="*80)
    
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Count different events
        stats = {
            'total_messages': 0,
            'regex_success': 0,
            'claude_api_calls': 0,
            'claude_success': 0,
            'filtered_before_api': 0,
            'parsing_failed': 0,
            'stored_signals': 0,
            'duplicates': 0
        }
        
        for line in lines:
            if '[NEW] Message from:' in line:
                stats['total_messages'] += 1
            elif '[REGEX] Parsed successfully' in line:
                stats['regex_success'] += 1
            elif '[FALLBACK] Regex failed, trying Claude API' in line:
                stats['claude_api_calls'] += 1
            elif '[SKIP]' in line and 'Not worth parsing' in line:
                stats['filtered_before_api'] += 1
            elif '[CLAUDE] Parsed successfully' in line:
                stats['claude_success'] += 1
            elif '[FAILED] Could not parse' in line:
                stats['parsing_failed'] += 1
            elif '[✓ STORED]' in line or 'Signal ID:' in line:
                stats['stored_signals'] += 1
            elif '[DUPLICATE]' in line:
                stats['duplicates'] += 1
        
        print(f"\n📨 Total Messages Received:        {stats['total_messages']}")
        print(f"\n✅ PARSED SUCCESSFULLY:")
        print(f"  - Via Regex (fast, free):        {stats['regex_success']}")
        print(f"  - Via Claude API:                 {stats['claude_success']}")
        print(f"  - Total Parsed:                   {stats['regex_success'] + stats['claude_success']}")
        
        print(f"\n🔍 FILTERING:")
        print(f"  - Filtered before API:            {stats['filtered_before_api']}")
        print(f"  - Sent to Claude API:             {stats['claude_api_calls']}")
        print(f"  - Claude success rate:            {(stats['claude_success']/max(stats['claude_api_calls'],1)*100):.1f}%")
        
        print(f"\n❌ REJECTED:")
        print(f"  - Not parsed (no keywords):       {stats['parsing_failed']}")
        print(f"  - Filtered (junk):                {stats['filtered_before_api']}")
        
        print(f"\n💾 STORAGE:")
        print(f"  - Stored in database:             {stats['stored_signals']}")
        print(f"  - Duplicates (skipped):           {stats['duplicates']}")
        
        # Calculate percentages
        if stats['total_messages'] > 0:
            parse_rate = (stats['regex_success'] + stats['claude_success']) / stats['total_messages'] * 100
            regex_rate = stats['regex_success'] / max(stats['total_messages'], 1) * 100
            api_rate = stats['claude_api_calls'] / max(stats['total_messages'], 1) * 100
            
            print(f"\n📊 PERFORMANCE:")
            print(f"  - Overall parse rate:             {parse_rate:.1f}%")
            print(f"  - Regex handled:                  {regex_rate:.1f}%")
            print(f"  - API needed:                     {api_rate:.1f}%")
        
        # Estimate costs
        if stats['claude_api_calls'] > 0:
            cost_per_call = 0.0003  # Approximate USD
            total_cost_usd = stats['claude_api_calls'] * cost_per_call
            total_cost_inr = total_cost_usd * 85
            
            print(f"\n💰 CLAUDE API COSTS:")
            print(f"  - Total API calls:                {stats['claude_api_calls']}")
            print(f"  - Estimated cost:                 ${total_cost_usd:.4f} (~₹{total_cost_inr:.2f})")
            print(f"  - Cost per successful parse:      ${(total_cost_usd/max(stats['claude_success'],1)):.4f}")
        
        # Find session start time
        for line in lines:
            if 'TELEGRAM SIGNAL READER' in line and 'CLAUDE AI ENHANCED' in line:
                try:
                    timestamp = line.split(' - ')[0]
                    print(f"\n⏰ Session started:                {timestamp}")
                except:
                    pass
                break
        
    except Exception as e:
        print(f"❌ Error reading log file: {e}")


def analyze_message_file():
    """Analyze telegram_messages/*.txt for all received messages"""
    
    messages_dir = 'telegram_messages'
    
    if not os.path.exists(messages_dir):
        print(f"\n⚠️  Messages directory not found: {messages_dir}")
        return
    
    print("\n" + "="*80)
    print("MESSAGE FILES ANALYSIS - ALL RECEIVED MESSAGES")
    print("="*80)
    
    try:
        # Find today's message file
        today = datetime.now().strftime("%Y-%m-%d")
        message_file = os.path.join(messages_dir, f"messages_{today}.txt")
        
        if not os.path.exists(message_file):
            # Try to find any message file
            files = [f for f in os.listdir(messages_dir) if f.startswith('messages_')]
            if files:
                message_file = os.path.join(messages_dir, sorted(files)[-1])
            else:
                print(f"⚠️  No message files found")
                return
        
        with open(message_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Count messages by separators
        message_count = content.count('MESSAGE RECEIVED:')
        
        # Count by channel (approximate)
        channels = {}
        for line in content.split('\n'):
            if line.startswith('Channel Name:'):
                channel_name = line.split(':', 1)[1].strip()
                channels[channel_name] = channels.get(channel_name, 0) + 1
        
        print(f"\nMessage File: {os.path.basename(message_file)}")
        print(f"Total Messages Logged:            {message_count}")
        print(f"\nTop 10 Active Channels:")
        
        for channel, count in sorted(channels.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {channel}: {count} messages")
        
    except Exception as e:
        print(f"❌ Error reading message files: {e}")


def show_summary():
    """Show overall summary"""
    print("\n" + "="*80)
    print("SUMMARY & RECOMMENDATIONS")
    print("="*80)
    
    # Check if logs exist
    has_db = os.path.exists('trading.db')
    has_log = os.path.exists('telegram_reader.log')
    has_messages = os.path.exists('telegram_messages')
    
    print(f"\n✅ Files Found:")
    print(f"  - Database (trading.db):          {'Yes' if has_db else 'No'}")
    print(f"  - Log file:                       {'Yes' if has_log else 'No'}")
    print(f"  - Message files:                  {'Yes' if has_messages else 'No'}")
    
    if has_db and has_log:
        print(f"\n💡 Next Steps:")
        print(f"  1. Review parsed signals in database")
        print(f"  2. Check if Claude API filtering is working well")
        print(f"  3. Consider tuning regex patterns if API usage is high")
        print(f"  4. Ready to test order placement in test mode")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    print("\n" + "="*80)
    print("TELEGRAM READER SESSION ANALYSIS")
    print("="*80)
    print("Analyzing your 8-9 hour session...")
    
    # Run all analyses
    analyze_database()
    analyze_log_file()
    analyze_message_file()
    show_summary()
    
    print("\n✅ Analysis complete!\n")
