"""
historical_trainer.py
Train Claude agent on previous month's messages
Builds initial KB from historical data
"""

import json
import sqlite3
import logging
from datetime import datetime
from premium_channel_agent_v2 import PremiumChannelAgentV2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('HISTORICAL_TRAINER')

class HistoricalTrainer:
    """Train agent on historical messages"""
    
    def __init__(self, agent, signals_db='premium_signals.db', kb_db='agent_kb.db'):
        self.agent = agent
        self.signals_db = signals_db
        self.kb_db = kb_db
    
    def get_historical_messages(self, days_back=30, limit=100):
        """Get historical messages from database"""
        conn = sqlite3.connect(self.signals_db)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get messages from last N days
        cursor.execute("""
            SELECT * FROM signals 
            WHERE datetime(timestamp) >= datetime('now', '-' || ? || ' days')
            ORDER BY timestamp ASC
            LIMIT ?
        """, (days_back, limit))
        
        messages = []
        for row in cursor.fetchall():
            messages.append(dict(row))
        
        conn.close()
        return messages
    
    def parse_with_message_date(self, message_text, message_date):
        """Parse message using its original timestamp for expiry calculation"""
        
        # Skip obvious non-trading messages
        skip_patterns = [
            'renewed', 'whatsapp', 'group', 'join', 'link',
            'subscribe', 'payment', 'discount', 'offer',
            'december month', 'january', 'february'
        ]
        
        message_lower = message_text.lower()
        if any(pattern in message_lower for pattern in skip_patterns):
            if len(message_text) < 100:  # Skip short promotional messages
                logger.info(f"[SKIP] Non-trading message: {message_text[:50]}...")
                return None
        
        logger.info(f"\n{'='*70}")
        logger.info(f"[HISTORICAL] Message from: {message_date[:10]}")
        logger.info(f"[RAW] {message_text[:80]}...")
        
        # Parse normally
        signal = self.agent.parse_signal(
            message=message_text,
            channel_id="historical",
            channel_name="Historical Training"
        )
        
        return signal
    
    def train_on_historical(self, days_back=30, review_mode=True):
        """Train on historical messages with human review"""
        logger.info("="*70)
        logger.info("HISTORICAL TRAINING")
        logger.info("="*70)
        logger.info(f"Loading messages from last {days_back} days...")
        
        messages = self.get_historical_messages(days_back=days_back, limit=100)
        
        if not messages:
            logger.info("No historical messages found!")
            return
        
        logger.info(f"Found {len(messages)} historical messages")
        logger.info("="*70)
        
        successful = 0
        failed = 0
        corrections_needed = 0
        skipped = 0
        
        for i, msg in enumerate(messages, 1):
            logger.info(f"\n[{i}/{len(messages)}] Processing...")
            
            # Parse with message's original date
            signal = self.parse_with_message_date(
                msg['raw_text'],
                msg['timestamp']
            )
            
            if signal is None:
                skipped += 1
                continue
            
            if signal:
                successful += 1
                logger.info(f"[SUCCESS] {signal['symbol']} {signal['strike']} {signal['option_type']}")
                logger.info(f"   Expiry: {signal['expiry_date']}")
                
                if review_mode:
                    # Show to human for review
                    print("\n" + "="*70)
                    print(f"MESSAGE {i}/{len(messages)}")
                    print("="*70)
                    print(f"Date: {msg['timestamp']}")
                    print(f"Raw: {msg['raw_text']}")
                    print(f"\nParsed:")
                    print(json.dumps(signal, indent=2))
                    print("\nIs this CORRECT? (y/n/skip/quit)")
                    
                    response = input("> ").strip().lower()
                    
                    if response == 'q' or response == 'quit':
                        logger.info("Training stopped by user")
                        break
                    
                    if response == 'skip' or response == 's':
                        continue
                    
                    if response == 'n':
                        corrections_needed += 1
                        print("\nProvide corrected JSON (paste and press Enter twice):")
                        json_lines = []
                        while True:
                            line = input()
                            if line.strip() == '' and json_lines:
                                break
                            json_lines.append(line)
                        
                        try:
                            corrected = json.loads('\n'.join(json_lines))
                            
                            # Save correction to KB
                            self.agent.save_correction(
                                raw_message=msg['raw_text'],
                                claude_parsed=signal,
                                human_corrected=corrected,
                                channel_id=msg['channel_id'],
                                notes=f"Historical training correction (msg date: {msg['timestamp']})"
                            )
                            
                            logger.info("[KB] Correction saved!")
                            
                        except json.JSONDecodeError as e:
                            logger.error(f"Invalid JSON: {e}")
                    
                    elif response == 'y':
                        logger.info("[OK] Confirmed correct")
            else:
                failed += 1
                logger.info(f"[FAILED] Could not parse")
                
                if review_mode:
                    print("\n" + "="*70)
                    print(f"FAILED MESSAGE {i}/{len(messages)}")
                    print("="*70)
                    print(f"Raw: {msg['raw_text']}")
                    print("\nProvide correct parsing? (y/n/skip)")
                    
                    response = input("> ").strip().lower()
                    
                    if response == 'y':
                        print("\nProvide correct JSON:")
                        json_lines = []
                        while True:
                            line = input()
                            if line.strip() == '' and json_lines:
                                break
                            json_lines.append(line)
                        
                        try:
                            corrected = json.loads('\n'.join(json_lines))
                            
                            self.agent.save_correction(
                                raw_message=msg['raw_text'],
                                claude_parsed=None,
                                human_corrected=corrected,
                                channel_id=msg['channel_id'],
                                notes=f"Historical training - manual entry (msg date: {msg['timestamp']})"
                            )
                            
                            logger.info("[KB] Manual correction saved!")
                            
                        except json.JSONDecodeError as e:
                            logger.error(f"Invalid JSON: {e}")
        
        # Summary
        print("\n" + "="*70)
        print("TRAINING SUMMARY")
        print("="*70)
        print(f"Total messages: {len(messages)}")
        print(f"Skipped (non-trading): {skipped}")
        print(f"Successfully parsed: {successful}")
        print(f"Failed to parse: {failed}")
        print(f"Corrections made: {corrections_needed}")
        if (len(messages) - skipped) > 0:
            print(f"Success rate: {(successful/(len(messages)-skipped)*100):.1f}%")
        print("="*70)
        
        # Show KB stats
        kb_stats = self.agent.get_statistics()
        print("\nKnowledge Base:")
        print(f"  Corrections stored: {kb_stats['corrections_kb']}")
        print("\nClaude will use these examples in future parsing!")
        print("="*70)
    
    def batch_train_without_review(self, days_back=30):
        """Quick training without human review (for already-validated data)"""
        logger.info("BATCH TRAINING (No Review)")
        
        messages = self.get_historical_messages(days_back=days_back, limit=500)
        
        successful = 0
        failed = 0
        
        for msg in messages:
            signal = self.parse_with_message_date(msg['raw_text'], msg['timestamp'])
            
            if signal:
                successful += 1
            else:
                failed += 1
        
        logger.info(f"\nProcessed {len(messages)} messages")
        logger.info(f"Success: {successful}, Failed: {failed}")
        logger.info(f"Success rate: {(successful/len(messages)*100):.1f}%")


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Train agent on historical messages')
    parser.add_argument('--days', type=int, default=30, help='Days of history to process')
    parser.add_argument('--no-review', action='store_true', help='Skip human review')
    parser.add_argument('--api-key', required=True, help='Claude API key')
    args = parser.parse_args()
    
    # Initialize agent
    agent = PremiumChannelAgentV2(
        claude_api_key=args.api_key,
        instruments_csv='../valid_instruments.csv'
    )
    
    # Initialize trainer
    trainer = HistoricalTrainer(agent)
    
    # Train
    if args.no_review:
        trainer.batch_train_without_review(days_back=args.days)
    else:
        trainer.train_on_historical(days_back=args.days, review_mode=True)


if __name__ == "__main__":
    main()
