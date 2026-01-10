import re
import csv
import argparse
from pathlib import Path
from datetime import datetime

def parse_telegram_messages(file_path):
    """
    Parse Telegram message file and extract structured data.
    
    Returns:
        List of dictionaries containing message data
    """
    messages = []
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Split by message blocks (identified by the separator lines)
    message_blocks = re.split(r'\n={80}\n\n', content)
    
    for block in message_blocks:
        if not block.strip():
            continue
            
        # Extract MESSAGE RECEIVED timestamp
        msg_received_match = re.search(r'MESSAGE RECEIVED: (.+)', block)
        if not msg_received_match:
            continue
            
        msg_received = msg_received_match.group(1).strip()
        
        # Extract Channel ID
        channel_id_match = re.search(r'Channel ID: (.+)', block)
        channel_id = channel_id_match.group(1).strip() if channel_id_match else ''
        
        # Extract Channel Name
        channel_name_match = re.search(r'Channel Name: (.+)', block)
        channel_name = channel_name_match.group(1).strip() if channel_name_match else ''
        
        # Extract Message ID
        msg_id_match = re.search(r'Message ID: (.+)', block)
        msg_id = msg_id_match.group(1).strip() if msg_id_match else ''
        
        # Extract Timestamp
        timestamp_match = re.search(r'Timestamp: (.+)', block)
        timestamp = timestamp_match.group(1).strip() if timestamp_match else ''
        
        # Extract MESSAGE CONTENT (everything after "--- MESSAGE CONTENT ---")
        content_match = re.search(r'--- MESSAGE CONTENT ---\n(.*?)(?:\n={80}|$)', block, re.DOTALL)
        message_content = content_match.group(1).strip() if content_match else ''
        
        messages.append({
            'MESSAGE RECEIVED': msg_received,
            'Channel ID': channel_id,
            'Channel Name': channel_name,
            'Message ID': msg_id,
            'Timestamp': timestamp,
            'MESSAGE CONTENT': message_content
        })
    
    return messages


def convert_to_csv(input_files, output_csv):
    """
    Convert multiple Telegram message files to a single CSV file.
    
    Args:
        input_files: List of input file paths
        output_csv: Output CSV file path
    """
    all_messages = []
    
    # Parse all input files
    for file_path in input_files:
        if not Path(file_path).exists():
            print(f"Warning: File not found: {file_path}")
            continue
            
        print(f"Parsing {file_path}...")
        messages = parse_telegram_messages(file_path)
        all_messages.extend(messages)
        print(f"  Found {len(messages)} messages")
    
    # Write to CSV
    if all_messages:
        fieldnames = ['MESSAGE RECEIVED', 'Channel ID', 'Channel Name', 
                     'Message ID', 'Timestamp', 'MESSAGE CONTENT']
        
        with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_messages)
        
        print(f"\n✓ Successfully created {output_csv}")
        print(f"✓ Total messages: {len(all_messages)}")
        
        # Show some statistics
        channels = set(msg['Channel Name'] for msg in all_messages)
        print(f"✓ Unique channels: {len(channels)}")
        
    else:
        print("✗ No messages found!")


def main():
    parser = argparse.ArgumentParser(
        description='Convert Telegram message text files to CSV format'
    )
    parser.add_argument(
        'input_files',
        nargs='+',
        help='Input text file(s) containing Telegram messages'
    )
    parser.add_argument(
        '-o', '--output',
        default=f'telegram_messages_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
        help='Output CSV file (default: telegram_messages_TIMESTAMP.csv)'
    )
    
    args = parser.parse_args()
    
    # Convert to CSV
    convert_to_csv(args.input_files, args.output)


if __name__ == "__main__":
    main()
