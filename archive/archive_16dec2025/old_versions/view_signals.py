"""
View Parsed Signals Log
Quick viewer for logs/parsed_signals.csv
"""

import pandas as pd
from pathlib import Path

def view_signals():
    """View parsed signals from CSV"""
    
    csv_file = "logs/parsed_signals.csv"
    
    if not Path(csv_file).exists():
        print(f"❌ No log file found: {csv_file}")
        print("Run the trading system first to generate logs")
        return
    
    # Read CSV
    df = pd.read_csv(csv_file)
    
    print("\n" + "="*100)
    print("PARSED SIGNALS LOG")
    print("="*100)
    
    print(f"\nTotal signals: {len(df)}")
    print(f"Successful: {len(df[df['parse_status'] == 'SUCCESS'])}")
    print(f"Failed: {len(df[df['parse_status'] == 'FAILED'])}")
    
    # Show by channel
    print("\n" + "-"*100)
    print("BY CHANNEL:")
    print("-"*100)
    for channel in df['channel'].unique():
        channel_df = df[df['channel'] == channel]
        success = len(channel_df[channel_df['parse_status'] == 'SUCCESS'])
        total = len(channel_df)
        print(f"{channel:15} | Total: {total:3} | Success: {success:3} | Failed: {total-success:3} | Rate: {100*success/total:.0f}%")
    
    # Show recent signals
    print("\n" + "-"*100)
    print("RECENT SIGNALS (Last 10):")
    print("-"*100)
    
    recent = df.tail(10)
    
    for idx, row in recent.iterrows():
        print(f"\n[{row['timestamp']}] {row['channel']}")
        if row['parse_status'] == 'SUCCESS':
            print(f"  ✅ {row['underlying']} {row['strike']} {row['option_type']} @ ₹{row['entry_price']}")
            if row['stop_loss']:
                print(f"     SL: ₹{row['stop_loss']}")
            if row['targets']:
                print(f"     Targets: {row['targets']}")
        else:
            print(f"  ❌ FAILED TO PARSE")
            print(f"     Message: {row['raw_message'][:100]}...")
    
    print("\n" + "="*100)
    print(f"Full log available at: {csv_file}")
    print("Open in Excel/Google Sheets for detailed analysis")
    print("="*100 + "\n")

if __name__ == "__main__":
    view_signals()
