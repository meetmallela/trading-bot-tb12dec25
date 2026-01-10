"""
update_instruments_cache.py - Update instruments cache from Kite
Downloads fresh instrument data and saves to CSV
"""

import json
import csv
from kiteconnect import KiteConnect
from datetime import datetime

def load_kite_config():
    """Load Kite credentials from config file"""
    try:
        with open('kite_config.json', 'r') as f:
            config = json.load(f)
        return config
    except Exception as e:
        print(f"Error loading config: {e}")
        return None

def update_cache():
    """Update instruments cache"""
    
    print("="*80)
    print("UPDATING INSTRUMENTS CACHE")
    print("="*80)
    
    # Load config
    config = load_kite_config()
    if not config:
        print("❌ Failed to load kite_config.json")
        return
    
    # Initialize Kite
    kite = KiteConnect(api_key=config['api_key'])
    kite.set_access_token(config['access_token'])
    
    print(f"\n✓ Connected to Kite as: {config.get('user_name', 'User')}")
    
    # Fetch instruments from both exchanges
    all_instruments = []
    
    exchanges = ['NFO', 'MCX']
    
    for exchange in exchanges:
        print(f"\nFetching {exchange} instruments...")
        try:
            instruments = kite.instruments(exchange)
            print(f"  ✓ Found {len(instruments)} instruments")
            
            # Convert to our format
            for inst in instruments:
                all_instruments.append({
                    'symbol': inst.get('tradingsymbol', ''),
                    'strike': inst.get('strike', ''),
                    'option_type': inst.get('instrument_type', ''),
                    'expiry_date': inst.get('expiry', '').strftime('%d-%m-%Y') if inst.get('expiry') else '',
                    'lot_size': inst.get('lot_size', ''),
                    'exchange': inst.get('exchange', '')
                })
        except Exception as e:
            print(f"  ❌ Error fetching {exchange}: {e}")
    
    # Save to CSV
    output_file = 'instruments_cache.csv'
    
    print(f"\nSaving {len(all_instruments)} instruments to {output_file}...")
    
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['symbol', 'strike', 'option_type', 'expiry_date', 'lot_size', 'exchange']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            writer.writeheader()
            writer.writerows(all_instruments)
        
        print(f"✓ Successfully saved to {output_file}")
        
    except Exception as e:
        print(f"❌ Error saving cache: {e}")
        return
    
    print("\n" + "="*80)
    print("CACHE UPDATE COMPLETE")
    print("="*80)
    
    # Show some stats
    print(f"\nTotal Instruments: {len(all_instruments)}")
    
    # Count by exchange
    nfo_count = sum(1 for i in all_instruments if i['exchange'] == 'NFO')
    mcx_count = sum(1 for i in all_instruments if i['exchange'] == 'MCX')
    
    print(f"  NFO: {nfo_count}")
    print(f"  MCX: {mcx_count}")
    
    # Sample searches
    print("\n" + "="*80)
    print("SAMPLE SEARCHES")
    print("="*80)
    
    # Search for MARUTI
    maruti_options = [i for i in all_instruments if 'MARUTI' in i['symbol'] and i['option_type'] in ['CE', 'PE']]
    if maruti_options:
        print(f"\nMARUTI Options found: {len(maruti_options)}")
        print("Sample strikes:")
        strikes = sorted(set(i['strike'] for i in maruti_options if i['strike']))
        for strike in strikes[:10]:
            print(f"  {strike}")
    else:
        print("\n❌ No MARUTI options found")
    
    # Search for ZINC
    zinc_options = [i for i in all_instruments if 'ZINC' in i['symbol'] and i['option_type'] in ['CE', 'PE']]
    if zinc_options:
        print(f"\nZINC Options found: {len(zinc_options)}")
        print("Sample strikes:")
        strikes = sorted(set(i['strike'] for i in zinc_options if i['strike']))
        for strike in strikes[:10]:
            print(f"  {strike}")
    else:
        print("\n❌ No ZINC options found")
    
    # Search for NIFTY
    nifty_options = [i for i in all_instruments if i['symbol'].startswith('NIFTY') and i['option_type'] in ['CE', 'PE'] and '26' in str(i.get('strike', ''))]
    if nifty_options:
        print(f"\nNIFTY 26XXX strikes found: {len(nifty_options)}")
        strikes = sorted(set(int(float(i['strike'])) for i in nifty_options if i['strike']))
        print(f"Strike range: {min(strikes)} to {max(strikes)}")
    
    print("\n✅ Cache update complete! Restart order_placer to use new cache.\n")


if __name__ == "__main__":
    update_cache()