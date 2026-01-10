"""
Check CRUDEOIL availability and generate SQL insert
"""

import pandas as pd
import json
from datetime import datetime

# Load instruments
df = pd.read_csv('valid_instruments.csv')

print("="*80)
print("CRUDEOIL INSTRUMENT CHECK")
print("="*80)

# Check for CRUDEOIL
crudeoil = df[df['symbol'] == 'CRUDEOIL']

if len(crudeoil) > 0:
    print(f"\n✅ Found {len(crudeoil)} CRUDEOIL instruments")
    
    # Check for 5150 strike
    strike_5150 = crudeoil[crudeoil['strike'] == 5150]
    
    if len(strike_5150) > 0:
        print(f"\n✅ Strike 5150 exists!")
        print("\nAvailable 5150 options:")
        print(strike_5150[['symbol', 'strike', 'option_type', 'expiry_date', 'tick_size', 'lot_size', 'exchange']])
        
        # Check for PE
        pe_5150 = strike_5150[strike_5150['option_type'] == 'PE']
        
        if len(pe_5150) > 0:
            print(f"\n✅ CRUDEOIL 5150 PE found!")
            
            # Get December expiry
            december = pe_5150[pe_5150['expiry_date'].str.contains('2025-12')]
            
            if len(december) > 0:
                print(f"\n✅ December expiry available!")
                instrument = december.iloc[0]
                
                print("\nInstrument details:")
                print(f"  Symbol: {instrument['symbol']}")
                print(f"  Strike: {instrument['strike']}")
                print(f"  Option Type: {instrument['option_type']}")
                print(f"  Expiry: {instrument['expiry_date']}")
                print(f"  Tick Size: {instrument['tick_size']}")
                print(f"  Lot Size: {instrument['lot_size']}")
                print(f"  Exchange: {instrument['exchange']}")
                
                # Generate SQL
                signal_data = {
                    "symbol": "CRUDEOIL",
                    "strike": 5150,
                    "option_type": "PE",
                    "action": "BUY",
                    "entry_price": 101.0,
                    "stop_loss": 95.0,
                    "targets": [110],
                    "expiry_date": instrument['expiry_date'],
                    "quantity": int(instrument['lot_size'])
                }
                
                channel_id = "-1002770917134"
                channel_name = "MANUAL_TRADE"
                message_id = 9999
                raw_text = "BUY CRUDEOIL 5150 PE above 101 SL 95 Target 110 December Expiry"
                timestamp = datetime.now().isoformat()
                
                sql = f"""INSERT INTO signals 
(channel_id, channel_name, message_id, raw_text, parsed_data, timestamp, processed)
VALUES (
    '{channel_id}',
    '{channel_name}',
    {message_id},
    '{raw_text}',
    '{json.dumps(signal_data)}',
    '{timestamp}',
    0
);"""
                
                print("\n" + "="*80)
                print("SQL INSERT STATEMENT")
                print("="*80)
                print(sql)
                
                print("\n" + "="*80)
                print("PARSED DATA (for verification)")
                print("="*80)
                print(json.dumps(signal_data, indent=2))
                
            else:
                print(f"\n❌ No December expiry found")
                print(f"Available expiries: {pe_5150['expiry_date'].unique()}")
        else:
            print(f"\n❌ No PE option for strike 5150")
    else:
        print(f"\n❌ Strike 5150 not found")
        print(f"\nAvailable strikes:")
        print(sorted(crudeoil['strike'].unique()))
else:
    print("\n❌ CRUDEOIL not found in CSV")

print("\n" + "="*80)
