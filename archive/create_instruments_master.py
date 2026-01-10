"""
create_instruments_master.py - Merge and filter instruments for fast lookup

Creates instruments_master.csv with:
- Merged data from instruments_cache.csv and any other sources
- Only next 4 expiries per symbol
- Indexed by symbol, strike, option_type for fast lookup
"""

import pandas as pd
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def create_master_instruments():
    """Create master instruments file with next 4 expiries only"""
    
    logging.info("Loading instruments_cache.csv...")
    
    # Load the cache
    df = pd.read_csv('instruments_cache.csv')
    
    logging.info(f"Loaded {len(df)} instruments")
    
    # Convert expiry_date to datetime
    df['expiry_date_dt'] = pd.to_datetime(df['expiry_date'], format='%d-%m-%Y', errors='coerce')
    
    # Filter only future expiries
    today = datetime.now().date()
    df = df[df['expiry_date_dt'].dt.date >= today]
    
    logging.info(f"After filtering past expiries: {len(df)} instruments")
    
    # Extract base symbol (NIFTY, BANKNIFTY, GOLD, etc.)
    df['base_symbol'] = df['symbol'].str.extract(r'^([A-Z]+)', expand=False)
    
    # For each base symbol, keep only next 4 expiries
    filtered_dfs = []
    
    for base_symbol in df['base_symbol'].unique():
        symbol_df = df[df['base_symbol'] == base_symbol].copy()
        
        # Get unique expiries and sort
        unique_expiries = symbol_df['expiry_date_dt'].drop_duplicates().sort_values()
        
        # Keep only next 4 expiries
        next_4_expiries = unique_expiries.head(4)
        
        # Filter data for these expiries
        filtered = symbol_df[symbol_df['expiry_date_dt'].isin(next_4_expiries)]
        filtered_dfs.append(filtered)
        
        logging.info(f"{base_symbol}: Kept {len(filtered)} instruments across {len(next_4_expiries)} expiries")
    
    # Combine all filtered data
    master_df = pd.concat(filtered_dfs, ignore_index=True)
    
    # Sort by base_symbol, expiry, strike
    master_df = master_df.sort_values(['base_symbol', 'expiry_date_dt', 'strike'])
    
    # Save to CSV
    output_columns = ['symbol', 'strike', 'option_type', 'expiry_date', 'exchange', 'lot_size']
    master_df[output_columns].to_csv('instruments_master.csv', index=False)
    
    logging.info(f"✓ Created instruments_master.csv with {len(master_df)} instruments")
    
    # Print summary
    logging.info("\n" + "="*80)
    logging.info("SUMMARY")
    logging.info("="*80)
    
    for base_symbol in sorted(master_df['base_symbol'].unique()):
        count = len(master_df[master_df['base_symbol'] == base_symbol])
        expiries = master_df[master_df['base_symbol'] == base_symbol]['expiry_date'].unique()
        logging.info(f"{base_symbol:15s}: {count:5d} instruments | Expiries: {len(expiries)}")
    
    logging.info("="*80)
    
    return master_df

if __name__ == "__main__":
    create_master_instruments()
    logging.info("\n✓ Done! Use instruments_master.csv for fast lookups")
