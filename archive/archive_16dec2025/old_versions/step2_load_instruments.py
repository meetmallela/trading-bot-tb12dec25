"""
STEP 2: Load and Cache Instruments
Run this after step1_kite_login.py
"""

from kiteconnect import KiteConnect
import pandas as pd
import os

def load_kite_credentials():
    """Load credentials from kite_token.txt"""
    if not os.path.exists("kite_token.txt"):
        print("ERROR: kite_token.txt not found!")
        print("Please run step1_kite_login.py first")
        return None, None
    
    with open("kite_token.txt", "r") as f:
        lines = f.readlines()
        access_token = None
        api_key = None
        
        for line in lines:
            if line.startswith("ACCESS_TOKEN="):
                access_token = line.split("=")[1].strip()
            elif line.startswith("API_KEY="):
                api_key = line.split("=")[1].strip()
    
    return api_key, access_token

def download_instruments():
    """Download instruments from Kite"""
    
    print("\n" + "="*80)
    print("INSTRUMENT LOADER")
    print("="*80)
    
    # Load credentials
    api_key, access_token = load_kite_credentials()
    
    if not api_key or not access_token:
        return False
    
    print(f"\nUsing API Key: {api_key[:10]}...")
    print(f"Using Access Token: {access_token[:20]}...")
    
    # Initialize Kite
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    
    print("\nDownloading instruments from Kite...")
    
    try:
        # Download all instruments
        instruments = kite.instruments()
        print(f"Downloaded {len(instruments)} total instruments")
        
        # Convert to DataFrame
        df = pd.DataFrame(instruments)
        
        # Filter for options and futures
        if 'instrument_type' in df.columns:
            df_filtered = df[df['instrument_type'].isin(['CE', 'PE', 'FUT'])].copy()
        else:
            df_filtered = df.copy()
        
        print(f"Filtered to {len(df_filtered)} options/futures")
        
        # Prepare simplified format
        result_df = pd.DataFrame()
        result_df['symbol'] = df_filtered['tradingsymbol']
        result_df['strike'] = df_filtered['strike'] if 'strike' in df_filtered.columns else 0
        result_df['option_type'] = df_filtered['instrument_type'] if 'instrument_type' in df_filtered.columns else ''
        result_df['expiry_date'] = df_filtered['expiry'] if 'expiry' in df_filtered.columns else None
        result_df['lot_size'] = df_filtered['lot_size'] if 'lot_size' in df_filtered.columns else 1
        result_df['exchange'] = df_filtered['exchange'] if 'exchange' in df_filtered.columns else ''
        
        # Remove empty symbols
        result_df = result_df[result_df['symbol'] != '']
        
        # Save to CSV
        os.makedirs("config", exist_ok=True)
        result_df.to_csv("config/instruments_cache.csv", index=False)
        
        print(f"\n✅ SUCCESS! Saved {len(result_df)} instruments to config/instruments_cache.csv")
        print("\nYou can now run step3_test_parsers.py")
        print("="*80)
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        print(traceback.format_exc())
        return False

if __name__ == "__main__":
    download_instruments()
