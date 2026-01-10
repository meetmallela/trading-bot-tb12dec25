"""
Instrument Downloader - FIXED VERSION
Downloads complete instrument list from Kite API and caches locally
"""

import pandas as pd
from kiteconnect import KiteConnect
from datetime import datetime, time
import os
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class InstrumentDownloader:
    """Downloads and caches instruments from Kite API"""
    
    def __init__(self, kite: KiteConnect, cache_file: str):
        self.kite = kite
        self.cache_file = cache_file
        self.instruments_df = None
        
    def download_instruments(self) -> pd.DataFrame:
        """Download complete instrument list from Kite"""
        try:
            logger.info("Downloading instruments from Kite API...")
            
            # Download instruments from all exchanges
            instruments = self.kite.instruments()
            
            # Convert to DataFrame
            df = pd.DataFrame(instruments)
            
            logger.info(f"Downloaded {len(df)} total instruments")
            logger.info(f"Columns available: {df.columns.tolist()}")
            
            # Filter for options and futures only (no equity)
            if 'instrument_type' in df.columns:
                df = df[df['instrument_type'].isin(['CE', 'PE', 'FUT'])]
            else:
                logger.warning("'instrument_type' column not found, keeping all instruments")
            
            # Prepare the dataframe with correct column mapping
            result_df = pd.DataFrame()
            
            # Map columns correctly based on what Kite API actually returns
            result_df['symbol'] = df['tradingsymbol'] if 'tradingsymbol' in df.columns else df['instrument_token']
            result_df['underlying'] = df['name'] if 'name' in df.columns else df['tradingsymbol']
            result_df['strike'] = df['strike'] if 'strike' in df.columns else 0
            result_df['option_type'] = df['instrument_type'] if 'instrument_type' in df.columns else ''
            result_df['expiry_date'] = df['expiry'] if 'expiry' in df.columns else None
            result_df['tick_size'] = df['tick_size'] if 'tick_size' in df.columns else 0.05
            result_df['lot_size'] = df['lot_size'] if 'lot_size' in df.columns else 1
            result_df['exchange'] = df['exchange'] if 'exchange' in df.columns else ''
            
            # Add instrument_type classification
            result_df['instrument_type'] = df.apply(self._classify_instrument, axis=1)
            
            # Remove rows with missing critical data
            result_df = result_df.dropna(subset=['symbol', 'underlying'])
            
            # Save to cache
            self._save_cache(result_df)
            
            logger.info(f"Processed {len(result_df)} instruments successfully")
            return result_df
            
        except Exception as e:
            logger.error(f"Error downloading instruments: {e}")
            logger.error(f"Error details: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
    
    def _classify_instrument(self, row) -> str:
        """Classify instrument type"""
        try:
            # Check if segment column exists
            if 'segment' not in row or pd.isna(row['segment']):
                # Fallback classification based on exchange
                exchange = row.get('exchange', '')
                instrument_type = row.get('instrument_type', '')
                
                if exchange == 'NFO':
                    if instrument_type in ['CE', 'PE']:
                        return 'INDEX_OPTION' if row.get('underlying', '') in ['NIFTY', 'BANKNIFTY', 'FINNIFTY'] else 'STOCK_OPTION'
                    elif instrument_type == 'FUT':
                        return 'INDEX_FUTURE' if row.get('underlying', '') in ['NIFTY', 'BANKNIFTY', 'FINNIFTY'] else 'STOCK_FUTURE'
                elif exchange == 'BFO':
                    return 'INDEX_OPTION'
                elif exchange == 'MCX':
                    return 'COMMODITY'
                elif exchange in ['CDS', 'BCD']:
                    return 'CURRENCY'
                else:
                    return 'OTHER'
            
            segment = row['segment']
            underlying = row.get('name', row.get('underlying', ''))
            
            if segment == 'NFO-OPT':
                return 'INDEX_OPTION' if underlying in ['NIFTY', 'BANKNIFTY', 'FINNIFTY'] else 'STOCK_OPTION'
            elif segment == 'BFO-OPT':
                return 'INDEX_OPTION'  # SENSEX
            elif segment == 'NFO-FUT':
                return 'INDEX_FUTURE' if underlying in ['NIFTY', 'BANKNIFTY', 'FINNIFTY'] else 'STOCK_FUTURE'
            elif segment == 'MCX-OPT':
                return 'COMMODITY_OPTION'
            elif segment == 'MCX-FUT':
                return 'COMMODITY_FUTURE'
            elif segment in ['CDS-OPT', 'CDS-FUT', 'BCD-OPT', 'BCD-FUT']:
                return 'CURRENCY'
            else:
                return 'OTHER'
                
        except Exception as e:
            logger.debug(f"Error classifying instrument: {e}")
            return 'OTHER'
    
    def _save_cache(self, df: pd.DataFrame):
        """Save instruments to cache file"""
        try:
            # Create directory if doesn't exist
            Path(self.cache_file).parent.mkdir(parents=True, exist_ok=True)
            
            # Save with timestamp
            df.to_csv(self.cache_file, index=False)
            
            # Also save with timestamp for backup
            timestamp = datetime.now().strftime("%Y%m%d")
            backup_file = self.cache_file.replace('.csv', f'_{timestamp}.csv')
            df.to_csv(backup_file, index=False)
            
            logger.info(f"Instruments cached to {self.cache_file}")
            
        except Exception as e:
            logger.error(f"Error saving cache: {e}")
            raise
    
    def load_from_cache(self) -> pd.DataFrame:
        """Load instruments from cache file"""
        try:
            if not os.path.exists(self.cache_file):
                logger.warning("Cache file not found, downloading fresh instruments")
                return self.download_instruments()
            
            # Check if cache is from today
            file_time = datetime.fromtimestamp(os.path.getmtime(self.cache_file))
            if file_time.date() < datetime.now().date():
                logger.info("Cache is stale, downloading fresh instruments")
                return self.download_instruments()
            
            logger.info(f"Loading instruments from cache: {self.cache_file}")
            df = pd.read_csv(self.cache_file)
            
            # Convert expiry_date to datetime
            if 'expiry_date' in df.columns:
                df['expiry_date'] = pd.to_datetime(df['expiry_date'], errors='coerce')
            
            self.instruments_df = df
            logger.info(f"Loaded {len(df)} instruments from cache")
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading cache: {e}")
            # Try to download fresh
            return self.download_instruments()
    
    def get_instruments(self, force_download: bool = False) -> pd.DataFrame:
        """Get instruments (from cache or download)"""
        if force_download or self.instruments_df is None:
            return self.download_instruments()
        return self.instruments_df
    
    def find_instrument(self, underlying: str, strike: float, option_type: str, 
                       expiry: datetime = None) -> dict:
        """
        Find specific instrument
        
        Args:
            underlying: Instrument name (NIFTY, SENSEX, INDIGO, etc)
            strike: Strike price
            option_type: CE or PE
            expiry: Expiry date (optional)
        
        Returns:
            Dictionary with instrument details or None
        """
        if self.instruments_df is None or len(self.instruments_df) == 0:
            logger.warning("No instruments loaded")
            return None
        
        try:
            # Filter by underlying and strike
            df = self.instruments_df[
                (self.instruments_df['underlying'].str.upper() == underlying.upper()) &
                (self.instruments_df['strike'] == float(strike)) &
                (self.instruments_df['option_type'].str.upper() == option_type.upper())
            ]
            
            # If expiry specified, filter by expiry
            if expiry:
                df = df[df['expiry_date'] == pd.Timestamp(expiry)]
            else:
                # Get nearest expiry
                today = datetime.now().date()
                df = df[df['expiry_date'] >= pd.Timestamp(today)]
                if len(df) > 0:
                    df = df.sort_values('expiry_date').head(1)
            
            if len(df) == 0:
                logger.warning(f"Instrument not found: {underlying} {strike} {option_type}")
                return None
            
            return df.iloc[0].to_dict()
            
        except Exception as e:
            logger.error(f"Error finding instrument: {e}")
            return None
    
    def should_download(self, download_time_str: str = "08:45") -> bool:
        """Check if it's time to download instruments"""
        now = datetime.now().time()
        download_time = time(*map(int, download_time_str.split(':')))
        
        # Check if cache exists and is from today
        if os.path.exists(self.cache_file):
            file_time = datetime.fromtimestamp(os.path.getmtime(self.cache_file))
            if file_time.date() == datetime.now().date():
                return False  # Already downloaded today
        
        # Download if current time is past download time
        return now >= download_time


# Singleton instance
_downloader_instance = None


def get_instrument_downloader(kite: KiteConnect = None, cache_file: str = None) -> InstrumentDownloader:
    """Get singleton instance of instrument downloader"""
    global _downloader_instance
    
    if _downloader_instance is None and kite is not None:
        _downloader_instance = InstrumentDownloader(kite, cache_file)
    
    return _downloader_instance
