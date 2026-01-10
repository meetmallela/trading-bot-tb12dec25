"""
Instrument Downloader - FULLY FIXED VERSION
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
            
            if not instruments:
                logger.error("No instruments received from Kite API")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(instruments)
            
            logger.info(f"Downloaded {len(df)} total instruments")
            logger.info(f"Sample instrument: {instruments[0]}")
            
            # Check available columns
            logger.info(f"Available columns: {df.columns.tolist()}")
            
            # Filter for options and futures only
            if 'instrument_type' in df.columns:
                df_filtered = df[df['instrument_type'].isin(['CE', 'PE', 'FUT'])].copy()
                logger.info(f"Filtered to {len(df_filtered)} options/futures")
            else:
                logger.warning("'instrument_type' column not found, keeping all")
                df_filtered = df.copy()
            
            # Prepare result dataframe with safe column mapping
            result_df = pd.DataFrame()
            
            # Symbol (trading symbol)
            result_df['symbol'] = df_filtered['tradingsymbol'] if 'tradingsymbol' in df_filtered.columns else ''
            
            # Underlying (instrument name)
            if 'name' in df_filtered.columns:
                result_df['underlying'] = df_filtered['name']
            elif 'tradingsymbol' in df_filtered.columns:
                # Extract underlying from trading symbol
                result_df['underlying'] = df_filtered['tradingsymbol'].str.extract(r'^([A-Z]+)')[0]
            else:
                result_df['underlying'] = ''
            
            # Strike price
            result_df['strike'] = df_filtered['strike'] if 'strike' in df_filtered.columns else 0.0
            
            # Option type (CE/PE/FUT)
            result_df['option_type'] = df_filtered['instrument_type'] if 'instrument_type' in df_filtered.columns else ''
            
            # Expiry date
            if 'expiry' in df_filtered.columns:
                result_df['expiry_date'] = pd.to_datetime(df_filtered['expiry'], errors='coerce')
            else:
                result_df['expiry_date'] = None
            
            # Tick size
            result_df['tick_size'] = df_filtered['tick_size'] if 'tick_size' in df_filtered.columns else 0.05
            
            # Lot size
            result_df['lot_size'] = df_filtered['lot_size'] if 'lot_size' in df_filtered.columns else 1
            
            # Exchange
            result_df['exchange'] = df_filtered['exchange'] if 'exchange' in df_filtered.columns else ''
            
            # Classify instrument type
            result_df['instrument_type'] = 'OTHER'
            for idx, row in df_filtered.iterrows():
                result_df.at[idx, 'instrument_type'] = self._classify_instrument_safe(row)
            
            # Remove rows with empty symbols
            result_df = result_df[result_df['symbol'] != ''].copy()
            
            logger.info(f"Processed {len(result_df)} instruments successfully")
            
            if len(result_df) == 0:
                logger.error("No valid instruments after processing")
                return pd.DataFrame()
            
            # Save to cache
            self._save_cache(result_df)
            
            self.instruments_df = result_df
            return result_df
            
        except Exception as e:
            logger.error(f"Error downloading instruments: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return pd.DataFrame()
    
    def _classify_instrument_safe(self, row) -> str:
        """Safely classify instrument type"""
        try:
            exchange = str(row.get('exchange', ''))
            instrument_type = str(row.get('instrument_type', ''))
            underlying = str(row.get('name', row.get('tradingsymbol', ''))).upper()
            
            # Index names
            index_names = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'SENSEX', 'BANKEX']
            
            # NFO - National Futures & Options (NSE)
            if exchange == 'NFO':
                if instrument_type in ['CE', 'PE']:
                    # Check if underlying is an index
                    if any(idx in underlying for idx in index_names):
                        return 'INDEX_OPTION'
                    else:
                        return 'STOCK_OPTION'
                elif instrument_type == 'FUT':
                    if any(idx in underlying for idx in index_names):
                        return 'INDEX_FUTURE'
                    else:
                        return 'STOCK_FUTURE'
            
            # BFO - BSE Futures & Options
            elif exchange == 'BFO':
                return 'INDEX_OPTION'  # SENSEX options
            
            # MCX - Multi Commodity Exchange
            elif exchange == 'MCX':
                if instrument_type in ['CE', 'PE']:
                    return 'COMMODITY_OPTION'
                else:
                    return 'COMMODITY_FUTURE'
            
            # CDS/BCD - Currency Derivatives
            elif exchange in ['CDS', 'BCD']:
                return 'CURRENCY'
            
            return 'OTHER'
            
        except Exception as e:
            logger.debug(f"Error classifying: {e}")
            return 'OTHER'
    
    def _save_cache(self, df: pd.DataFrame):
        """Save instruments to cache file"""
        try:
            if len(df) == 0:
                logger.warning("Cannot save empty DataFrame to cache")
                return
            
            # Create directory if doesn't exist
            Path(self.cache_file).parent.mkdir(parents=True, exist_ok=True)
            
            # Save main cache
            df.to_csv(self.cache_file, index=False)
            
            # Save timestamped backup
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = self.cache_file.replace('.csv', f'_backup_{timestamp}.csv')
            df.to_csv(backup_file, index=False)
            
            logger.info(f"Instruments cached to {self.cache_file}")
            logger.info(f"Backup saved to {backup_file}")
            
        except Exception as e:
            logger.error(f"Error saving cache: {e}")
    
    def load_from_cache(self) -> pd.DataFrame:
        """Load instruments from cache file"""
        try:
            if not os.path.exists(self.cache_file):
                logger.warning("Cache file not found, downloading fresh instruments")
                return self.download_instruments()
            
            # Check if cache is from today
            file_time = datetime.fromtimestamp(os.path.getmtime(self.cache_file))
            cache_age_hours = (datetime.now() - file_time).total_seconds() / 3600
            
            if file_time.date() < datetime.now().date():
                logger.info(f"Cache is {cache_age_hours:.1f} hours old, downloading fresh")
                return self.download_instruments()
            
            logger.info(f"Loading instruments from cache: {self.cache_file}")
            df = pd.read_csv(self.cache_file)
            
            if len(df) == 0:
                logger.warning("Cache file is empty, downloading fresh")
                return self.download_instruments()
            
            # Convert expiry_date to datetime
            if 'expiry_date' in df.columns:
                df['expiry_date'] = pd.to_datetime(df['expiry_date'], errors='coerce')
            
            self.instruments_df = df
            logger.info(f"Loaded {len(df)} instruments from cache")
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading cache: {e}")
            logger.info("Attempting to download fresh instruments")
            return self.download_instruments()
    
    def get_instruments(self, force_download: bool = False) -> pd.DataFrame:
        """Get instruments (from cache or download)"""
        if force_download or self.instruments_df is None or len(self.instruments_df) == 0:
            result = self.download_instruments()
        else:
            result = self.instruments_df
        
        # Always return a DataFrame, even if empty
        if result is None:
            logger.error("get_instruments returning None, creating empty DataFrame")
            return pd.DataFrame()
        
        return result
    
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
            logger.warning("No instruments loaded, attempting to load")
            self.load_from_cache()
            
            if self.instruments_df is None or len(self.instruments_df) == 0:
                logger.error("Still no instruments available")
                return None
        
        try:
            # Normalize inputs
            underlying_upper = underlying.upper()
            option_type_upper = option_type.upper()
            strike_float = float(strike)
            
            # Filter by underlying, strike, and option type
            mask = (
                (self.instruments_df['underlying'].str.upper() == underlying_upper) &
                (self.instruments_df['strike'] == strike_float) &
                (self.instruments_df['option_type'].str.upper() == option_type_upper)
            )
            
            df = self.instruments_df[mask].copy()
            
            if len(df) == 0:
                logger.warning(f"No instruments match: {underlying} {strike} {option_type}")
                return None
            
            # If expiry specified, filter by expiry
            if expiry:
                df = df[df['expiry_date'] == pd.Timestamp(expiry)]
            else:
                # Get nearest expiry
                today = pd.Timestamp(datetime.now().date())
                df = df[df['expiry_date'] >= today]
                if len(df) > 0:
                    df = df.sort_values('expiry_date').head(1)
            
            if len(df) == 0:
                logger.warning(f"No valid expiry found for: {underlying} {strike} {option_type}")
                return None
            
            result = df.iloc[0].to_dict()
            logger.info(f"Found instrument: {result.get('symbol')}")
            return result
            
        except Exception as e:
            logger.error(f"Error finding instrument: {e}")
            return None
    
    def should_download(self, download_time_str: str = "08:45") -> bool:
        """Check if it's time to download instruments"""
        try:
            now = datetime.now().time()
            download_time = time(*map(int, download_time_str.split(':')))
            
            # Check if cache exists and is from today
            if os.path.exists(self.cache_file):
                file_time = datetime.fromtimestamp(os.path.getmtime(self.cache_file))
                if file_time.date() == datetime.now().date():
                    return False  # Already downloaded today
            
            # Download if current time is past download time
            return now >= download_time
        except:
            return True  # Default to downloading if unsure


# Singleton instance
_downloader_instance = None


def get_instrument_downloader(kite: KiteConnect = None, cache_file: str = None) -> InstrumentDownloader:
    """Get singleton instance of instrument downloader"""
    global _downloader_instance
    
    if _downloader_instance is None and kite is not None:
        _downloader_instance = InstrumentDownloader(kite, cache_file)
    
    return _downloader_instance
