"""
instrument_finder_df.py - FIXED VERSION
Returns expiry_date in YYYY-MM-DD format (not DD-MM-YYYY)
"""

import pandas as pd
from datetime import datetime
import logging

class InstrumentFinderDF:
    """Fast instrument finder using pandas DataFrame"""
    
    def __init__(self, csv_file='instruments_master.csv'):
        """Load instruments into DataFrame"""
        self.logger = logging.getLogger('INSTRUMENT_FINDER')
        
        try:
            self.df = pd.read_csv(csv_file)
            
            # ✅ CRITICAL FIX: Ensure expiry_date is in YYYY-MM-DD format
            if 'expiry_date' in self.df.columns:
                # Handle both DD-MM-YYYY and YYYY-MM-DD formats
                self.df['expiry_date'] = pd.to_datetime(
                    self.df['expiry_date'], 
                    format='mixed',  # Auto-detect format
                    dayfirst=True    # Assume DD-MM-YYYY if ambiguous
                ).dt.strftime('%Y-%m-%d')
            
            self.logger.info(f"[OK] Loaded {len(self.df)} instruments with fast indexing")
            
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to load instruments: {e}")
            self.df = pd.DataFrame()
    
    def find_instrument(self, symbol, strike, option_type, expiry_date=None):
        """
        Find instrument by symbol, strike, option_type
        
        Returns dict with expiry_date in YYYY-MM-DD format ✅
        """
        if self.df.empty:
            return None
        
        try:
            mask = (
                (self.df['symbol'] == symbol) &
                (self.df['strike'] == float(strike)) &
                (self.df['option_type'] == option_type)
            )
            
            if expiry_date:
                mask &= (self.df['expiry_date'] == expiry_date)
            
            matches = self.df[mask]
            
            if len(matches) == 0:
                return None
            
            if len(matches) > 1:
                matches = matches.sort_values('expiry_date')
            
            instrument = matches.iloc[0]
            
            # ✅ BUILD tradingsymbol since CSV doesn't have it
            # Format: NIFTY25DEC26050CE or GOLDM25DEC118000CE
            expiry_str = instrument['expiry_date']  # YYYY-MM-DD format
            expiry_dt = pd.to_datetime(expiry_str)
            
            year_code = expiry_dt.strftime('%y')    # 25
            month_code = expiry_dt.strftime('%b').upper()  # DEC
            
            tradingsymbol = f"{instrument['symbol']}{year_code}{month_code}{int(instrument['strike'])}{instrument['option_type']}"
            
            # ✅ Return date in YYYY-MM-DD format
            return {
                'symbol': tradingsymbol,  # Built from components
                'expiry_date': instrument['expiry_date'],  # Already YYYY-MM-DD ✅
                'exchange': instrument['exchange'],
                'lot_size': int(instrument['lot_size'])
            }
            
        except Exception as e:
            self.logger.error(f"[ERROR] Search failed: {e}")
            return None
    
    def get_default_quantity(self, symbol):
        """Get default lot size for symbol"""
        if self.df.empty:
            return 1
        
        try:
            match = self.df[self.df['symbol'] == symbol].iloc[0]
            return int(match['lot_size'])
        except:
            defaults = {
                'NIFTY': 25, 'BANKNIFTY': 15, 'FINNIFTY': 25, 'SENSEX': 10,
                'GOLD': 1, 'GOLDM': 1, 'SILVER': 1, 'SILVERM': 1,
                'NATURALGAS': 1250, 'CRUDEOIL': 100
            }
            return defaults.get(symbol, 1)
