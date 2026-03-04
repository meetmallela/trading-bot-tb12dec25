"""
tradingsymbol_lookup.py - CSV-based symbol lookup
Replaces symbol generation with direct CSV lookup

Process:
1. Load valid_instruments.csv into DataFrame
2. Filter by symbol
3. Filter by expiry_date (closest/next available)
4. Filter by strike
5. Filter by option_type
6. Return tradingsymbol from CSV
"""

import pandas as pd
import logging
from datetime import datetime, timedelta

logger = logging.getLogger('TRADINGSYMBOL_LOOKUP')

# Global DataFrame - loaded once at startup
_instruments_df = None


def load_instruments(csv_path='valid_instruments.csv'):
    """
    Load instruments CSV into memory
    
    Expected columns:
    - symbol: NIFTY, BANKNIFTY, SENSEX, etc.
    - tradingsymbol: NIFTY2621725800CE, etc.
    - strike: 25800.0
    - option_type: CE or PE
    - expiry_date: 2026-02-17
    - tick_size: 0.05
    - lot_size: 65
    - exchange: NFO, BFO, MCX
    - instrument_type: CE, PE, FUT
    """
    global _instruments_df
    
    try:
        _instruments_df = pd.read_csv(csv_path)
        
        # Verify required columns
        required_cols = ['symbol', 'tradingsymbol', 'strike', 'option_type', 'expiry_date']
        missing = [col for col in required_cols if col not in _instruments_df.columns]
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}")
        
        # Convert expiry_date to datetime
        _instruments_df['expiry_date'] = pd.to_datetime(_instruments_df['expiry_date'])
        
        # Convert strike to float for comparison
        _instruments_df['strike'] = _instruments_df['strike'].astype(float)
        
        logger.info(f"[LOADED] {len(_instruments_df)} instruments from {csv_path}")
        logger.info(f"[COLUMNS] {list(_instruments_df.columns)}")
        
        # Show sample
        sample_symbols = _instruments_df['symbol'].unique()[:5]
        logger.info(f"[SYMBOLS] Sample: {list(sample_symbols)}")
        
        return True
        
    except Exception as e:
        logger.error(f"[ERROR] Failed to load {csv_path}: {e}")
        raise


def lookup_tradingsymbol(symbol, strike, option_type, expiry_date=None):
    """
    Lookup tradingsymbol from CSV following exact steps:
    
    Step 1: Filter by symbol
    Step 2: Filter by expiry_date (closest/next if not specified)
    Step 3: Filter by strike
    Step 4: Filter by option_type
    Step 5: Return tradingsymbol
    
    Args:
        symbol: Base symbol (NIFTY, BANKNIFTY, SENSEX, etc.)
        strike: Strike price (25800, 25600, etc.)
        option_type: CE or PE
        expiry_date: Optional specific expiry (YYYY-MM-DD)
                    If None, picks next/closest available expiry
    
    Returns:
        dict with: {
            'tradingsymbol': str,
            'expiry_date': datetime,
            'lot_size': int,
            'exchange': str,
            'tick_size': float
        }
    """
    global _instruments_df
    
    if _instruments_df is None:
        load_instruments()
    
    try:
        # STEP 1: Filter by symbol
        df = _instruments_df[_instruments_df['symbol'] == symbol].copy()
        
        if df.empty:
            logger.error(f"[STEP 1 FAILED] Symbol '{symbol}' not found in CSV")
            return None
        
        logger.debug(f"[STEP 1] Filtered by symbol='{symbol}': {len(df)} rows")
        
        # STEP 2: Filter by expiry_date (closest/next available)
        if expiry_date:
            # Specific expiry requested
            target_expiry = pd.to_datetime(expiry_date)
            df_expiry = df[df['expiry_date'] == target_expiry]
            
            if df_expiry.empty:
                # Try to find closest expiry
                logger.warning(f"[STEP 2] Exact expiry {expiry_date} not found, finding closest")
                future_expiries = df[df['expiry_date'] >= target_expiry]
                if not future_expiries.empty:
                    closest_expiry = future_expiries['expiry_date'].min()
                    df = df[df['expiry_date'] == closest_expiry]
                    logger.info(f"[STEP 2] Using closest expiry: {closest_expiry.date()}")
                else:
                    logger.error(f"[STEP 2 FAILED] No expiries found >= {expiry_date}")
                    return None
            else:
                df = df_expiry
                logger.debug(f"[STEP 2] Filtered by expiry_date={expiry_date}: {len(df)} rows")
        else:
            # Find next/closest expiry
            today = pd.Timestamp.now().normalize()
            future_expiries = df[df['expiry_date'] >= today]
            
            if future_expiries.empty:
                logger.error(f"[STEP 2 FAILED] No future expiries for {symbol}")
                return None
            
            # Pick earliest (closest) expiry
            closest_expiry = future_expiries['expiry_date'].min()
            df = df[df['expiry_date'] == closest_expiry]
            logger.debug(f"[STEP 2] Using next expiry: {closest_expiry.date()} ({len(df)} rows)")
        
        # STEP 3: Filter by strike
        strike_float = float(strike)
        df = df[df['strike'] == strike_float]
        
        if df.empty:
            logger.error(f"[STEP 3 FAILED] Strike {strike} not found")
            return None
        
        logger.debug(f"[STEP 3] Filtered by strike={strike}: {len(df)} rows")
        
        # STEP 4: Filter by option_type
        df = df[df['option_type'] == option_type]
        
        if df.empty:
            logger.error(f"[STEP 4 FAILED] Option type '{option_type}' not found")
            return None
        
        logger.debug(f"[STEP 4] Filtered by option_type='{option_type}': {len(df)} rows")
        
        # STEP 5: Extract tradingsymbol and related data
        if len(df) > 1:
            logger.warning(f"[MULTIPLE MATCHES] Found {len(df)} instruments, using first")
        
        row = df.iloc[0]
        
        result = {
            'tradingsymbol': row['tradingsymbol'],
            'expiry_date': row['expiry_date'],
            'lot_size': int(row['lot_size']) if 'lot_size' in row else None,
            'exchange': row['exchange'] if 'exchange' in row else None,
            'tick_size': float(row['tick_size']) if 'tick_size' in row else None
        }
        
        logger.info(f"[✓ LOOKUP SUCCESS] {symbol} {strike} {option_type} → {result['tradingsymbol']}")
        logger.info(f"   Expiry: {result['expiry_date'].date()} | Lot: {result['lot_size']} | Exchange: {result['exchange']}")
        
        return result
        
    except Exception as e:
        logger.error(f"[LOOKUP FAILED] {symbol} {strike} {option_type}: {e}", exc_info=True)
        return None


def get_available_expiries(symbol):
    """Get all available expiry dates for a symbol"""
    global _instruments_df
    
    if _instruments_df is None:
        load_instruments()
    
    try:
        df_symbol = _instruments_df[_instruments_df['symbol'] == symbol]
        if df_symbol.empty:
            return []
        
        expiries = sorted(df_symbol['expiry_date'].unique())
        return [exp.date() for exp in expiries]
        
    except Exception as e:
        logger.error(f"[ERROR] Failed to get expiries for {symbol}: {e}")
        return []


def get_lot_size(symbol, strike=None, option_type=None, expiry_date=None):
    """Quick lookup for lot size"""
    result = lookup_tradingsymbol(symbol, strike, option_type, expiry_date)
    return result['lot_size'] if result else None


# Auto-load on import
try:
    load_instruments()
    logger.info("[READY] Tradingsymbol lookup initialized")
except Exception as e:
    logger.warning(f"[INIT WARN] Could not preload instruments: {e}")


if __name__ == "__main__":
    # Test the lookup
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("\n" + "="*60)
    print("TRADINGSYMBOL LOOKUP TEST")
    print("="*60)
    
    # Test 1: NIFTY with specific expiry
    print("\nTest 1: NIFTY 25800 CE for Feb 17")
    result = lookup_tradingsymbol('NIFTY', 25800, 'CE', '2026-02-17')
    if result:
        print(f"✓ Tradingsymbol: {result['tradingsymbol']}")
        print(f"  Lot Size: {result['lot_size']}")
        print(f"  Exchange: {result['exchange']}")
    else:
        print("✗ Lookup failed")
    
    # Test 2: NIFTY without specific expiry (next available)
    print("\nTest 2: NIFTY 25600 PE (next expiry)")
    result = lookup_tradingsymbol('NIFTY', 25600, 'PE')
    if result:
        print(f"✓ Tradingsymbol: {result['tradingsymbol']}")
        print(f"  Expiry: {result['expiry_date'].date()}")
    else:
        print("✗ Lookup failed")
    
    # Test 3: Get available expiries
    print("\nTest 3: Available NIFTY expiries")
    expiries = get_available_expiries('NIFTY')
    for exp in expiries[:5]:
        print(f"  - {exp}")
