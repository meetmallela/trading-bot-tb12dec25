"""
find_instrument_from_cache.py - Improved instrument finder
Reads instruments_cache.csv to find exact matches
"""

import csv
import logging
from datetime import datetime

class InstrumentFinder:
    """Find instruments from cache CSV file"""
    
    def __init__(self, cache_file='instruments_cache.csv'):
        self.cache_file = cache_file
        self.instruments = []
        self.logger = logging.getLogger('INSTRUMENT_FINDER')
        self.load_cache()
    
    def load_cache(self):
        """Load instruments from CSV cache"""
        try:
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                self.instruments = list(reader)
            
            self.logger.info(f"[OK] Loaded {len(self.instruments)} instruments from cache")
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to load cache: {e}")
            self.instruments = []
    
    def find_instrument(self, symbol, strike, option_type, expiry_date=None):
        """
        Find instrument matching criteria
        
        Args:
            symbol: NIFTY, BANKNIFTY, GOLD, etc.
            strike: Strike price (e.g., 26050)
            option_type: CE or PE
            expiry_date: Optional expiry in YYYY-MM-DD format
            
        Returns:
            dict with instrument details or None
        """
        
        self.logger.info(f"[SEARCH] Looking for: {symbol} {strike} {option_type} exp={expiry_date}")
        
        # Convert inputs to strings for comparison
        strike_str = str(int(strike)) if strike else None
        option_type_upper = option_type.upper() if option_type else None
        
        # Search through cache
        matches = []
        
        for inst in self.instruments:
            # Match symbol
            if symbol.upper() not in inst.get('symbol', '').upper():
                continue
            
            # Match strike
            if strike_str and inst.get('strike') != strike_str:
                continue
            
            # Match option type
            if option_type_upper and inst.get('option_type') != option_type_upper:
                continue
            
            # Match expiry if provided
            if expiry_date:
                inst_expiry = inst.get('expiry_date', '')
                # Compare dates (handle different formats)
                if expiry_date not in inst_expiry and inst_expiry not in expiry_date:
                    continue
            
            matches.append(inst)
        
        if not matches:
            self.logger.warning(f"[NOT FOUND] No instruments match: {symbol} {strike} {option_type}")
            return None
        
        # If multiple matches, prefer the one with nearest expiry
        if len(matches) > 1:
            self.logger.info(f"[MULTIPLE] Found {len(matches)} matches, selecting first")
            # Sort by expiry date
            matches.sort(key=lambda x: x.get('expiry_date', ''))
        
        result = matches[0]
        self.logger.info(f"[FOUND] {result.get('symbol')} | Strike={result.get('strike')} | Expiry={result.get('expiry_date')}")
        
        return result
    
    def find_with_auto_expiry(self, symbol, strike, option_type):
        """
        Find instrument and auto-select nearest expiry
        """
        
        # Get all matching instruments (any expiry)
        matches = []
        strike_str = str(int(strike))
        
        for inst in self.instruments:
            if (symbol.upper() in inst.get('symbol', '').upper() and
                inst.get('strike') == strike_str and
                inst.get('option_type') == option_type.upper()):
                matches.append(inst)
        
        if not matches:
            return None
        
        # Sort by expiry date, get nearest
        today = datetime.now().strftime('%Y-%m-%d')
        future_expiries = [m for m in matches if m.get('expiry_date', '') >= today]
        
        if future_expiries:
            future_expiries.sort(key=lambda x: x.get('expiry_date', ''))
            return future_expiries[0]
        
        return matches[0]


# Test function
def test_finder():
    """Test the instrument finder"""
    
    logging.basicConfig(level=logging.INFO)
    
    finder = InstrumentFinder('instruments_cache.csv')
    
    # Test cases
    tests = [
        ("NIFTY", 26050, "PE", "2025-12-16"),
        ("NIFTY", 26050, "CE", None),
        ("BANKNIFTY", 59800, "CE", None),
        ("GOLD", 137000, "CE", None),
    ]
    
    print("\n" + "="*80)
    print("TESTING INSTRUMENT FINDER")
    print("="*80)
    
    for symbol, strike, opt_type, expiry in tests:
        print(f"\nTest: {symbol} {strike} {opt_type} exp={expiry}")
        
        if expiry:
            result = finder.find_instrument(symbol, strike, opt_type, expiry)
        else:
            result = finder.find_with_auto_expiry(symbol, strike, opt_type)
        
        if result:
            print(f"  ✓ Found: {result.get('symbol')}")
            print(f"    Strike: {result.get('strike')}")
            print(f"    Expiry: {result.get('expiry_date')}")
            print(f"    Exchange: {result.get('exchange')}")
        else:
            print(f"  ✗ NOT FOUND")
        
        print("-"*80)


if __name__ == "__main__":
    test_finder()
