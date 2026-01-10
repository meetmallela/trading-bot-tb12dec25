"""
Additional Symbols Configuration
Add more indices and symbols that parsers should recognize
"""

# BSE Index Options
BSE_INDICES = [
    'SENSEX',      # BSE Sensex Index
    'BANKEX',      # BSE Bank Index
]

# NSE Index Options (complete list)
NSE_INDICES = [
    'NIFTY',       # Nifty 50
    'BANKNIFTY',   # Bank Nifty
    'FINNIFTY',    # Financial Nifty
    'MIDCPNIFTY',  # Midcap Nifty
]

# MCX Commodities
MCX_COMMODITIES = [
    'CRUDEOIL',    # Crude Oil
    'CRUDEOILM',   # Crude Oil Mini
    'GOLD',        # Gold
    'GOLDM',       # Gold Mini
    'GOLDPETAL',   # Gold Petal
    'SILVER',      # Silver
    'SILVERM',     # Silver Mini
    'SILVERMIC',   # Silver Micro
    'NATURALGAS',  # Natural Gas
    'COPPER',      # Copper
    'ZINC',        # Zinc
    'LEAD',        # Lead
    'NICKEL',      # Nickel
    'ALUMINIUM',   # Aluminium
]

# All recognized symbols
ALL_RECOGNIZED_SYMBOLS = NSE_INDICES + BSE_INDICES + MCX_COMMODITIES

# Exchange mapping
SYMBOL_TO_EXCHANGE = {
    # NSE
    'NIFTY': 'NFO',
    'BANKNIFTY': 'NFO',
    'FINNIFTY': 'NFO',
    'MIDCPNIFTY': 'NFO',
    
    # BSE
    'SENSEX': 'BFO',
    'BANKEX': 'BFO',
    
    # MCX
    'CRUDEOIL': 'MCX',
    'CRUDEOILM': 'MCX',
    'GOLD': 'MCX',
    'GOLDM': 'MCX',
    'GOLDPETAL': 'MCX',
    'SILVER': 'MCX',
    'SILVERM': 'MCX',
    'SILVERMIC': 'MCX',
    'NATURALGAS': 'MCX',
    'COPPER': 'MCX',
    'ZINC': 'MCX',
    'LEAD': 'MCX',
    'NICKEL': 'MCX',
    'ALUMINIUM': 'MCX',
}

# Default lot sizes (for quantity validation)
DEFAULT_LOT_SIZES = {
    'NIFTY': 25,
    'BANKNIFTY': 15,
    'FINNIFTY': 25,
    'MIDCPNIFTY': 50,
    'SENSEX': 10,
    'BANKEX': 15,
    'CRUDEOIL': 100,
    'GOLD': 100,
    'SILVER': 30,
}

def is_valid_symbol(symbol):
    """Check if symbol is recognized"""
    return symbol.upper() in ALL_RECOGNIZED_SYMBOLS

def get_exchange_for_symbol(symbol):
    """Get exchange for a given symbol"""
    return SYMBOL_TO_EXCHANGE.get(symbol.upper(), 'NFO')

def get_lot_size(symbol):
    """Get lot size for a symbol"""
    return DEFAULT_LOT_SIZES.get(symbol.upper(), 1)


if __name__ == "__main__":
    print("="*60)
    print("RECOGNIZED SYMBOLS")
    print("="*60)
    print()
    
    print("NSE Indices:", NSE_INDICES)
    print("BSE Indices:", BSE_INDICES)
    print("MCX Commodities:", MCX_COMMODITIES)
    print()
    
    print(f"Total: {len(ALL_RECOGNIZED_SYMBOLS)} symbols")
    print()
    
    # Test SENSEX
    print("="*60)
    print("TEST: SENSEX")
    print("="*60)
    print(f"Valid? {is_valid_symbol('SENSEX')}")
    print(f"Exchange: {get_exchange_for_symbol('SENSEX')}")
    print(f"Lot Size: {get_lot_size('SENSEX')}")
