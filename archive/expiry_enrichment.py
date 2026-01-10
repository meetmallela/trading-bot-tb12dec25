"""
Expiry Date Enrichment for Options Signals
Adds intelligent expiry date defaulting for incomplete signals
"""

import datetime
import re

def get_next_thursday():
    """Get the next Thursday from today"""
    today = datetime.date.today()
    days_ahead = 3 - today.weekday()  # Thursday is 3 (Mon=0)
    if days_ahead <= 0:  # Thursday already passed this week
        days_ahead += 7
    next_thursday = today + datetime.timedelta(days_ahead)
    return next_thursday.strftime('%Y-%m-%d')

def get_last_thursday_of_month(year, month):
    """Get the last Thursday of a given month"""
    # Get last day of month
    if month == 12:
        last_day = datetime.date(year + 1, 1, 1) - datetime.timedelta(days=1)
    else:
        last_day = datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)
    
    # Find last Thursday
    offset = (last_day.weekday() - 3) % 7
    return (last_day - datetime.timedelta(days=offset)).strftime('%Y-%m-%d')

def get_current_month_expiry():
    """Get the monthly expiry (last Thursday) of current month"""
    today = datetime.date.today()
    return get_last_thursday_of_month(today.year, today.month)

def get_next_month_expiry():
    """Get the monthly expiry (last Thursday) of next month"""
    today = datetime.date.today()
    next_month = today.month + 1 if today.month < 12 else 1
    next_year = today.year if today.month < 12 else today.year + 1
    return get_last_thursday_of_month(next_year, next_month)

def enrich_expiry_date(parsed_data, raw_text=""):
    """
    Enrich parsed data with expiry date if missing
    
    Rules:
    1. If expiry already exists -> keep it
    2. If raw text has "monthly" or "month" -> use monthly expiry
    3. For NIFTY/BANKNIFTY -> default to next weekly Thursday
    4. For stocks (with spaces like "TATA STEEL") -> use monthly expiry
    5. For MCX -> use monthly expiry (MCX typically monthly)
    """
    
    # If expiry already exists, keep it
    if parsed_data.get('expiry_date'):
        return parsed_data
    
    symbol = parsed_data.get('symbol', '').upper()
    raw_lower = raw_text.lower()
    
    # Check for explicit monthly mention
    if 'month' in raw_lower or 'monthly' in raw_lower:
        parsed_data['expiry_date'] = get_current_month_expiry()
        parsed_data['expiry_type'] = 'MONTHLY'
        return parsed_data
    
    # Index options (NIFTY, BANKNIFTY) -> weekly expiry by default
    if symbol in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']:
        parsed_data['expiry_date'] = get_next_thursday()
        parsed_data['expiry_type'] = 'WEEKLY'
        return parsed_data
    
    # MCX commodities -> monthly expiry
    mcx_symbols = ['CRUDEOIL', 'GOLD', 'SILVER', 'NATURALGAS', 'COPPER', 
                   'ZINC', 'LEAD', 'NICKEL', 'ALUMINIUM']
    if symbol in mcx_symbols:
        parsed_data['expiry_date'] = get_current_month_expiry()
        parsed_data['expiry_type'] = 'MONTHLY'
        return parsed_data
    
    # Stock options (typically have spaces or are not indices) -> monthly
    # Examples: "TATA STEEL", "RELIANCE", "HDFC BANK"
    if ' ' in symbol or (symbol and symbol not in ['NIFTY', 'BANKNIFTY']):
        parsed_data['expiry_date'] = get_current_month_expiry()
        parsed_data['expiry_type'] = 'MONTHLY'
        return parsed_data
    
    # Default fallback -> next weekly Thursday
    parsed_data['expiry_date'] = get_next_thursday()
    parsed_data['expiry_type'] = 'WEEKLY_DEFAULT'
    
    return parsed_data


if __name__ == "__main__":
    # Test cases
    test_cases = [
        {
            'symbol': 'NIFTY',
            'strike': 26150,
            'option_type': 'PE',
            'action': 'BUY',
            'entry_price': 135,
            'stop_loss': 120
        },
        {
            'symbol': 'BANKNIFTY',
            'strike': 51000,
            'option_type': 'CE',
            'action': 'BUY',
            'entry_price': 250
        },
        {
            'symbol': 'CRUDEOIL',
            'strike': 5300,
            'option_type': 'PE',
            'action': 'BUY',
            'entry_price': 150
        }
    ]
    
    print("="*60)
    print("EXPIRY DATE ENRICHMENT TEST")
    print("="*60)
    print()
    
    for test in test_cases:
        enriched = enrich_expiry_date(test.copy())
        print(f"Symbol: {enriched['symbol']}")
        print(f"  → Expiry: {enriched.get('expiry_date')} ({enriched.get('expiry_type')})")
        print()
    
    print("="*60)
    print(f"Next Thursday: {get_next_thursday()}")
    print(f"Current Month Expiry: {get_current_month_expiry()}")
    print(f"Next Month Expiry: {get_next_month_expiry()}")
    print("="*60)
