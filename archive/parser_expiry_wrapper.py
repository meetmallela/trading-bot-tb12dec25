"""
Parser Wrapper with Expiry Enrichment
Wraps any existing parser and adds intelligent expiry date defaulting
"""

import datetime

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
    if month == 12:
        last_day = datetime.date(year + 1, 1, 1) - datetime.timedelta(days=1)
    else:
        last_day = datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)
    
    offset = (last_day.weekday() - 3) % 7
    return (last_day - datetime.timedelta(days=offset)).strftime('%Y-%m-%d')

def get_current_month_expiry():
    """Get the monthly expiry (last Thursday) of current month"""
    today = datetime.date.today()
    return get_last_thursday_of_month(today.year, today.month)

def enrich_expiry_date(parsed_data, raw_text=""):
    """
    Add expiry date to parsed data if missing
    
    Rules:
    - NIFTY/BANKNIFTY -> next weekly Thursday
    - MCX commodities -> monthly expiry
    - Stock options -> monthly expiry
    """
    
    if not parsed_data:
        return parsed_data
    
    # If expiry already exists, keep it
    if parsed_data.get('expiry_date'):
        return parsed_data
    
    symbol = parsed_data.get('symbol', '').upper()
    raw_lower = raw_text.lower()
    
    # Check for explicit monthly mention
    if 'month' in raw_lower or 'monthly' in raw_lower:
        parsed_data['expiry_date'] = get_current_month_expiry()
        return parsed_data
    
    # Index options (NSE + BSE) -> weekly expiry
    if symbol in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX']:
        parsed_data['expiry_date'] = get_next_thursday()
        return parsed_data
    
    # MCX commodities -> monthly expiry
    mcx_symbols = ['CRUDEOIL', 'GOLD', 'SILVER', 'NATURALGAS', 'COPPER']
    if symbol in mcx_symbols:
        parsed_data['expiry_date'] = get_current_month_expiry()
        return parsed_data
    
    # Default -> monthly expiry for stocks
    parsed_data['expiry_date'] = get_current_month_expiry()
    return parsed_data


class ParserWithExpiryEnrichment:
    """Wrapper that adds expiry enrichment to any parser"""
    
    def __init__(self, base_parser):
        """
        base_parser: The original parser object with a parse() method
        """
        self.base_parser = base_parser
    
    def parse(self, message_text, **kwargs):
        """Parse message and enrich with expiry if needed"""
        # Call the original parser
        parsed_data = self.base_parser.parse(message_text, **kwargs)
        
        # Enrich with expiry date
        if parsed_data:
            parsed_data = enrich_expiry_date(parsed_data, message_text)
        
        return parsed_data


# For direct import and use
def add_expiry_enrichment(parser_instance):
    """
    Wrap an existing parser instance with expiry enrichment
    
    Usage:
        from signal_parser_with_claude_fallback import SignalParserWithClaudeFallback
        from parser_expiry_wrapper import add_expiry_enrichment
        
        parser = SignalParserWithClaudeFallback(...)
        parser = add_expiry_enrichment(parser)  # Now has expiry enrichment
    """
    return ParserWithExpiryEnrichment(parser_instance)
