"""
Add this to your signal parser to support FUTURES signals
"""

def parse_futures_signal(self, message):
    """
    Parse futures signals like:
    "NEW CALLL BUY GOLD FEB CMP 136830 136700 WITH SL"
    """
    result = {}
    
    # Action
    action_match = re.search(r'\b(BUY|SELL)\b', message, re.IGNORECASE)
    if action_match:
        result['action'] = action_match.group(1).upper()
    
    # Symbol - Check for commodity futures
    futures_symbols = ['GOLD', 'GOLDM', 'SILVER', 'SILVERM', 'CRUDEOIL', 
                      'NATURALGAS', 'COPPER', 'ZINC', 'LEAD', 'NICKEL']
    
    for symbol in futures_symbols:
        if symbol in message.upper():
            result['symbol'] = symbol
            result['instrument_type'] = 'FUTURES'  # Flag as futures
            break
    
    # Month code (FEB, MAR, APR, etc.)
    month_match = re.search(r'\b(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\b', 
                           message, re.IGNORECASE)
    if month_match:
        result['expiry_month'] = month_match.group(1).upper()
    
    # Entry price (CMP or direct price)
    entry_patterns = [
        r'CMP\s*(\d+)',
        r'(?:ABOVE|BUY|SELL)\s+.*?(\d{5,6})',  # 5-6 digit price
    ]
    for pattern in entry_patterns:
        match = re.search(pattern, message, re.IGNORECASE)
        if match:
            result['entry_price'] = float(match.group(1))
            break
    
    # Stop loss
    sl_match = re.search(r'(?:SL|STOPLOSS|STOP LOSS)\s*[-:]*\s*(\d+)', 
                        message, re.IGNORECASE)
    if sl_match:
        result['stop_loss'] = float(sl_match.group(1))
    
    return result if result else None


def parse(self, message, channel_id=None):
    """
    Modified parse function to handle both OPTIONS and FUTURES
    """
    # Check if should ignore
    if self._should_ignore(message):
        return None
    
    # Check if it's a futures signal
    is_futures = any(word in message.upper() for word in 
                     ['GOLD FEB', 'GOLD MAR', 'SILVER', 'CRUDE', 'NATURALGAS'])
    
    if is_futures:
        self.logger.info("[FUTURES] Detected futures signal")
        result = self.parse_futures_signal(message)
        
        if result:
            # Futures don't need strike/option_type
            result['strike'] = None
            result['option_type'] = None
            
            # Add missing fields
            result = self._enrich_futures_data(result)
            
            return result
    
    # Otherwise use normal options parsing
    result = self._extract_with_regex(message)
    
    # ... rest of your existing parse logic
    
    return result


def _enrich_futures_data(self, result):
    """
    Enrich futures signal with expiry date and quantity
    """
    # Convert month code to expiry date
    if result.get('expiry_month'):
        # Get current year
        year = datetime.now().year
        month_map = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4,
            'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8,
            'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
        }
        
        month_num = month_map.get(result['expiry_month'])
        if month_num:
            # Futures expire on last Thursday
            # Simplified: use last day of month
            import calendar
            last_day = calendar.monthrange(year, month_num)[1]
            result['expiry_date'] = f"{year}-{month_num:02d}-{last_day}"
            result['expiry_auto_added'] = True
    
    # Add default quantity for futures
    if not result.get('quantity'):
        # Lot sizes for MCX futures
        lot_sizes = {
            'GOLD': 100,
            'GOLDM': 10,
            'SILVER': 30000,
            'SILVERM': 5000,
            'CRUDEOIL': 100,
            'NATURALGAS': 1250,
            'COPPER': 1000,
            'ZINC': 5000,
            'LEAD': 5000,
            'NICKEL': 250
        }
        result['quantity'] = lot_sizes.get(result['symbol'], 1)
        result['quantity_auto_added'] = True
    
    return result