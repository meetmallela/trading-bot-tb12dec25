"""
FUTURES SUPPORT FOR ORDER PLACER

Add this to order_placer_db_production.py
"""

# ========================================
# ADD THIS FUNCTION TO YOUR ORDER PLACER
# ========================================

def place_futures_order(self, signal_data):
    """
    Place futures order
    
    Signal data for futures:
    - symbol: GOLD, SILVER, etc.
    - action: BUY/SELL
    - entry_price: Entry price
    - stop_loss: SL price
    - quantity: Lot size
    - tradingsymbol: GOLD25FEBFUT
    - exchange: MCX
    """
    try:
        symbol = signal_data['symbol']
        action = signal_data['action']
        entry_price = signal_data['entry_price']
        quantity = signal_data['quantity']
        tradingsymbol = signal_data.get('tradingsymbol', f"{symbol}FUT")
        exchange = signal_data.get('exchange', 'MCX')
        
        logging.info(f"[FUTURES] Placing {action} order for {tradingsymbol}")
        logging.info(f"  Quantity: {quantity}")
        logging.info(f"  Entry: ₹{entry_price}")
        
        # Place market order for futures
        order_params = {
            'tradingsymbol': tradingsymbol,
            'exchange': exchange,
            'transaction_type': action,  # BUY or SELL
            'quantity': quantity,
            'order_type': 'MARKET',  # Market order
            'product': 'MIS',  # Intraday
            'validity': 'DAY'
        }
        
        logging.info(f"[SEND] Placing FUTURES order: {order_params}")
        
        # Place order
        order_id = self.kite.place_order(
            variety='regular',
            **order_params
        )
        
        logging.info(f"[OK] Futures order placed! Order ID: {order_id}")
        
        return {
            'order_id': order_id,
            'status': 'PLACED',
            'tradingsymbol': tradingsymbol,
            'quantity': quantity,
            'entry_price': entry_price
        }
        
    except Exception as e:
        logging.error(f"[ERROR] Failed to place futures order: {e}")
        return None


# ========================================
# MODIFY process_signal() METHOD
# ========================================

def process_signal(self, signal_record):
    """
    Process signal - handles both OPTIONS and FUTURES
    """
    try:
        signal_id = signal_record[0]
        parsed_json = signal_record[5]
        
        # Parse JSON
        signal_data = json.loads(parsed_json)
        
        # Check instrument type
        instrument_type = signal_data.get('instrument_type', 'OPTIONS')
        
        if instrument_type == 'FUTURES':
            # ===== FUTURES ORDER =====
            logging.info(f"\n{'='*80}")
            logging.info(f"[FUTURES TASK] Processing Signal #{signal_id}")
            logging.info(f"{'='*80}")
            
            symbol = signal_data.get('symbol')
            action = signal_data.get('action')
            entry_price = signal_data.get('entry_price')
            stop_loss = signal_data.get('stop_loss')
            
            logging.info(f"[FUTURES] {symbol} | Action: {action} | Entry: {entry_price} | SL: {stop_loss}")
            
            # Place futures order
            order_result = self.place_futures_order(signal_data)
            
            if order_result:
                # Save to database
                self.save_order_to_db(
                    signal_id=signal_id,
                    order_id=order_result['order_id'],
                    order_type='ENTRY',
                    status='PLACED',
                    tradingsymbol=order_result['tradingsymbol'],
                    quantity=order_result['quantity'],
                    price=order_result['entry_price'],
                    stop_loss=stop_loss
                )
                
                # Mark signal as processed
                self.mark_signal_processed(signal_id)
                logging.info(f"[OK] Futures order saved to database")
            else:
                logging.error(f"[FAILED] Could not place futures order")
        
        else:
            # ===== OPTIONS ORDER (existing logic) =====
            logging.info(f"\n{'='*80}")
            logging.info(f"[OPTIONS TASK] Processing Signal #{signal_id}")
            logging.info(f"{'='*80}")
            
            # ... your existing options processing code ...
            
    except Exception as e:
        logging.error(f"[ERROR] Failed to process signal: {e}")


# ========================================
# USAGE INSTRUCTIONS
# ========================================

print("""
HOW TO ADD FUTURES SUPPORT TO ORDER PLACER:

1. Open: order_placer_db_production.py

2. Add the place_futures_order() method to your OrderPlacer class

3. Modify your process_signal() method to check instrument_type:
   
   if instrument_type == 'FUTURES':
       order_result = self.place_futures_order(signal_data)
   else:
       # existing options logic
       
4. That's it! Your order placer will now handle both OPTIONS and FUTURES

5. Restart: python order_placer_db_production.py --continuous
""")
