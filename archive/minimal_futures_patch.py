"""
MINIMAL PATCH: Add FUTURES support to your existing order_placer_db_production.py
NO CONFIG CHANGES - uses your existing kite_config.json
"""

print("="*80)
print("SIMPLE PATCH FOR FUTURES SUPPORT")
print("="*80)

print("""
STEP 1: Add this method to OrderPlacerProduction class (around line 250)
========================================================================

    def place_futures_order(self, signal_data):
        '''Place FUTURES order on MCX'''
        try:
            symbol = signal_data['symbol']
            action = signal_data['action']
            quantity = signal_data['quantity']
            tradingsymbol = signal_data.get('tradingsymbol', f"{symbol}FUT")
            exchange = signal_data.get('exchange', 'MCX')
            
            logging.info(f"[FUTURES] {tradingsymbol} | {action} | Qty: {quantity}")
            
            if self.test_mode:
                logging.info("[TEST] Would place futures order")
                return {'order_id': f'TEST_{int(time.time())}', 'status': 'TEST'}
            
            # Place market order
            order_id = self.kite.place_order(
                variety='regular',
                tradingsymbol=tradingsymbol,
                exchange=exchange,
                transaction_type=action,
                quantity=quantity,
                order_type='MARKET',
                product='MIS',
                validity='DAY'
            )
            
            logging.info(f"[OK] Futures order placed: {order_id}")
            return {'order_id': order_id, 'status': 'PLACED'}
            
        except Exception as e:
            logging.error(f"[ERROR] Futures order failed: {e}")
            return None


STEP 2: Modify process_pending_signals() (around line 330)
===========================================================

Find this section in process_pending_signals():

    # Extract signal data
    sig_data = json.loads(sig['parsed_data'])
    
ADD THIS RIGHT AFTER:
    
    # Check instrument type
    instrument_type = sig_data.get('instrument_type', 'OPTIONS')
    
    if instrument_type == 'FUTURES':
        # Handle futures
        logging.info(f"[FUTURES SIGNAL] {sig_data.get('symbol')}")
        
        order_result = self.place_futures_order(sig_data)
        
        if order_result:
            # Save to orders table
            cursor.execute('''
                INSERT INTO orders 
                (signal_id, order_id, order_type, status, tradingsymbol, quantity, price, stop_loss)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                sig['id'],
                order_result['order_id'],
                'ENTRY',
                order_result['status'],
                sig_data.get('tradingsymbol'),
                sig_data.get('quantity'),
                sig_data.get('entry_price'),
                sig_data.get('stop_loss')
            ))
            
            # Mark as processed
            cursor.execute("UPDATE signals SET processed = 1 WHERE id = ?", (sig['id'],))
            conn.commit()
            logging.info(f"[SUCCESS] Futures order completed")
        
        continue  # Skip to next signal
    
    # EXISTING OPTIONS CODE CONTINUES HERE...


STEP 3: That's it!
==================

Your order_placer will now:
✅ Use existing kite_config.json (no changes)
✅ Handle OPTIONS (existing code, untouched)
✅ Handle FUTURES (new code added)
✅ Keep all your existing logic

No new files, no config changes!
""")

print("\n" + "="*80)
print("TESTING")
print("="*80)

print("""
After adding the patch:

python order_placer_db_production.py --continuous

When a FUTURES signal comes in:
[FUTURES SIGNAL] GOLD
[FUTURES] GOLD25FEBFUT | BUY | Qty: 100
[OK] Futures order placed: 123456789
[SUCCESS] Futures order completed

When an OPTIONS signal comes in:
(existing behavior - unchanged)
""")
