"""
Order Manager
Handles 2-leg order placement and trailing stop-loss
"""

import time
from typing import Dict, Optional
from datetime import datetime
from trading.kite_integration import get_kite
from trading.instrument_downloader import get_instrument_downloader
from utils.logger import get_logger

logger = get_logger()


class OrderManager:
    """Manages order placement and trailing stop-loss"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.kite = get_kite()
        self.instrument_downloader = get_instrument_downloader()
        
        # Configuration
        self.entry_buffer = config['trading']['entry_price_buffer']
        self.tick_size = config['trading']['tick_size']
        self.trailing_increment = config['trading']['trailing_sl_increment']
        self.max_retries = config['trading']['max_retries']
        self.retry_delay = config['trading']['retry_delay']
        self.sl_defaults = config['stop_loss_defaults']
        
        # Active positions tracking
        self.active_positions = {}  # {tradingsymbol: position_info}
    
    def round_to_tick(self, price: float) -> float:
        """Round price to tick size"""
        return round(price / self.tick_size) * self.tick_size
    
    def process_signal(self, signal: Dict) -> bool:
        """
        Process trading signal - complete 2-leg order flow
        
        Args:
            signal: Parsed signal dictionary
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Processing signal: {signal['underlying']} {signal['strike']} {signal['option_type']}")
            
            # Step 1: Find instrument
            instrument = self._find_instrument(signal)
            if not instrument:
                logger.error("Instrument not found")
                return False
            
            tradingsymbol = instrument['symbol']
            exchange = instrument['exchange']
            lot_size = instrument['lot_size']
            
            # Step 2: Calculate entry price (signal price + buffer, rounded to tick)
            entry_price = self.round_to_tick(signal['entry_price'] + self.entry_buffer)
            
            logger.info(f"Instrument: {tradingsymbol}, Entry trigger: {entry_price}, Lot size: {lot_size}")
            
            # Step 3: Place LEG 1 - Entry order (SL-M buy-stop)
            entry_order_id = self._place_entry_order(
                tradingsymbol=tradingsymbol,
                exchange=exchange,
                quantity=lot_size,
                trigger_price=entry_price,
                signal=signal
            )
            
            if not entry_order_id:
                logger.error("Failed to place entry order")
                return False
            
            # Step 4: Wait for entry order to fill
            if not self._wait_for_order_fill(entry_order_id, timeout=300):
                logger.warning(f"Entry order {entry_order_id} not filled within timeout")
                return False
            
            logger.info(f"Entry order {entry_order_id} FILLED")
            
            # Step 5: Calculate stop-loss
            sl_price = self._calculate_stop_loss(signal, entry_price, instrument)
            
            # Step 6: Place LEG 2 - Stop-loss order (SL-M sell-stop)
            sl_order_id = self._place_sl_order(
                tradingsymbol=tradingsymbol,
                exchange=exchange,
                quantity=lot_size,
                trigger_price=sl_price,
                entry_order_id=entry_order_id
            )
            
            if not sl_order_id:
                logger.error("Failed to place SL order")
                # Entry is filled, but SL failed - log critically
                logger.error(f"CRITICAL: Position open without SL! {tradingsymbol}")
                return False
            
            # Step 7: Track position for trailing SL
            self._track_position(
                tradingsymbol=tradingsymbol,
                exchange=exchange,
                quantity=lot_size,
                entry_price=entry_price,
                sl_price=sl_price,
                sl_order_id=sl_order_id,
                signal=signal
            )
            
            logger.log_trade(signal, entry_order_id, "ACTIVE")
            return True
            
        except Exception as e:
            logger.error(f"Error processing signal: {e}")
            return False
    
    def _find_instrument(self, signal: Dict) -> Optional[Dict]:
        """Find instrument from cache"""
        try:
            return self.instrument_downloader.find_instrument(
                underlying=signal['underlying'],
                strike=signal['strike'],
                option_type=signal['option_type'],
                expiry=signal.get('expiry_date')
            )
        except Exception as e:
            logger.error(f"Error finding instrument: {e}")
            return None
    
    def _place_entry_order(self, tradingsymbol: str, exchange: str, quantity: int,
                          trigger_price: float, signal: Dict) -> Optional[str]:
        """Place entry order (LEG 1)"""
        
        if self.config['trading']['mode'] == 'test':
            logger.info(f"[TEST MODE] Would place: BUY {quantity} x {tradingsymbol} @ {trigger_price}")
            return f"TEST_{int(time.time())}"
        
        return self.kite.place_order_with_retry(
            tradingsymbol=tradingsymbol,
            exchange=exchange,
            transaction_type="BUY",
            quantity=quantity,
            order_type="SL-M",
            trigger_price=trigger_price,
            product="MIS",
            tag=f"ENTRY_{signal['channel']}",
            max_retries=self.max_retries,
            retry_delay=self.retry_delay
        )
    
    def _place_sl_order(self, tradingsymbol: str, exchange: str, quantity: int,
                       trigger_price: float, entry_order_id: str) -> Optional[str]:
        """Place stop-loss order (LEG 2)"""
        
        if self.config['trading']['mode'] == 'test':
            logger.info(f"[TEST MODE] Would place SL: SELL {quantity} x {tradingsymbol} @ {trigger_price}")
            return f"TEST_SL_{int(time.time())}"
        
        return self.kite.place_order_with_retry(
            tradingsymbol=tradingsymbol,
            exchange=exchange,
            transaction_type="SELL",
            quantity=quantity,
            order_type="SL-M",
            trigger_price=trigger_price,
            product="MIS",
            tag=f"SL_{entry_order_id}",
            max_retries=self.max_retries,
            retry_delay=self.retry_delay
        )
    
    def _wait_for_order_fill(self, order_id: str, timeout: int = 300) -> bool:
        """Wait for order to fill"""
        
        if self.config['trading']['mode'] == 'test':
            logger.info(f"[TEST MODE] Simulating order fill for {order_id}")
            time.sleep(1)
            return True
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            order_status = self.kite.get_order_status(order_id)
            
            if order_status and order_status.get('status') == 'COMPLETE':
                return True
            
            time.sleep(5)  # Check every 5 seconds
        
        return False
    
    def _calculate_stop_loss(self, signal: Dict, entry_price: float, instrument: Dict) -> float:
        """Calculate stop-loss price"""
        
        # If signal has SL, use it
        if signal.get('stop_loss'):
            sl_price = signal['stop_loss']
        else:
            # Use default based on instrument type
            instrument_type = instrument.get('instrument_type', 'INDEX_OPTION')
            sl_loss = self.sl_defaults.get(instrument_type, 1000)
            
            # Calculate SL price based on loss amount
            lot_size = instrument['lot_size']
            loss_per_unit = sl_loss / lot_size
            sl_price = entry_price - loss_per_unit
        
        # Round to tick and ensure positive
        sl_price = max(self.round_to_tick(sl_price), self.tick_size)
        
        logger.info(f"Stop-loss calculated: {sl_price} (Entry: {entry_price})")
        return sl_price
    
    def _track_position(self, tradingsymbol: str, exchange: str, quantity: int,
                       entry_price: float, sl_price: float, sl_order_id: str, signal: Dict):
        """Track position for trailing SL"""
        
        self.active_positions[tradingsymbol] = {
            'exchange': exchange,
            'quantity': quantity,
            'entry_price': entry_price,
            'current_sl': sl_price,
            'sl_order_id': sl_order_id,
            'last_trail_price': entry_price,  # Track when we last updated SL
            'signal': signal,
            'timestamp': datetime.now()
        }
        
        logger.info(f"Position tracked: {tradingsymbol}, Entry: {entry_price}, SL: {sl_price}")
    
    def update_trailing_stops(self):
        """Update trailing stop-losses for all active positions"""
        
        if not self.active_positions:
            return
        
        logger.debug(f"Checking trailing SL for {len(self.active_positions)} positions")
        
        for tradingsymbol, position in list(self.active_positions.items()):
            try:
                # Get current LTP
                ltp = self.kite.get_ltp(position['exchange'], tradingsymbol)
                
                if not ltp:
                    continue
                
                entry_price = position['entry_price']
                last_trail_price = position['last_trail_price']
                current_sl = position['current_sl']
                
                # Check if price has moved up by trailing increment
                price_gain = ltp - last_trail_price
                
                if price_gain >= self.trailing_increment:
                    # Calculate new SL (move up by increment)
                    new_sl = self.round_to_tick(current_sl + self.trailing_increment)
                    
                    # Modify SL order
                    if self._modify_sl_order(position['sl_order_id'], new_sl):
                        position['current_sl'] = new_sl
                        position['last_trail_price'] = ltp
                        logger.info(f"Trailing SL updated: {tradingsymbol}, New SL: {new_sl}, LTP: {ltp}")
                
            except Exception as e:
                logger.error(f"Error updating trailing SL for {tradingsymbol}: {e}")
    
    def _modify_sl_order(self, order_id: str, new_trigger_price: float) -> bool:
        """Modify stop-loss order trigger price"""
        
        if self.config['trading']['mode'] == 'test':
            logger.info(f"[TEST MODE] Would modify SL order {order_id} to {new_trigger_price}")
            return True
        
        return self.kite.modify_order(
            order_id=order_id,
            trigger_price=new_trigger_price
        )
    
    def exit_position(self, tradingsymbol: str, reason: str = "EXIT_SIGNAL"):
        """Exit position immediately (market order)"""
        
        if tradingsymbol not in self.active_positions:
            logger.warning(f"Position {tradingsymbol} not found in active positions")
            return
        
        position = self.active_positions[tradingsymbol]
        
        logger.info(f"Exiting position: {tradingsymbol}, Reason: {reason}")
        
        # Cancel SL order
        self.kite.cancel_order(position['sl_order_id'])
        
        # Place market order to exit
        if self.config['trading']['mode'] != 'test':
            self.kite.square_off_position(
                tradingsymbol=tradingsymbol,
                exchange=position['exchange'],
                quantity=position['quantity']
            )
        else:
            logger.info(f"[TEST MODE] Would exit {tradingsymbol}")
        
        # Remove from tracking
        del self.active_positions[tradingsymbol]
        logger.info(f"Position {tradingsymbol} closed")
    
    def exit_all_positions(self, reason: str = "EXIT_ALL"):
        """Exit all active positions"""
        logger.info(f"Exiting all positions, Reason: {reason}")
        
        for tradingsymbol in list(self.active_positions.keys()):
            self.exit_position(tradingsymbol, reason)
    
    def get_active_positions_count(self) -> int:
        """Get number of active positions"""
        return len(self.active_positions)


# Singleton instance
_order_manager_instance = None


def get_order_manager(config: Dict = None) -> OrderManager:
    """Get singleton order manager"""
    global _order_manager_instance
    
    if _order_manager_instance is None and config:
        _order_manager_instance = OrderManager(config)
    
    return _order_manager_instance
