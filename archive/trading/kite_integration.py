"""
Kite Connect API Integration
Wrapper for Zerodha Kite Connect API
"""

from kiteconnect import KiteConnect
from typing import Dict, Optional, List
import time
from utils.logger import get_logger

logger = get_logger()


class KiteIntegration:
    """Wrapper for Kite Connect API"""
    
    def __init__(self, api_key: str, api_secret: str, access_token: str = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.kite = KiteConnect(api_key=api_key)
        self.access_token = access_token
        
        if access_token:
            self.kite.set_access_token(access_token)
            logger.info("Kite API initialized with access token")
    
    def generate_session(self, request_token: str) -> str:
        """Generate access token from request token"""
        try:
            data = self.kite.generate_session(request_token, api_secret=self.api_secret)
            self.access_token = data["access_token"]
            self.kite.set_access_token(self.access_token)
            logger.info("Kite session generated successfully")
            return self.access_token
        except Exception as e:
            logger.error(f"Failed to generate session: {e}")
            raise
    
    def get_login_url(self) -> str:
        """Get login URL for manual authentication"""
        return self.kite.login_url()
    
    def place_order(self, tradingsymbol: str, exchange: str, transaction_type: str,
                   quantity: int, order_type: str, price: float = None,
                   trigger_price: float = None, product: str = "MIS",
                   validity: str = "DAY", tag: str = None) -> Optional[str]:
        """
        Place order on Kite
        
        Args:
            tradingsymbol: Trading symbol (e.g., "NIFTY24DEC26000PE")
            exchange: Exchange (NFO, BFO, MCX, CDS)
            transaction_type: BUY or SELL
            quantity: Quantity (must be in lot size)
            order_type: MARKET, LIMIT, SL, SL-M
            price: Limit price (for LIMIT and SL orders)
            trigger_price: Trigger price (for SL and SL-M orders)
            product: MIS (intraday) or NRML (carry forward)
            validity: DAY or IOC
            tag: Optional tag for tracking
        
        Returns:
            Order ID or None if failed
        """
        try:
            order_params = {
                "tradingsymbol": tradingsymbol,
                "exchange": exchange,
                "transaction_type": transaction_type,
                "quantity": quantity,
                "order_type": order_type,
                "product": product,
                "validity": validity
            }
            
            if price:
                order_params["price"] = price
            
            if trigger_price:
                order_params["trigger_price"] = trigger_price
            
            if tag:
                order_params["tag"] = tag
            
            logger.info(f"Placing order: {transaction_type} {quantity} x {tradingsymbol} @ {trigger_price or price}")
            
            order_id = self.kite.place_order(**order_params)
            
            logger.log_order(
                action=transaction_type,
                instrument=tradingsymbol,
                quantity=quantity,
                price=trigger_price or price or 0,
                order_id=order_id,
                status="PLACED"
            )
            
            return order_id
            
        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            return None
    
    def place_order_with_retry(self, max_retries: int = 3, retry_delay: int = 30, **kwargs) -> Optional[str]:
        """Place order with retry logic"""
        for attempt in range(1, max_retries + 1):
            order_id = self.place_order(**kwargs)
            
            if order_id:
                return order_id
            
            if attempt < max_retries:
                logger.warning(f"Order failed, retrying in {retry_delay}s (attempt {attempt}/{max_retries})")
                time.sleep(retry_delay)
        
        logger.error(f"Order failed after {max_retries} attempts")
        return None
    
    def get_order_status(self, order_id: str) -> Optional[Dict]:
        """Get order status"""
        try:
            orders = self.kite.orders()
            for order in orders:
                if order['order_id'] == order_id:
                    return order
            return None
        except Exception as e:
            logger.error(f"Failed to get order status: {e}")
            return None
    
    def modify_order(self, order_id: str, order_type: str = None, price: float = None,
                    trigger_price: float = None, quantity: int = None) -> bool:
        """Modify existing order"""
        try:
            params = {}
            if order_type:
                params['order_type'] = order_type
            if price:
                params['price'] = price
            if trigger_price:
                params['trigger_price'] = trigger_price
            if quantity:
                params['quantity'] = quantity
            
            self.kite.modify_order(order_id, **params)
            logger.info(f"Order {order_id} modified successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to modify order {order_id}: {e}")
            return False
    
    def cancel_order(self, order_id: str) -> bool:
        """Cancel order"""
        try:
            self.kite.cancel_order(order_id)
            logger.info(f"Order {order_id} cancelled")
            return True
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            return False
    
    def get_positions(self) -> Dict:
        """Get current positions"""
        try:
            return self.kite.positions()
        except Exception as e:
            logger.error(f"Failed to get positions: {e}")
            return {'net': [], 'day': []}
    
    def get_ltp(self, exchange: str, tradingsymbol: str) -> Optional[float]:
        """Get Last Traded Price"""
        try:
            key = f"{exchange}:{tradingsymbol}"
            ltp_data = self.kite.ltp([key])
            return ltp_data[key]['last_price']
        except Exception as e:
            logger.error(f"Failed to get LTP for {tradingsymbol}: {e}")
            return None
    
    def square_off_position(self, tradingsymbol: str, exchange: str, 
                           quantity: int, product: str = "MIS") -> Optional[str]:
        """Square off position (market order)"""
        try:
            # Determine transaction type (opposite of current position)
            positions = self.get_positions()
            transaction_type = "SELL"  # Default
            
            for pos in positions.get('day', []):
                if pos['tradingsymbol'] == tradingsymbol:
                    transaction_type = "SELL" if pos['quantity'] > 0 else "BUY"
                    break
            
            return self.place_order(
                tradingsymbol=tradingsymbol,
                exchange=exchange,
                transaction_type=transaction_type,
                quantity=abs(quantity),
                order_type="MARKET",
                product=product,
                tag="SQUARE_OFF"
            )
            
        except Exception as e:
            logger.error(f"Failed to square off {tradingsymbol}: {e}")
            return None
    
    def get_instruments(self, exchange: str = None) -> List[Dict]:
        """Get instruments list"""
        try:
            if exchange:
                return self.kite.instruments(exchange)
            else:
                return self.kite.instruments()
        except Exception as e:
            logger.error(f"Failed to get instruments: {e}")
            return []
    
    def is_market_open(self) -> bool:
        """Check if market is open"""
        try:
            # Try to fetch quote - if successful, market is open
            self.kite.ltp(["NSE:NIFTY 50"])
            return True
        except:
            return False


# Singleton instance
_kite_instance = None


def get_kite(api_key: str = None, api_secret: str = None, access_token: str = None) -> KiteIntegration:
    """Get singleton Kite instance"""
    global _kite_instance
    
    if _kite_instance is None and api_key:
        _kite_instance = KiteIntegration(api_key, api_secret, access_token)
    
    return _kite_instance
