"""
Demo: RECURSIVE Trailing SL
Shows how SL trails continuously every 5% price move
"""

def simulate_recursive_trailing(buy_price, price_points):
    """Simulate recursive trailing SL as price moves"""
    
    # Start with initial SL at 95% of buy
    current_sl = int(buy_price * 0.95)
    
    print(f"BUY PRICE: ₹{buy_price}")
    print(f"INITIAL SL: ₹{current_sl} (95% of buy)\n")
    print("="*80)
    
    for ltp in price_points:
        # Calculate threshold: 105% of CURRENT SL
        threshold = current_sl * 1.05
        
        # Check if we should trail
        if ltp >= threshold:
            old_sl = current_sl
            new_sl = int(ltp * 0.95)  # New SL at 95% of current LTP
            
            if new_sl > current_sl:
                current_sl = new_sl
                profit_from_buy = ((ltp - buy_price) / buy_price) * 100
                sl_from_buy = ((current_sl - buy_price) / buy_price) * 100
                
                print(f"LTP: ₹{ltp:.2f} (+{profit_from_buy:.1f}% from buy)")
                print(f"  Threshold: ₹{threshold:.2f} (105% of SL)")
                print(f"  >>> TRAIL SL: {old_sl} -> {new_sl}")
                print(f"  >>> Protected profit: {sl_from_buy:.1f}% from buy price")
                print()
        else:
            profit_from_buy = ((ltp - buy_price) / buy_price) * 100
            print(f"LTP: ₹{ltp:.2f} (+{profit_from_buy:.1f}% from buy)")
            print(f"  Threshold: ₹{threshold:.2f} - NOT REACHED")
            print(f"  SL stays at: ₹{current_sl}")
            print()
    
    print("="*80)
    print(f"FINAL SL: ₹{current_sl}")
    final_profit_locked = ((current_sl - buy_price) / buy_price) * 100
    print(f"LOCKED PROFIT: {final_profit_locked:.1f}% from buy price")
    print("="*80)

# YOUR ACTUAL EXAMPLE
print("\n" + "="*80)
print("YOUR IRFC EXAMPLE - RECURSIVE TRAILING")
print("="*80 + "\n")

simulate_recursive_trailing(
    buy_price=2.69,
    price_points=[
        2.70,   # Small move up
        2.82,   # 5% from buy - first trail trigger
        2.90,   # More gains
        3.00,   # Crossing threshold again
        3.20,   # More gains
        3.40,   # Trail again
        3.54,   # Current price
        3.70,   # Even higher
        3.98,   # Your mentioned price
    ]
)

print("\n\n" + "="*80)
print("NIFTY EXAMPLE - RECURSIVE TRAILING")
print("="*80 + "\n")

simulate_recursive_trailing(
    buy_price=150,
    price_points=[
        155,    # Small gain
        158,    # 5% threshold
        165,    # More gains
        175,    # Trail again
        180,    # Keep going
        190,    # Trail again
    ]
)
