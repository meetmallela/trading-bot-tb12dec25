"""
Demo: Trailing SL Logic
Shows how SL trails as price moves favorably
"""

def calculate_trailing_sl_demo(buy_price, current_ltp):
    """Demo of trailing SL calculation for manual orders"""
    
    # Initial SL (when position opened)
    initial_sl = int(buy_price * 0.95)
    
    # Threshold for trailing (5% profit)
    threshold = buy_price * 1.05
    
    # Trailing SL (104% of buy price)
    trailing_sl = int(buy_price * 1.04)
    
    # Determine current SL
    if current_ltp >= threshold:
        current_sl = trailing_sl
        status = "TRAILING"
    else:
        current_sl = initial_sl
        status = "INITIAL"
    
    return {
        'buy_price': buy_price,
        'ltp': current_ltp,
        'threshold': threshold,
        'initial_sl': initial_sl,
        'trailing_sl': trailing_sl,
        'current_sl': current_sl,
        'status': status,
        'profit_pct': ((current_ltp - buy_price) / buy_price) * 100
    }

# Test scenarios
scenarios = [
    # Your example: Buy @ 150
    (150, 145),   # Price down - keep initial SL
    (150, 150),   # Price flat - keep initial SL
    (150, 155),   # Price up 3.3% - still initial SL
    (150, 157.5), # Price up 5% - TRIGGER trailing!
    (150, 160),   # Price up 6.7% - trailing SL active
    (150, 165),   # Price up 10% - trailing SL active
    
    # Another example: Buy @ 200
    (200, 190),   # Price down
    (200, 210),   # Price up 5% - TRIGGER!
]

print("="*80)
print("TRAILING SL DEMONSTRATION - MANUAL ORDERS")
print("="*80)
print()

for buy, ltp in scenarios:
    result = calculate_trailing_sl_demo(buy, ltp)
    
    print(f"Buy: ₹{result['buy_price']:.2f} | LTP: ₹{result['ltp']:.2f} | "
          f"Profit: {result['profit_pct']:+.1f}%")
    print(f"  Threshold: ₹{result['threshold']:.2f} (5% profit mark)")
    print(f"  Initial SL: ₹{result['initial_sl']} (95% of buy)")
    print(f"  Trailing SL: ₹{result['trailing_sl']} (104% of buy)")
    print(f"  >>> ACTIVE SL: ₹{result['current_sl']} [{result['status']}]")
    
    if result['status'] == 'TRAILING':
        print(f"  *** SL TRAILED! Price hit 5% profit, SL moved to protect 4% gain ***")
    
    print()

print("="*80)
print("\nTRAILING LOGIC:")
print("1. Initial SL: 95% of buy price (max 5% loss)")
print("2. When LTP reaches 105% of buy (5% profit):")
print("   -> Trail SL to 104% of buy price (locks in 4% gain)")
print("3. Manual orders follow this 5%->4% trailing rule")
print("4. Signal-based orders use original signal's SL")
print("="*80)
