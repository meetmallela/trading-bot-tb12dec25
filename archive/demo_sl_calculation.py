"""
Demo: Fallback SL Calculation
Shows how SL is calculated when signal is not in database
"""

def calculate_fallback_sl(buy_price):
    """Calculate SL at 95% of buy price, rounded down"""
    sl_price = buy_price * 0.95
    sl_price = int(sl_price)  # Round down to nearest rupee
    return sl_price

# Test cases
test_cases = [
    150,   # Your example
    200,
    175.5,
    100,
    250.75
]

print("="*60)
print("FALLBACK SL CALCULATION (95% Rule)")
print("="*60)

for buy_price in test_cases:
    sl_price = calculate_fallback_sl(buy_price)
    loss = buy_price - sl_price
    loss_pct = (loss / buy_price) * 100
    
    print(f"\nBuy Price: ₹{buy_price}")
    print(f"SL Price:  ₹{sl_price}")
    print(f"Loss:      ₹{loss:.2f} ({loss_pct:.1f}%)")

print("\n" + "="*60)
print("\nLogic:")
print("• SL = Buy Price × 0.95")
print("• Rounded DOWN to nearest ₹1")
print("• Ensures ~5% max loss per trade")
print("="*60)
