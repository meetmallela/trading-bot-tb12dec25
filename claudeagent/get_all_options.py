# Save this as get_all_options.py
import csv, json
from datetime import datetime, timedelta
from kiteconnect import KiteConnect

with open("kite_config.json") as f:
    cfg = json.load(f)

kite = KiteConnect(api_key=cfg["api_key"])
kite.set_access_token(cfg["access_token"])

today = datetime.now().date()
expiry_limit = today + timedelta(days=45)

print("Fetching NFO instruments...")
instruments = kite.instruments("NFO")

rows = []
for inst in instruments:
    if inst.get("instrument_type") not in ["CE", "PE"]:
        continue
    
    expiry = inst.get("expiry")
    if hasattr(expiry, "date"):
        expiry_date = expiry.date()
    else:
        expiry_date = expiry
    
    if not (today <= expiry_date <= expiry_limit):
        continue
    
    if not inst.get("strike") or inst.get("strike") <= 0:
        continue
    
    rows.append({
        "symbol": inst.get("name"),
        "tradingsymbol": inst.get("tradingsymbol"),
        "strike": inst.get("strike", 0),
        "option_type": inst.get("instrument_type"),
        "expiry_date": expiry_date.strftime("%Y-%m-%d"),
        "tick_size": inst.get("tick_size"),
        "lot_size": inst.get("lot_size"),
        "exchange": "NFO",
        "instrument_type": inst.get("instrument_type")
    })

with open("valid_instruments.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["symbol", "tradingsymbol", "strike", "option_type", "expiry_date", "tick_size", "lot_size", "exchange", "instrument_type"])
    writer.writeheader()
    writer.writerows(rows)

print(f"✅ Generated {len(rows):,} instruments")

# Check for stock options
stocks = [r for r in rows if r['symbol'] not in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']]
print(f"Stock options: {len(stocks):,}")
print("\nSamples:")
for s in stocks[:5]:
    print(f"  {s['tradingsymbol']:25s} Lot: {s['lot_size']}")