"""
STEP 1: Kite Login and Token Generation
Run this ONCE to generate access token
"""

from kiteconnect import KiteConnect
import os
from datetime import datetime

# Your Kite API credentials
API_KEY = "olcwzq15suerys7u"
API_SECRET = "5ep60dw8pf4kq4u83un461ugq0lt4qnc"

def generate_kite_token():
    """Generate Kite access token"""
    
    print("\n" + "="*80)
    print("KITE TOKEN GENERATOR")
    print("="*80)
    
    # Initialize Kite
    kite = KiteConnect(api_key=API_KEY)
    
    # Get login URL
    login_url = kite.login_url()
    
    print("\nSTEP 1: Open this URL in your browser:")
    print(login_url)
    print("\nSTEP 2: Login and authorize")
    print("STEP 3: Copy the 'request_token' from the redirect URL")
    print("="*80)
    
    # Get request token from user
    request_token = input("\nEnter request_token: ").strip()
    
    if not request_token:
        print("ERROR: No request token provided")
        return None
    
    try:
        # Generate session
        data = kite.generate_session(request_token, api_secret=API_SECRET)
        access_token = data["access_token"]
        
        # Save to file
        with open("kite_token.txt", "w") as f:
            f.write(f"# Generated on: {datetime.now()}\n")
            f.write(f"ACCESS_TOKEN={access_token}\n")
            f.write(f"API_KEY={API_KEY}\n")
        
        print("\n" + "="*80)
        print("SUCCESS! Token generated and saved to kite_token.txt")
        print("="*80)
        print(f"\nAccess Token: {access_token}")
        print("\nYou can now run step2_load_instruments.py")
        print("="*80)
        
        return access_token
        
    except Exception as e:
        print(f"\nERROR: {e}")
        return None

if __name__ == "__main__":
    generate_kite_token()
