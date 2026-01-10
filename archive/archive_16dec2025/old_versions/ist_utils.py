"""
ist_utils.py - IST Timestamp Utilities
Helper functions to ensure all timestamps are in IST (GMT+5:30)
"""

from datetime import datetime, timedelta, timezone


def get_ist_now():
    """
    Get current time in IST (Indian Standard Time)
    
    Returns:
        datetime object in IST
    """
    # Method 1: If system is already in IST, use this
    # return datetime.now()
    
    # Method 2: Calculate IST from UTC (more reliable)
    utc_now = datetime.now(timezone.utc)
    ist_offset = timedelta(hours=5, minutes=30)
    ist_now = utc_now + ist_offset
    return ist_now


def get_ist_now_string(format='%Y-%m-%d %H:%M:%S'):
    """
    Get current time in IST as formatted string
    
    Args:
        format: strftime format string
        
    Returns:
        Formatted IST timestamp string
    """
    ist_now = get_ist_now()
    return ist_now.strftime(format)


def convert_to_ist(dt):
    """
    Convert any datetime to IST
    
    Args:
        dt: datetime object (naive or timezone-aware)
        
    Returns:
        datetime object in IST
    """
    if dt.tzinfo is None:
        # Naive datetime - assume UTC
        utc_dt = dt.replace(tzinfo=timezone.utc)
    else:
        # Timezone-aware - convert to UTC first
        utc_dt = dt.astimezone(timezone.utc)
    
    ist_offset = timedelta(hours=5, minutes=30)
    ist_dt = utc_dt + ist_offset
    return ist_dt


def format_ist_timestamp(dt=None, format='%Y-%m-%d %H:%M:%S'):
    """
    Format a datetime in IST
    
    Args:
        dt: datetime object (None = current time)
        format: strftime format string
        
    Returns:
        Formatted IST timestamp string
    """
    if dt is None:
        return get_ist_now_string(format)
    
    ist_dt = convert_to_ist(dt)
    return ist_dt.strftime(format)


# Quick reference examples
if __name__ == "__main__":
    print("IST Timestamp Utilities - Examples")
    print("="*60)
    
    # Current IST time
    print(f"\nCurrent IST time: {get_ist_now_string()}")
    print(f"Current IST date: {get_ist_now_string('%Y-%m-%d')}")
    print(f"Current IST hour: {get_ist_now_string('%H:%M')}")
    
    # Comparison
    from datetime import datetime
    print(f"\nSystem time:      {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"IST time:         {get_ist_now_string()}")
    print(f"Difference:       +5:30 if system is UTC")
    
    # Usage examples
    print("\n" + "="*60)
    print("USAGE IN YOUR CODE:")
    print("="*60)
    print("""
# Instead of:
timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Use:
from ist_utils import get_ist_now_string
timestamp = get_ist_now_string()

# Or:
from ist_utils import get_ist_now
timestamp = get_ist_now().strftime('%Y-%m-%d %H:%M:%S')
""")
