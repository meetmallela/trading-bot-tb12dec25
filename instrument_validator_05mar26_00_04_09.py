"""
instrument_validator.py
========================
Pre-order validation guard for order_placer_db_production.py

PURPOSE:
    Validate that a tradingsymbol exists in valid_instruments.csv BEFORE
    sending to Kite API. This prevents wasted retries and [MARKED FAILED]
    errors for garbage signals like "NIFTY26FEB61400CE" (strike 61400 on
    NIFTY which is currently at ~22,500).

ROOT CAUSE of 20-Feb-26 failure:
    Signal 407: NIFTY 61400 CE
    Parser generated: NIFTY26FEB61400CE
    Order placer trusted parser blindly -> 3 retries -> MARKED FAILED
    Could have been caught instantly with this validator.

HOW TO INTEGRATE (order_placer_db_production.py):
    1. Import at top:
         from instrument_validator import InstrumentValidator

    2. Initialize after loading config (once at startup):
         validator = InstrumentValidator('valid_instruments.csv', logger=logging)

    3. In your order execution function, BEFORE kite.place_order():
         is_valid, reason = validator.validate(tradingsymbol, symbol, strike, option_type)
         if not is_valid:
             logging.error(f"[INVALID INSTRUMENT] {tradingsymbol}: {reason}")
             logging.error(f"[SKIP] Skipping order - instrument not in valid_instruments.csv")
             return None  # or however you mark it failed

WHAT IT CATCHES:
    - Strike levels impossible for the symbol (NIFTY 61400 = 3x current index)
    - Tradingsymbol simply not in CSV (expired, wrong format, etc.)
    - Option type mismatches
"""

import csv
import logging
from datetime import datetime
from typing import Optional, Tuple


# Strike range sanity bounds per symbol (approximate, very generous)
# Used as a LAST RESORT if CSV lookup fails
_STRIKE_BOUNDS = {
    'NIFTY':       (15000, 35000),
    'BANKNIFTY':   (40000, 65000),
    'SENSEX':      (55000, 90000),
    'FINNIFTY':    (18000, 30000),
    'MIDCPNIFTY':  (10000, 20000),
    'CRUDEOIL':    (3000,  12000),
    'NATURALGAS':  (100,   500),
    'GOLD':        (55000, 115000),
    'SILVER':      (65000, 140000),
    'GOLDM':       (55000, 115000),
    'SILVERM':     (65000, 140000),
    'COPPER':      (600,   1200),
    'ZINC':        (200,   400),
    'LEAD':        (150,   250),
    'ALUMINIUM':   (180,   320),
    'NICKEL':      (1200,  2400),
}


class InstrumentValidator:
    """
    Validates tradingsymbols against valid_instruments.csv before order placement.

    Usage:
        validator = InstrumentValidator('valid_instruments.csv', logger=logging)
        is_valid, reason = validator.validate('NIFTY26FEB61400CE', 'NIFTY', 61400, 'CE')
        # is_valid = False, reason = "Strike 61400 not found for NIFTY in CSV (valid range: 15000-35000)"
    """

    def __init__(self, csv_path: str = 'valid_instruments.csv',
                 logger=None):
        self.csv_path = csv_path
        self.logger = logger or logging.getLogger(__name__)
        self._tradingsymbols: set = set()
        self._symbol_strikes: dict = {}   # symbol -> set of valid strikes
        self._loaded = False
        self._load()

    def _load(self):
        """Load valid_instruments.csv into memory."""
        try:
            with open(self.csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            for row in rows:
                ts = row.get('tradingsymbol', '').strip()
                symbol = row.get('symbol', '').strip().upper()
                try:
                    strike = float(row.get('strike', 0))
                except (ValueError, TypeError):
                    strike = 0

                if ts:
                    self._tradingsymbols.add(ts)

                if symbol and strike:
                    if symbol not in self._symbol_strikes:
                        self._symbol_strikes[symbol] = set()
                    self._symbol_strikes[symbol].add(strike)

            self._loaded = True
            self.logger.info(
                f"[VALIDATOR] Loaded {len(self._tradingsymbols):,} instruments "
                f"for {len(self._symbol_strikes)} symbols from {self.csv_path}"
            )

        except FileNotFoundError:
            self.logger.warning(
                f"[VALIDATOR] {self.csv_path} not found — "
                f"CSV validation disabled, using strike-range checks only"
            )
        except Exception as e:
            self.logger.error(f"[VALIDATOR] Failed to load CSV: {e}")

    def validate(self, tradingsymbol: str,
                 symbol: Optional[str] = None,
                 strike: Optional[float] = None,
                 option_type: Optional[str] = None) -> Tuple[bool, str]:
        """
        Validate a tradingsymbol before sending to Kite.

        Returns:
            (True, "OK")  — safe to place order
            (False, reason) — do NOT place order
        """
        if not tradingsymbol:
            return False, "Empty tradingsymbol"

        # ── Layer 1: CSV exact match ──────────────────────────────────────
        if self._loaded and self._tradingsymbols:
            if tradingsymbol in self._tradingsymbols:
                return True, "OK"
            else:
                # Build a helpful message showing what DOES exist nearby
                hint = self._get_hint(symbol, strike, option_type)
                return False, (
                    f"'{tradingsymbol}' not found in valid_instruments.csv. {hint}"
                )

        # ── Layer 2: Strike range sanity check (fallback if CSV not loaded) ──
        if symbol and strike is not None:
            sym_upper = symbol.upper()
            if sym_upper in _STRIKE_BOUNDS:
                lo, hi = _STRIKE_BOUNDS[sym_upper]
                if not (lo <= strike <= hi):
                    return False, (
                        f"Strike {strike} is outside plausible range for "
                        f"{sym_upper} ({lo}–{hi}). "
                        f"Likely a parse error (e.g. target/price grabbed as strike)."
                    )

        # ── Layer 3: If CSV not loaded and no bounds defined, warn but allow ──
        self.logger.warning(
            f"[VALIDATOR] Cannot fully validate {tradingsymbol} "
            f"(CSV not loaded, no bounds for {symbol})"
        )
        return True, "Unverified (CSV not loaded)"

    def _get_hint(self, symbol: Optional[str],
                  strike: Optional[float],
                  option_type: Optional[str]) -> str:
        """Build a hint message showing nearby valid strikes."""
        if not symbol or strike is None:
            return ""

        sym = symbol.upper()
        if sym not in self._symbol_strikes:
            return f"Symbol '{sym}' not found in CSV at all."

        valid_strikes = sorted(self._symbol_strikes[sym])
        if not valid_strikes:
            return ""

        # Find nearest 5 valid strikes
        nearest = sorted(valid_strikes, key=lambda s: abs(s - strike))[:5]
        nearest_str = ", ".join(str(int(s)) for s in sorted(nearest))

        # Also check if strike is completely out of range
        min_s, max_s = valid_strikes[0], valid_strikes[-1]
        if strike < min_s or strike > max_s:
            return (
                f"Strike {int(strike)} is OUTSIDE valid range for {sym} "
                f"({int(min_s)}–{int(max_s)}). "
                f"Nearest valid strikes: {nearest_str}. "
                f"This is likely a parse error."
            )
        else:
            return f"Nearest valid strikes for {sym}: {nearest_str}."

    def reload(self):
        """Reload CSV (call this after generating fresh instruments CSV)."""
        self._tradingsymbols.clear()
        self._symbol_strikes.clear()
        self._loaded = False
        self._load()

    def is_loaded(self) -> bool:
        return self._loaded

    def stats(self) -> dict:
        return {
            'loaded': self._loaded,
            'total_tradingsymbols': len(self._tradingsymbols),
            'symbols': len(self._symbol_strikes),
            'csv_path': self.csv_path,
        }


# ---------------------------------------------------------------------------
# EXACT INTEGRATION CODE — copy this into order_placer_db_production.py
# ---------------------------------------------------------------------------
INTEGRATION_CODE = """
# ============================================================
# STEP 1: Add import near top of order_placer_db_production.py
# ============================================================
from instrument_validator import InstrumentValidator


# ============================================================
# STEP 2: Initialize validator ONCE after loading config
#         (add after: logging.basicConfig(...) or similar)
# ============================================================
validator = InstrumentValidator('valid_instruments.csv', logger=logging)


# ============================================================
# STEP 3: Add validation BEFORE kite.place_order()
#
# Find the section that looks like:
#   logging.info(f"[EXECUTE] Placing order for {tradingsymbol}")
#   ...
#   kite.place_order(...)
#
# INSERT this block BEFORE the kite.place_order() call:
# ============================================================

# --- INSTRUMENT VALIDATION: reject garbage strikes before Kite API ---
is_valid, reason = validator.validate(
    tradingsymbol=tradingsymbol,
    symbol=signal.get('symbol'),
    strike=signal.get('strike'),
    option_type=signal.get('option_type'),
)
if not is_valid:
    logging.error(f"[INVALID INSTRUMENT] {tradingsymbol}")
    logging.error(f"[INVALID INSTRUMENT] Reason: {reason}")
    logging.error(f"[SKIP] Order NOT placed — instrument validation failed")
    # Mark signal as failed in DB so order placer doesn't retry it
    db.mark_signal_failed(signal_id, f"INVALID_INSTRUMENT: {reason}")
    return None
# --- END VALIDATION ---

"""


# ---------------------------------------------------------------------------
# SELF-TEST
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

    print("\n" + "="*60)
    print("INSTRUMENT VALIDATOR — SELF TEST (no CSV, using bounds only)")
    print("="*60)

    # Test without CSV (bounds-only mode)
    v = InstrumentValidator(csv_path='nonexistent.csv')

    test_cases = [
        # (tradingsymbol, symbol, strike, option_type, expect_valid)
        ('NIFTY26FEB61400CE',  'NIFTY',    61400,  'CE', False),  # 20-Feb-26 bug
        ('NIFTY26FEB23000CE',  'NIFTY',    23000,  'CE', True),   # Valid NIFTY strike
        ('SENSEX26FEB76400CE', 'SENSEX',   76400,  'CE', True),   # Valid SENSEX
        ('BANKNIFTY26FEB50000PE','BANKNIFTY',50000, 'PE', True),  # Valid BNF
        ('GOLD26FEB75000CE',   'GOLD',     75000,  'CE', True),   # Below old range but valid - CSV needed to confirm
        ('GOLD26FEB85000CE',   'GOLD',     85000,  'CE', True),   # Valid GOLD
        ('NIFTY26FEB99999CE',  'NIFTY',    99999,  'CE', False),  # Clearly wrong
    ]

    all_pass = True
    for ts, sym, strike, opt, expect in test_cases:
        is_valid, reason = v.validate(ts, sym, strike, opt)
        ok = is_valid == expect
        if not ok:
            all_pass = False
        status = "✅" if ok else "❌ FAIL"
        valid_str = "VALID  " if is_valid else "INVALID"
        print(f"  {status} [{valid_str}] {ts:30} | {reason[:60]}")

    print()
    print(f"Result: {'ALL PASSED ✅' if all_pass else 'SOME FAILED ❌'}")
    print()
    print("="*60)
    print("INTEGRATION INSTRUCTIONS:")
    print("="*60)
    print(INTEGRATION_CODE)
