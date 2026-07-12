"""Reconcile ignore-list: assets the user holds manually (e.g. BNB for the fee
discount) must never be auto-closed by the hourly orphan sweep. Only the pure
parse is unit-tested; the skip itself is a plain `asset in set` membership check
in _reconcile_live_positions.
"""
from engine import PaperTradingEngine as E


def test_parse_ignore_assets():
    p = E._parse_ignore_assets
    assert p("BNB") == {"BNB"}
    assert p("bnb, eth ,Sol") == {"BNB", "ETH", "SOL"}   # trims + upper-cases
    assert p("") == set()                                  # disabled = nothing ignored
    assert p("  ") == set()
