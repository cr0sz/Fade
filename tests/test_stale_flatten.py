"""Blind-flatten decision: a LIVE position held too long on a dead price feed must close.

Only the pure decision predicate is unit-tested; the close sequence mirrors the proven
F7 stale-emergency flatten in _check_positions.
"""
from engine import PaperTradingEngine as E


def test_blind_flatten_due():
    f = E._blind_flatten_due
    assert f(True, True, 1300, 1200)        # live + stale + past threshold → flatten
    assert not f(True, True, 1100, 1200)    # stale but within window → hold (hysteresis guard)
    assert not f(True, True, 1300, 0)       # flatten_s=0 → disabled
    assert not f(False, True, 9999, 1200)   # paper → never (no real-money blind risk)
    assert not f(True, False, 9999, 1200)   # feed fresh → never
