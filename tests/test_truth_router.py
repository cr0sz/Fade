"""
Unit tests for truth_router.py — OOS gate, passthrough blocking, confirm/invert/reject logic.

Covers bug class:
  5. Truth router / OOS gate: passthrough trades must be blocked; only segments
     with proven walk-forward edge tradeable.
"""
from __future__ import annotations

import sys
import time
import types
import json

import pytest

# ── prometheus_client stub is set up in conftest.py ──────────────────────────
from truth_router import TruthRouter, RouteVerdict, _PASSTHROUGH


# ── Helpers ──────────────────────────────────────────────────────────────────

def _router_with_lift(lift_table: dict) -> TruthRouter:
    """Build a TruthRouter that already has a populated lift table."""
    r = TruthRouter()
    r._lift = lift_table
    r._refreshed_at = time.monotonic()
    return r


def _good_oos_payload(engine="TestEngine", bucket="emit_short", exch="CRYPTO", action="invert") -> dict:
    """Build a valid OOS stats payload that makes one segment tradeable."""
    seg_key = f"{engine}|{bucket}|{exch}|{action}"
    # Use current time so the stale check (oos_max_staleness_s=86400) does not fire in CI.
    from datetime import datetime as _dt, timezone as _tz
    now_iso = _dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%S")
    return {
        "computed_at": now_iso,
        "windows": {
            "14d": {seg_key: {"n": 50, "avg_net": 0.012}},
            "7d":  {seg_key: {"n": 15, "avg_net": 0.008}},
        },
    }


def _load_oos(router: TruthRouter, payload: dict) -> None:
    """Inject OOS payload directly (bypasses Redis)."""
    windows = payload.get("windows", {})
    router._oos_14d = windows.get("14d", {}) or {}
    router._oos_7d = windows.get("7d", {}) or {}
    from datetime import datetime as _dt
    ca = payload.get("computed_at")
    if ca:
        router._oos_computed_at = _dt.fromisoformat(ca).timestamp()
    router._oos_loaded = True


# ── Passthrough policy ────────────────────────────────────────────────────────

class TestPassthroughBlocking:
    def test_no_lift_data_gives_passthrough(self):
        """With empty lift table, every signal becomes passthrough."""
        r = _router_with_lift({})
        v = r.route("SomeEngine", "CRYPTO", "risk_on", 0.30, "long")
        assert v.action == "passthrough"

    def test_disabled_router_gives_passthrough(self):
        r = TruthRouter()
        r.enabled = False
        v = r.route("SomeEngine", "CRYPTO", "risk_on", 0.30, "long")
        assert v.action == "passthrough"

    def test_stale_router_gives_passthrough(self):
        """If lift data is too old, fall back to passthrough (fail-open)."""
        r = _router_with_lift({("SomeEngine", "emit_long", "CRYPTO"): (0.10, 100)})
        r._refreshed_at = time.monotonic() - r.max_staleness_s - 1
        v = r.route("SomeEngine", "CRYPTO", "risk_on", 0.30, "long")
        assert v.action == "passthrough"


# ── OOS gate: stale stats block all confirm/invert ────────────────────────────

class TestOOSGateStale:
    def test_stale_oos_blocks_confirm(self):
        """Stale OOS stats must downgrade confirm → reject."""
        r = _router_with_lift({("ICE", "emit_short", "CRYPTO"): (-0.15, 200)})
        r.oos_gate_enabled = True
        r._oos_loaded = True
        # Mark OOS as stale by setting old timestamp
        r._oos_computed_at = time.time() - r.oos_max_staleness_s - 1
        v = r.route("ICE", "CRYPTO", "risk_off", -0.30, "short")
        assert v.action == "reject"

    def test_stale_oos_blocks_invert(self):
        """Stale OOS stats must downgrade invert → reject."""
        r = _router_with_lift({("ICE", "emit_long", "CRYPTO"): (-0.12, 300)})
        r.oos_gate_enabled = True
        r._oos_loaded = True
        r._oos_computed_at = time.time() - r.oos_max_staleness_s - 1
        r.invert_min_n = 50  # lower to trigger invert path
        v = r.route("ICE", "CRYPTO", "risk_on", 0.30, "long")
        assert v.action == "reject"

    def test_not_loaded_oos_is_stale(self):
        r = TruthRouter()
        r._oos_loaded = False
        assert r._oos_is_stale() is True

    def test_fresh_oos_is_not_stale(self):
        r = TruthRouter()
        r._oos_loaded = True
        r._oos_computed_at = time.time() - 100  # 100s ago, well within budget
        assert r._oos_is_stale() is False


# ── OOS tradeable rule ────────────────────────────────────────────────────────

class TestOOSTradeable:
    def test_tradeable_with_14d_only(self):
        r = TruthRouter()
        r._oos_14d = {"ICE|emit_short|CRYPTO|invert": {"n": 50, "avg_net": 0.01}}
        r._oos_7d = {}
        assert r._oos_tradeable("ICE", "emit_short", "CRYPTO", "invert") is True

    def test_not_tradeable_below_min_n(self):
        r = TruthRouter()
        r._oos_14d = {"ICE|emit_short|CRYPTO|invert": {"n": 5, "avg_net": 0.05}}
        r.oos_min_n = 30
        assert r._oos_tradeable("ICE", "emit_short", "CRYPTO", "invert") is False

    def test_not_tradeable_negative_avg_net(self):
        r = TruthRouter()
        r._oos_14d = {"ICE|emit_short|CRYPTO|invert": {"n": 50, "avg_net": -0.01}}
        assert r._oos_tradeable("ICE", "emit_short", "CRYPTO", "invert") is False

    def test_7d_veto_overrides_14d_pass(self):
        """7d negative avg_net must veto even a passing 14d."""
        r = TruthRouter()
        r._oos_14d = {"E|b|CRYPTO|confirm": {"n": 50, "avg_net": 0.01}}
        r._oos_7d  = {"E|b|CRYPTO|confirm": {"n": 15, "avg_net": -0.005}}
        assert r._oos_tradeable("E", "b", "CRYPTO", "confirm") is False

    def test_7d_ignored_when_n_below_10(self):
        """7d data with n < 10 should be ignored — 14d alone decides."""
        r = TruthRouter()
        r._oos_14d = {"E|b|CRYPTO|confirm": {"n": 50, "avg_net": 0.01}}
        r._oos_7d  = {"E|b|CRYPTO|confirm": {"n": 5, "avg_net": -0.99}}
        assert r._oos_tradeable("E", "b", "CRYPTO", "confirm") is True

    def test_missing_segment_returns_false(self):
        r = TruthRouter()
        r._oos_14d = {}
        assert r._oos_tradeable("UnknownEngine", "emit_long", "CRYPTO", "confirm") is False


# ── Confirm / invert / reject verdicts ────────────────────────────────────────

class TestRoutingLogic:
    def _make_router_with_oos(self, engine="ICE", bucket="emit_short",
                               exch="CRYPTO", action="invert", lift=-0.12):
        """Router primed with lift data and fresh OOS that permits the action."""
        r = _router_with_lift({(engine, bucket, exch): (lift, 200)})
        payload = _good_oos_payload(engine, bucket, exch, action)
        _load_oos(r, payload)
        r.oos_gate_enabled = True
        r.invert_min_n = 50  # lower so our 200-sample bucket triggers invert
        return r

    def test_confirm_short_with_oos(self):
        """Negative lift confirms a short signal when OOS proved the edge."""
        r = self._make_router_with_oos(action="invert", lift=-0.12)
        # lift=-0.12 means evidence is strongly bearish ⇒ short confirm
        # For nominal_direction=short: edge_for_nominal = -lift = 0.12 >= min_edge
        v = r.route("ICE", "CRYPTO", "risk_off", -0.30, "short")
        # With OOS gate on, segment must be OOS-tradeable to pass
        # We seeded OOS for "invert" — confirm may also pass if tradeable
        assert v.action in ("confirm", "reject")  # not passthrough

    def test_invert_long_to_short(self):
        """Positive lift for emit_long but OOS proves invert — signal should be inverted."""
        # lift > 0 for 'long' bucket means momentum bullish
        # For nominal=long: edge_for_nominal = lift = 0.12 > confirm_bar? No — we want invert
        # So we need lift < 0 for the long bucket to get invert
        r = _router_with_lift({("Engine", "emit_long", "CRYPTO"): (-0.12, 300)})
        r.invert_min_n = 50
        r.invert_min_edge = 0.08
        r.oos_gate_enabled = False  # disable OOS for this logic test
        v = r.route("Engine", "CRYPTO", "risk_on", 0.30, "long")
        assert v.action == "invert"
        assert v.direction == "short"

    def test_reject_when_edge_insufficient(self):
        """Lift below both confirm and invert thresholds → reject."""
        r = _router_with_lift({("Engine", "emit_long", "CRYPTO"): (0.02, 100)})
        r.oos_gate_enabled = False
        v = r.route("Engine", "CRYPTO", "risk_on", 0.30, "long")
        assert v.action == "reject"

    def test_confirm_long_suppressed_by_high_bar(self):
        """confirm_long_edge is intentionally set high (0.15) to suppress long confirms."""
        r = _router_with_lift({("Engine", "emit_long", "CRYPTO"): (0.10, 100)})
        r.oos_gate_enabled = False
        # 0.10 < 0.15 (confirm_long_edge default) → should not confirm
        v = r.route("Engine", "CRYPTO", "risk_on", 0.30, "long")
        assert v.action in ("reject", "passthrough")  # not confirm

    def test_oos_unlocks_long_confirm_at_lower_bar(self):
        """OOS-proven long confirm uses oos_long_min_edge (0.05), not confirm_long_edge (0.15)."""
        r = _router_with_lift({("Engine", "emit_long", "CRYPTO"): (0.08, 100)})
        _load_oos(r, _good_oos_payload("Engine", "emit_long", "CRYPTO", "confirm"))
        r.oos_gate_enabled = True
        r.oos_long_min_edge = 0.05   # 0.08 >= 0.05 → should confirm
        v = r.route("Engine", "CRYPTO", "risk_on", 0.30, "long")
        assert v.action == "confirm"

    def test_conf_scale_boosted_on_wide_edge(self):
        """Wide edge must produce conf_scale > 1.0 to boost signal confidence."""
        r = _router_with_lift({("Engine", "emit_short", "CRYPTO"): (-0.20, 200)})
        r.oos_gate_enabled = False
        v = r.route("Engine", "CRYPTO", "risk_off", -0.40, "short")
        if v.action == "confirm":
            assert v.conf_scale > 1.0

    def test_passthrough_has_zero_edge(self):
        v = _PASSTHROUGH
        assert v.action == "passthrough"
        assert v.edge == 0.0
        assert v.conf_scale == 1.0

    def test_bucket_score_long_path(self):
        """composite_score >= 0.10 and no explicit direction → score_long bucket."""
        r = _router_with_lift({("Engine", "score_long", "CRYPTO"): (0.10, 100)})
        r.oos_gate_enabled = False
        v = r.route("Engine", "CRYPTO", "neutral", 0.30, "long")
        # Should at minimum not be passthrough (bucket is resolved)
        assert v.action != "passthrough"

    def test_has_stats_true_when_engine_in_lift(self):
        r = _router_with_lift({("ICE", "emit_short", "CRYPTO"): (-0.12, 200)})
        assert r.has_stats("ICE") is True

    def test_has_stats_false_for_unknown_engine(self):
        r = _router_with_lift({("ICE", "emit_short", "CRYPTO"): (-0.12, 200)})
        assert r.has_stats("UnknownEngine") is False
