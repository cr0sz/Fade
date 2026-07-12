"""Truth Router — routes signals by measured edge instead of emitted direction.

Sprint 38. Every engine's directional reliability is measured continuously by the
outcome tracker (signal_outcomes = real price returns, honest since Sprint 35).
This module turns those measurements into per-signal routing decisions:

  confirm      measured lift agrees with the signal's nominal direction
  invert       measured lift is strongly AGAINST the nominal direction — trade
               the opposite side (fade). Requires a deep sample and a wide edge.
  reject       stats exist but show no tradeable edge either way
  passthrough  not enough data for this (engine, bucket, exchange) — legacy
               gates decide, exactly as before this module existed

Why lift and not raw win rate: the sample period's market drift (e.g. a bear
month where 63% of all 24h windows are down) would otherwise be misread as
engine skill. Lift = P(up | signal) − P(up | any signal in same exchange class),
computed over a rolling window, so a regime change re-anchors the baseline and
inversions auto-disable when their edge collapses.

Buckets: engines that emit explicit direction map to emit_long/emit_short;
neutral-direction signals that the trader would act on via composite-score
thresholds map to score_long/score_short. Bursts are deduplicated per
(ticker, hour) so one trending afternoon doesn't masquerade as 50 samples.

Kill switches: ROUTER_ENABLED=false restores pre-Sprint-38 behaviour entirely;
ROUTER_INVERT=false keeps confirm/reject but never fades a signal.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass

import asyncpg
import structlog
from prometheus_client import Counter

logger = structlog.get_logger(__name__)

_OOS_STATS_KEY = "router:oos_stats"

router_verdicts_total = Counter(
    "pt_router_verdicts_total", "Truth Router verdicts", ["action"]
)

_POSITIVE_DIRS = ("risk_on", "inflow", "liquidity_expansion")
_NEGATIVE_DIRS = ("risk_off", "outflow", "tightening", "volatility_regime")

# A return within ±0.2% is not a tradeable move (matches outcome tracker).
_FLAT_BAND = 0.002

_STATS_SQL = """
WITH src AS (
  SELECT sr.engine_name,
         CASE WHEN sr.direction IN ('risk_on','inflow','liquidity_expansion') THEN 'emit_long'
              WHEN sr.direction IN ('risk_off','outflow','tightening','volatility_regime') THEN 'emit_short'
              WHEN sr.composite_score >= 0.10 THEN 'score_long'
              WHEN sr.composite_score <= -0.10 THEN 'score_short'
              ELSE NULL END AS bucket,
         CASE WHEN UPPER(COALESCE(t.exchange,'')) = 'CRYPTO' THEN 'CRYPTO' ELSE 'OTHER' END AS exch,
         sr.ticker, date_trunc('hour', sr.emitted_at) AS hr,
         so.outcome_score_delta AS ret
    FROM signal_outcomes so
    JOIN signals_registry sr ON sr.id = so.signal_id
    LEFT JOIN tickers t ON t.symbol = sr.ticker
   WHERE so.evaluation_window_h = $1
     AND so.outcome_score_delta IS NOT NULL
     AND sr.emitted_at > NOW() - ($2 || ' days')::interval
     AND sr.engine_name <> 'technical_replay'
), dedup AS (
  SELECT DISTINCT ON (engine_name, bucket, exch, ticker, hr)
         engine_name, bucket, exch, ret
    FROM src WHERE bucket IS NOT NULL
)
SELECT engine_name, bucket, exch,
       COUNT(*) FILTER (WHERE ret > $3)  AS ups,
       COUNT(*) FILTER (WHERE ret < -$3) AS downs
  FROM dedup GROUP BY 1, 2, 3
"""


@dataclass(frozen=True)
class RouteVerdict:
    action: str          # confirm | invert | reject | passthrough
    direction: str       # final trade direction ("long" | "short"); for reject, the nominal one
    edge: float          # shrunk lift toward the FINAL direction (0.0 when passthrough)
    n: int               # effective sample size behind the verdict
    conf_scale: float    # multiplier for signal confidence (1.0 = unchanged)


_PASSTHROUGH = RouteVerdict("passthrough", "", 0.0, 0, 1.0)


class TruthRouter:
    def __init__(self, notifier=None) -> None:
        self.enabled = os.getenv("ROUTER_ENABLED", "true").lower() == "true"
        self.allow_invert = os.getenv("ROUTER_INVERT", "true").lower() == "true"
        self.window_h = int(os.getenv("ROUTER_LIFT_WINDOW_H", os.getenv("ROUTER_WINDOW_H", "24")))
        self.lookback_days = int(os.getenv("ROUTER_LOOKBACK_DAYS", "30"))
        self.min_edge = float(os.getenv("ROUTER_MIN_EDGE", "0.05"))
        # Walk-forward validation (scripts/router_walkforward.py, 14d OOS):
        # confirming engine BULLISHNESS off trailing lift loses badly (-1.7%/trade,
        # 22.8% WR — "longs worked recently" marks bounce tops in a bear), while
        # short confirms (+0.72%) and inversions (+1.21%) are profitable. Long
        # confirms therefore need a much wider bar; 0.15 is effectively "off"
        # until a persistent long edge appears. Re-run the script if regime flips.
        self.confirm_long_edge = float(os.getenv("ROUTER_CONFIRM_LONG_EDGE", "0.15"))
        self.invert_min_edge = float(os.getenv("ROUTER_INVERT_MIN_EDGE", "0.08"))
        self.min_n = int(os.getenv("ROUTER_MIN_N", "30"))
        self.invert_min_n = int(os.getenv("ROUTER_INVERT_MIN_N", "100"))
        self.refresh_interval_s = int(os.getenv("ROUTER_REFRESH_S", "3600"))
        # Shrinkage constant: lift is pulled toward 0 by k/(n+k) — a 50-sample
        # bucket only keeps half its observed lift.
        self.shrink_k = float(os.getenv("ROUTER_SHRINK_K", "50"))
        # After this long without a successful refresh, stop routing (fail open
        # to legacy behaviour) rather than act on a stale market snapshot.
        self.max_staleness_s = int(os.getenv("ROUTER_MAX_STALENESS_S", "86400"))

        # ── Sprint 53: continuous OOS guardrail ───────────────────────────────
        # Static thresholds can't notice edge death. The engine writes a rolling
        # walk-forward (7d + 14d, net of 0.30% cost) per-segment table to Redis;
        # the router gates every confirm/invert on it. No evidence ⇒ no trade.
        self._notifier = notifier
        self.oos_gate_enabled = os.getenv("OOS_GATE_ENABLED", "true").lower() == "true"
        self.oos_min_n = int(os.getenv("OOS_MIN_N", "30"))
        # Stale OOS stats ⇒ treat every confirm/invert as non-tradeable.
        self.oos_max_staleness_s = int(os.getenv("OOS_MAX_STALENESS_S", "86400"))
        # OOS-tradeable confirm-long is gated by min_edge, not confirm_long_edge —
        # the OOS proof supersedes the static long suppression bar.
        self.oos_long_min_edge = float(os.getenv("OOS_LONG_MIN_EDGE", "0.05"))

        # 14d/7d OOS tables keyed by "engine|bucket|exch|action"
        self._oos_14d: dict[str, dict] = {}
        self._oos_7d: dict[str, dict] = {}
        self._oos_computed_at: float = 0.0  # epoch seconds parsed from payload
        self._oos_loaded: bool = False
        self._oos_last_stale_log: float = 0.0  # monotonic; rate-limit loud log
        self._oos_stale_alerted: bool = False  # one-time Discord alert sent
        # First refresh after process start takes a few minutes; rejecting in
        # that window is correct but alerting about it is a false alarm.
        self._oos_alert_grace_until: float = time.monotonic() + 1800.0
        # Per-cycle OOS gate summary
        self._oos_gate_counts: dict[str, int] = {}

        # Sprint 58: per-ticker OOS override — bypass stat gate for specific (ticker, action) pairs.
        # Env: OOS_TICKER_OVERRIDE=AVAX:confirm,AAVE:confirm (comma-separated ticker:action pairs)
        _override_raw = os.getenv("OOS_TICKER_OVERRIDE", "")
        self.oos_ticker_overrides: frozenset[tuple[str, str]] = frozenset(
            tuple(item.split(":", 1)) for item in _override_raw.split(",") if ":" in item  # type: ignore[misc]
        )

        self._lift: dict[tuple[str, str, str], tuple[float, int]] = {}
        self._refreshed_at: float = 0.0

    # ── stats refresh ─────────────────────────────────────────────────────────

    async def maybe_refresh(self, pool: asyncpg.Pool, redis=None) -> None:
        """Recompute lift table when due. Called from the trader poll cycle.

        Also reloads the OOS guardrail table (router:oos_stats) from Redis every
        cycle — it's tiny and written by a separate background task on its own
        cadence; the router just needs the latest snapshot.
        """
        if not self.enabled:
            return
        await self._load_oos_stats(redis)
        if time.monotonic() - self._refreshed_at < self.refresh_interval_s and self._lift:
            return
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    _STATS_SQL, self.window_h, str(self.lookback_days), _FLAT_BAND
                )
        except Exception as exc:
            logger.warning("router_refresh_failed", error=str(exc))
            return

        base: dict[str, tuple[int, int]] = {}
        per_key: dict[tuple[str, str, str], tuple[int, int]] = {}
        for r in rows:
            ups, downs = int(r["ups"]), int(r["downs"])
            per_key[(r["engine_name"], r["bucket"], r["exch"])] = (ups, downs)
            bu, bd = base.get(r["exch"], (0, 0))
            base[r["exch"]] = (bu + ups, bd + downs)

        lift_table: dict[tuple[str, str, str], tuple[float, int]] = {}
        for key, (ups, downs) in per_key.items():
            n = ups + downs
            base_ups, base_downs = base[key[2]]
            base_n = base_ups + base_downs
            if n < self.min_n or base_n <= 0:
                continue
            p_up = ups / n
            base_p = base_ups / base_n
            shrunk = (p_up - base_p) * (n / (n + self.shrink_k))
            lift_table[key] = (shrunk, n)

        self._lift = lift_table
        self._refreshed_at = time.monotonic()
        logger.info(
            "router_stats_refreshed",
            buckets=len(lift_table),
            window_h=self.window_h,
            top={f"{k[0]}/{k[1]}/{k[2]}": round(v[0], 3) for k, v in
                 sorted(lift_table.items(), key=lambda kv: -abs(kv[1][0]))[:5]},
        )

    async def _load_oos_stats(self, redis) -> None:
        """Load router:oos_stats from Redis into in-memory 7d/14d tables.

        Defensive: any failure leaves the previous snapshot in place and is
        logged once. Staleness is judged at gate time off `computed_at`.
        """
        if redis is None:
            return
        try:
            raw = await redis.get(_OOS_STATS_KEY)
        except Exception as exc:
            logger.warning("oos_stats_load_failed", error=str(exc))
            return
        if not raw:
            # Missing key — mark unloaded so the gate treats verdicts as stale.
            self._oos_loaded = False
            return
        try:
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode()
            data = json.loads(raw)
            windows = data.get("windows", {})
            self._oos_14d = windows.get("14d", {}) or {}
            self._oos_7d = windows.get("7d", {}) or {}
            ca = data.get("computed_at")
            if ca:
                from datetime import datetime as _dt
                self._oos_computed_at = _dt.fromisoformat(ca).timestamp()
            self._oos_loaded = True
        except Exception as exc:
            logger.warning("oos_stats_parse_failed", error=str(exc))

    def _oos_is_stale(self) -> bool:
        """True when OOS stats are missing or older than the staleness budget."""
        if not self._oos_loaded or self._oos_computed_at <= 0.0:
            return True
        return (time.time() - self._oos_computed_at) > self.oos_max_staleness_s

    def _oos_tradeable(self, engine: str, bucket: str, exch: str, action: str, ticker: str = "") -> bool:
        """Apply the OOS-tradeable rule for one (engine, bucket, exch, action).

        14d avg_net > 0 with n >= oos_min_n, AND 7d avg_net > 0 when the 7d
        window has n >= 10 (if 7d n < 10, the 14d result alone decides).

        Sprint 58: if (ticker, action) is in oos_ticker_overrides, bypass stat gate
        entirely — stale-data and kill-switch checks (before this call) still apply.
        """
        if ticker and (ticker, action) in self.oos_ticker_overrides:
            logger.info("oos_ticker_override_applied", ticker=ticker, action=action)
            return True
        seg_key = f"{engine}|{bucket}|{exch}|{action}"
        s14 = self._oos_14d.get(seg_key)
        if s14 is None:
            return False
        if s14.get("n", 0) < self.oos_min_n or s14.get("avg_net", 0.0) <= 0.0:
            return False
        s7 = self._oos_7d.get(seg_key)
        if s7 is not None and s7.get("n", 0) >= 10 and s7.get("avg_net", 0.0) <= 0.0:
            return False
        return True

    def _oos_stale_notify(self) -> None:
        """Log loudly (≤1×/hr) and fire a one-time Discord alert on stale OOS."""
        now = time.monotonic()
        if now - self._oos_last_stale_log > 3600:
            logger.warning(
                "oos_stats_stale",
                computed_at=self._oos_computed_at,
                age_s=round(time.time() - self._oos_computed_at, 1)
                if self._oos_computed_at > 0 else None,
                loaded=self._oos_loaded,
                action="rejecting all confirm/invert verdicts",
            )
            self._oos_last_stale_log = now
        if now < self._oos_alert_grace_until:
            return
        if not self._oos_stale_alerted and self._notifier is not None:
            try:
                import asyncio
                msg = (
                    "🚨 Router OOS guardrail STALE — router:oos_stats missing or "
                    f">{self.oos_max_staleness_s // 3600}h old. Rejecting all "
                    "confirm/invert verdicts (no evidence, no trade) until refreshed."
                )
                task = asyncio.get_running_loop().create_task(
                    self._notifier.send_message(msg)
                )

                def _on_done(t: "asyncio.Task") -> None:
                    exc = t.exception()
                    if exc is None:
                        self._oos_stale_alerted = True
                    else:
                        logger.warning("oos_stale_alert_failed", error=str(exc))

                task.add_done_callback(_on_done)
            except Exception as exc:
                logger.warning("oos_stale_alert_failed", error=str(exc))

    def log_oos_cycle_summary(self) -> None:
        """Emit + reset the per-cycle OOS gate counts. Called by the trader."""
        if self._oos_gate_counts:
            logger.info("oos_gate_summary", **self._oos_gate_counts)
            self._oos_gate_counts = {}

    def has_stats(self, engine: str) -> bool:
        """True when the router has at least one measured bucket for this engine.

        Used to override dynamic engine exclusions: an engine the router has
        measured — even one with negative lift — is exploitable evidence and
        must reach the router. Pillar-2 lifecycle exclusion judges engines by
        their own accuracy, which is exactly the mistake the router corrects.
        """
        if not self.enabled or not self._lift:
            return False
        if time.monotonic() - self._refreshed_at > self.max_staleness_s:
            return False
        return any(k[0] == engine for k in self._lift)

    # ── routing ───────────────────────────────────────────────────────────────

    @staticmethod
    def _bucket(raw_direction: str | None, composite_score: float) -> str | None:
        if raw_direction in _POSITIVE_DIRS:
            return "emit_long"
        if raw_direction in _NEGATIVE_DIRS:
            return "emit_short"
        if composite_score >= 0.10:
            return "score_long"
        if composite_score <= -0.10:
            return "score_short"
        return None

    def route(
        self,
        engine: str,
        exchange: str,
        raw_direction: str | None,
        composite_score: float,
        nominal_direction: str,
        ticker: str = "",
    ) -> RouteVerdict:
        """Decide what to do with one signal. Pure lookup — no I/O."""
        verdict = self._route(engine, exchange, raw_direction, composite_score, nominal_direction, ticker=ticker)
        router_verdicts_total.labels(action=verdict.action).inc()
        return verdict

    def _route(
        self,
        engine: str,
        exchange: str,
        raw_direction: str | None,
        composite_score: float,
        nominal_direction: str,
        ticker: str = "",
    ) -> RouteVerdict:
        if not self.enabled:
            return _PASSTHROUGH
        if time.monotonic() - self._refreshed_at > self.max_staleness_s:
            return _PASSTHROUGH

        # OOS ticker override: must be checked BEFORE bucket/lift-table — assets with
        # scores in the -0.10..+0.10 dead zone produce bucket=None and would get
        # _PASSTHROUGH before the override fires (Sprint 71c bug).
        if ticker:
            _t = ticker.upper()
            for _ov_action in ("confirm", "invert"):
                if (_t, _ov_action) in self.oos_ticker_overrides:
                    _opp = "short" if nominal_direction == "long" else "long"
                    _trade_dir = nominal_direction if _ov_action == "confirm" else _opp
                    _exch_ov = "CRYPTO" if (exchange or "").upper() == "CRYPTO" else "OTHER"
                    _bucket_ov = self._bucket(raw_direction, composite_score)
                    if _bucket_ov is None:
                        # Dead-zone in bucket classifier but ticker is admin-approved —
                        # honour the override with minimal edge (engine already accepted it).
                        logger.info(
                            "oos_ticker_override_dead_zone",
                            ticker=_t,
                            action=_ov_action,
                            direction=_trade_dir,
                            composite_score=round(composite_score, 4),
                        )
                        return RouteVerdict(_ov_action, _trade_dir, 0.01, 0, 1.0)
                    _stats_ov = self._lift.get((engine, _bucket_ov, _exch_ov))
                    if _stats_ov is None:
                        # No lift data for this bucket — use minimal edge. The ticker
                        # override is explicitly admin-approved (OOS_TICKER_OVERRIDE),
                        # so we don't require router lift evidence for it.
                        _real_edge = 0.01
                    else:
                        _lift_ov, _ = _stats_ov
                        _edge_ov = _lift_ov if nominal_direction == "long" else -_lift_ov
                        if _ov_action == "invert":
                            _edge_ov = -_lift_ov if nominal_direction == "long" else _lift_ov
                        _real_edge = max(0.01, _edge_ov)
                    logger.info(
                        "oos_ticker_override_early",
                        ticker=_t,
                        action=_ov_action,
                        direction=_trade_dir,
                        edge=round(_real_edge, 4),
                    )
                    return RouteVerdict(_ov_action, _trade_dir, _real_edge, 0, 1.0 + min(0.30, _real_edge * 1.5))

        bucket = self._bucket(raw_direction, composite_score)
        if bucket is None:
            return _PASSTHROUGH

        exch = "CRYPTO" if (exchange or "").upper() == "CRYPTO" else "OTHER"

        stats = self._lift.get((engine, bucket, exch))
        if stats is None:
            return _PASSTHROUGH

        lift, n = stats  # lift > 0 ⇒ evidence for LONG, < 0 ⇒ evidence for SHORT
        edge_for_nominal = lift if nominal_direction == "long" else -lift

        # ── OOS guardrail pre-check ───────────────────────────────────────────
        # When the gate is on, a confirm-long segment that the OOS bench has
        # PROVEN tradeable may confirm at min_edge instead of the wide static
        # confirm_long_edge — the proof supersedes the static suppression.
        gate_on = self.oos_gate_enabled
        if gate_on and nominal_direction == "long":
            if self._oos_tradeable(engine, bucket, exch, "confirm", ticker=ticker):
                confirm_bar = self.oos_long_min_edge
            else:
                confirm_bar = self.confirm_long_edge
        elif nominal_direction == "long":
            confirm_bar = self.confirm_long_edge
        else:
            confirm_bar = self.min_edge

        if edge_for_nominal >= confirm_bar:
            verdict = RouteVerdict(
                "confirm", nominal_direction, edge_for_nominal, n,
                1.0 + min(0.30, edge_for_nominal * 1.5),
            )
            return self._apply_oos_gate(verdict, engine, bucket, exch, "confirm", ticker=ticker)

        opposite = "short" if nominal_direction == "long" else "long"
        edge_for_opposite = -edge_for_nominal
        if (
            self.allow_invert
            and edge_for_opposite >= self.invert_min_edge
            and n >= self.invert_min_n
        ):
            verdict = RouteVerdict(
                "invert", opposite, edge_for_opposite, n,
                1.0 + min(0.30, edge_for_opposite * 1.5),
            )
            return self._apply_oos_gate(verdict, engine, bucket, exch, "invert", ticker=ticker)

        # ── OOS-bridge: promote near-zero live edge to confirm when OOS proves it ──
        # Live lift can be neutral/noisy while OOS walk-forward shows the segment
        # was profitable over 14d. Gate tightly: live edge must be > -0.02 (not
        # actively anti-predictive), OOS not stale, and 7d must also pass.
        # This bridges the router-window vs walk-forward population mismatch.
        # Long bridge must also clear oos_long_min_edge (same bar as the main
        # confirm path) — SPY/GLD confirmed at 0.0987 despite 0.12 floor otherwise.
        if (
            not self._oos_is_stale()
            and edge_for_nominal >= -0.02   # not actively anti-predictive
            and (nominal_direction != "long" or edge_for_nominal >= self.oos_long_min_edge)
            and self._oos_tradeable(engine, bucket, exch, "confirm", ticker=ticker)
        ):
            logger.info(
                "oos_bridge_confirm",
                engine=engine, bucket=bucket, exch=exch,
                live_edge=round(edge_for_nominal, 3),
            )
            self._oos_gate_counts["pass_oos_bridge"] = (
                self._oos_gate_counts.get("pass_oos_bridge", 0) + 1)
            return RouteVerdict("confirm", nominal_direction, max(0.01, edge_for_nominal), n, 1.0)

        # ── OOS-bridge invert: promote to invert when OOS proves the fade edge ──
        # Same logic as confirm bridge but for invert segments. Catches cases where
        # live 24h lift is insufficient for invert threshold but 14d+7d walk-forward
        # proves the opposite direction is profitable (e.g. TDE short → fade long).
        # Requires live edge for the OPPOSITE direction to be > -0.02 (not anti-predictive).
        if (
            self.allow_invert
            and not self._oos_is_stale()
            and edge_for_opposite >= -0.02   # opposite direction not anti-predictive in live
            and self._oos_tradeable(engine, bucket, exch, "invert", ticker=ticker)
        ):
            logger.info(
                "oos_bridge_invert",
                engine=engine, bucket=bucket, exch=exch,
                live_edge_opposite=round(edge_for_opposite, 3),
            )
            self._oos_gate_counts["pass_oos_bridge_invert"] = (
                self._oos_gate_counts.get("pass_oos_bridge_invert", 0) + 1)
            return RouteVerdict("invert", opposite, max(0.01, edge_for_opposite), n, 1.0)

        return RouteVerdict("reject", nominal_direction, edge_for_nominal, n, 1.0)

    def lift_lookup(self, engine: str, bucket: str | None, exch: str) -> tuple[float, int] | None:
        """Public read of shrunk-lift table for cluster aggregation."""
        if not self.enabled or not self._lift:
            return None
        if time.monotonic() - self._refreshed_at > self.max_staleness_s:
            return None
        if bucket is None:
            return None
        return self._lift.get((engine, bucket, exch))

    def bucket_for(self, raw_direction: str | None, composite_score: float) -> str | None:
        """Public wrapper over _bucket classifier."""
        return self._bucket(raw_direction, composite_score)

    def _apply_oos_gate(self, verdict: "RouteVerdict", engine: str, bucket: str,
                        exch: str, action: str, ticker: str = "") -> "RouteVerdict":
        """Downgrade a confirm/invert verdict to reject unless OOS-proven.

        Rule (when OOS_GATE_ENABLED): the segment must be OOS-tradeable —
        14d avg_net > 0 with n >= OOS_MIN_N, AND 7d avg_net > 0 when 7d n >= 10.
        Stale/missing OOS stats ⇒ ALL confirm/invert rejected (no proof, no trade).
        """
        if not self.oos_gate_enabled:
            return verdict
        if self._oos_is_stale():
            self._oos_stale_notify()
            self._oos_gate_counts["reject_stale"] = (
                self._oos_gate_counts.get("reject_stale", 0) + 1)
            return RouteVerdict("reject", verdict.direction, verdict.edge, verdict.n, 1.0)
        if self._oos_tradeable(engine, bucket, exch, action, ticker=ticker):
            self._oos_gate_counts[f"pass_{action}"] = (
                self._oos_gate_counts.get(f"pass_{action}", 0) + 1)
            return verdict
        self._oos_gate_counts[f"reject_{action}"] = (
            self._oos_gate_counts.get(f"reject_{action}", 0) + 1)
        return RouteVerdict("reject", verdict.direction, verdict.edge, verdict.n, 1.0)
