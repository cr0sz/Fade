#!/usr/bin/env python3
"""
Pipeline Report — end-to-end verification of the market intelligence pipeline.

Connects to PostgreSQL and checks each stage of the pipeline:
  Stage 1  Provider activity    — did providers ingest data recently?
  Stage 2  Signal generation    — are engines producing signals?
  Stage 3  Signal evaluation    — are outcomes being recorded?
  Stage 4  Feedback records     — are performance metrics being written?
  Stage 5  Pipeline integrity   — do any cycle_ids span all three stages?

Exit codes:
  0  PASS — pipeline operational
  1  FAIL — pipeline broken or DB unreachable

Usage (from project root):
  python scripts/pipeline_report.py

  POSTGRES_HOST defaults to "localhost"; override if needed:
  POSTGRES_HOST=myhost python scripts/pipeline_report.py

Environment variables read (all optional, fall back to project defaults):
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

# ── Load .env from project root (best-effort) ─────────────────────────────────
try:
    from dotenv import dotenv_values

    _env_path = Path(__file__).resolve().parent.parent / ".env"
    if _env_path.exists():
        for _k, _v in dotenv_values(_env_path).items():
            os.environ.setdefault(_k, _v)
except ImportError:
    pass  # python-dotenv is in requirements.txt but may not be installed locally

import asyncpg  # noqa: E402  (after dotenv load)


# ── DB connection string ───────────────────────────────────────────────────────

def _dsn() -> str:
    host = os.getenv("POSTGRES_HOST", "localhost")
    # "postgres" is the in-Docker hostname; substitute localhost for host runs.
    if host == "postgres":
        host = "localhost"
    return (
        f"postgresql://{os.getenv('POSTGRES_USER', 'mktintel')}"
        f":{os.getenv('POSTGRES_PASSWORD', '')}"
        f"@{host}:{os.getenv('POSTGRES_PORT', '5432')}"
        f"/{os.getenv('POSTGRES_DB', 'mktintel')}"
    )


# ── Report formatting ─────────────────────────────────────────────────────────

_COL = 32  # label column width for dot-aligned output

GREEN  = "\033[0;32m"
RED    = "\033[0;31m"
YELLOW = "\033[0;33m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

_USE_COLOUR = sys.stdout.isatty()


def _c(code: str, text: str) -> str:
    return f"{code}{text}{RESET}" if _USE_COLOUR else text


def _row(label: str, value: str, warn: bool = False, fail: bool = False) -> str:
    dots = "." * max(2, _COL - len(label))
    suffix = ""
    if fail:
        suffix = "  " + _c(RED, "[FAIL]")
    elif warn:
        suffix = "  " + _c(YELLOW, "[WARN]")
    return f"  {label} {dots} {value}{suffix}"


def _section(title: str) -> None:
    print(f"\n{_c(BOLD, title + ':')}")


# ── Per-stage checks ──────────────────────────────────────────────────────────

async def _check_providers(conn: asyncpg.Connection) -> tuple[bool, list[str]]:
    """Stage 1: provider_ingestion_log — active providers in last 2 hours."""
    rows = await conn.fetch(
        """
        SELECT
            provider,
            SUM(fetched)               AS fetched,
            SUM(errors)                AS errors,
            MAX(status)                AS last_status,
            MAX(ts)                    AS last_seen
        FROM provider_ingestion_log
        WHERE ts > now() - interval '2 hours'
        GROUP BY provider
        ORDER BY provider
        """
    )
    lines: list[str] = []
    any_ok = False
    for r in rows:
        name      = r["provider"]
        fetched   = int(r["fetched"] or 0)
        errors    = int(r["errors"]  or 0)
        status    = str(r["last_status"] or "")
        is_fail   = status == "failed" or (errors > 0 and fetched == 0)
        is_warn   = errors > 0 and fetched > 0
        label     = f"{fetched} fetched"
        if status == "circuit_open":
            label = "circuit_open"
            is_warn = True
        lines.append(_row(name, label, warn=is_warn, fail=is_fail))
        if not is_fail:
            any_ok = True

    if not rows:
        lines.append(_row("(none active)", "0", warn=True))

    return any_ok, lines


async def _check_signals(conn: asyncpg.Connection) -> tuple[bool, list[str]]:
    """Stage 2: signal_trace — signals generated in last 24 hours."""
    rows = await conn.fetch(
        """
        SELECT signal_type, COUNT(*) AS cnt
        FROM signal_trace
        WHERE created_at > now() - interval '24 hours'
        GROUP BY signal_type
        ORDER BY signal_type
        """
    )
    lines: list[str] = []
    total = 0
    for r in rows:
        cnt = int(r["cnt"])
        total += cnt
        lines.append(_row(r["signal_type"].title(), str(cnt)))

    if not rows:
        lines.append(_row("(no signals)", "0", warn=True))

    return total > 0, lines


async def _check_evaluations(conn: asyncpg.Connection) -> tuple[bool, list[str]]:
    """Stage 3: signal_feedback — completed outcomes."""
    row = await conn.fetchrow(
        """
        SELECT
            COUNT(*) FILTER (WHERE outcome_type != 'pending') AS completed,
            COUNT(*) FILTER (WHERE outcome_type = 'pending')  AS pending
        FROM signal_feedback
        """
    )
    completed = int(row["completed"] or 0)
    pending   = int(row["pending"]   or 0)
    lines = [
        _row("Completed", str(completed)),
        _row("Pending",   str(pending)),
    ]
    # Not a failure — evaluations are time-delayed; 0 is normal on cold start
    return True, lines


async def _check_feedback(conn: asyncpg.Connection) -> tuple[bool, list[str]]:
    """Stage 4: signal_feedback outcomes written in last 7 days."""
    count = await conn.fetchval(
        """
        SELECT COUNT(*)
        FROM signal_feedback
        WHERE outcome_type != 'pending'
          AND evaluated_at > now() - interval '7 days'
        """
    )
    count = int(count or 0)
    return True, [_row("Generated (7d)", str(count))]


async def _check_win_rates(conn: asyncpg.Connection) -> list[str]:
    """Engine win rates from engine_performance_metrics (latest per engine)."""
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (engine_name)
            engine_name, win_rate_24h, avg_pnl_score, feedback_signals_total
        FROM engine_performance_metrics
        WHERE win_rate_24h IS NOT NULL
        ORDER BY engine_name, metric_date DESC
        """
    )
    if not rows:
        # Fall back to computing from signal_feedback directly
        rows = await conn.fetch(
            """
            SELECT
                signal_type  AS engine_name,
                ROUND(
                    100.0 * COUNT(*) FILTER (WHERE outcome_type='win')
                          / NULLIF(COUNT(*) FILTER (WHERE outcome_type IN ('win','loss')), 0)
                , 1)         AS win_rate_24h,
                NULL::float  AS avg_pnl_score,
                COUNT(*)     AS feedback_signals_total
            FROM signal_feedback
            WHERE outcome_type != 'pending'
              AND evaluated_at > now() - interval '7 days'
            GROUP BY signal_type
            ORDER BY signal_type
            """
        )

    lines: list[str] = []
    for r in rows:
        name = r["engine_name"]
        rate = r["win_rate_24h"]
        if rate is not None:
            pct = f"{float(rate):.1f}%"
            is_warn = float(rate) < 40.0
            lines.append(_row(name.replace("_", " ").title(), pct, warn=is_warn))
    if not lines:
        lines.append(_row("(no metrics yet)", "—", warn=True))
    return lines


async def _check_latest_cycle(conn: asyncpg.Connection) -> str:
    """Most recent cycle_id from provider_ingestion_log."""
    row = await conn.fetchrow(
        "SELECT cycle_id FROM provider_ingestion_log ORDER BY ts DESC LIMIT 1"
    )
    return str(row["cycle_id"]) if row else "(none)"


async def _check_pipeline_integrity(conn: asyncpg.Connection) -> tuple[bool, list[str]]:
    """Stage 5: do cycle_ids span all three pipeline stages?"""
    row = await conn.fetchrow(
        """
        WITH ingestion AS (
            SELECT DISTINCT cycle_id FROM provider_ingestion_log
            WHERE ts > now() - interval '48 hours'
        ),
        signals AS (
            SELECT DISTINCT cycle_id FROM signal_trace
            WHERE created_at > now() - interval '48 hours'
        ),
        feedback AS (
            SELECT DISTINCT cycle_id FROM signal_feedback
            WHERE evaluated_at > now() - interval '72 hours'
              AND outcome_type != 'pending'
        )
        SELECT
            COUNT(DISTINCT i.cycle_id)                   AS ingestion_count,
            COUNT(DISTINCT s.cycle_id)                   AS signal_count,
            COUNT(DISTINCT f.cycle_id)                   AS feedback_count,
            COUNT(DISTINCT i.cycle_id)
                FILTER (WHERE s.cycle_id IS NOT NULL
                           AND f.cycle_id IS NOT NULL)   AS complete_count
        FROM ingestion i
        LEFT JOIN signals  s USING (cycle_id)
        LEFT JOIN feedback f USING (cycle_id)
        """
    )
    ingestion = int(row["ingestion_count"] or 0)
    signals   = int(row["signal_count"]    or 0)
    feedback  = int(row["feedback_count"]  or 0)
    complete  = int(row["complete_count"]  or 0)

    lines = [
        _row("Ingestion cycles (48h)", str(ingestion)),
        _row("With signals",           str(signals)),
        _row("With feedback",          str(feedback)),
        _row("Fully complete",         str(complete),
             warn=(complete == 0 and signals > 0)),
    ]
    # A complete pipeline requires ingestion cycles to exist
    ok = ingestion > 0
    return ok, lines


# ── Main ──────────────────────────────────────────────────────────────────────

async def _run() -> int:
    """Run all checks and return exit code (0 = PASS, 1 = FAIL)."""
    print()
    print(_c(BOLD, "=" * 48))
    print(_c(BOLD, "PIPELINE REPORT"))
    print(_c(BOLD, "=" * 48))

    # ── Connect ───────────────────────────────────────────────────────────────
    try:
        conn = await asyncpg.connect(_dsn(), timeout=10)
    except Exception as exc:
        print(f"\n  {_c(RED, '✗')} DB connection failed: {exc}")
        print(f"\n{_c(BOLD, 'Pipeline Status:')}")
        print(f"  {_c(RED, 'FAIL')}")
        return 1

    broken: list[str] = []

    try:
        # Stage 1 — Providers
        _section("Providers")
        prov_ok, prov_lines = await _check_providers(conn)
        for line in prov_lines:
            print(line)
        if not prov_ok:
            broken.append("Providers (no active providers in last 2h)")

        # Stage 2 — Signals Generated
        _section("Signals Generated")
        sig_ok, sig_lines = await _check_signals(conn)
        for line in sig_lines:
            print(line)
        # Only fail on signals if providers were active (cascade check)
        if prov_ok and not sig_ok:
            broken.append("Signals (providers active but 0 signals in 24h)")

        # Stage 3 — Signals Evaluated
        _section("Signals Evaluated")
        _, eval_lines = await _check_evaluations(conn)
        for line in eval_lines:
            print(line)

        # Stage 4 — Feedback Records
        _section("Feedback Records")
        _, fb_lines = await _check_feedback(conn)
        for line in fb_lines:
            print(line)

        # Stage 5 — Engine Win Rates
        _section("Engine Win Rates")
        wr_lines = await _check_win_rates(conn)
        for line in wr_lines:
            print(line)

        # Pipeline integrity detail
        _section("Pipeline Integrity (48h)")
        _, integ_lines = await _check_pipeline_integrity(conn)
        for line in integ_lines:
            print(line)

        # Latest cycle
        latest = await _check_latest_cycle(conn)
        print(f"\n{_c(BOLD, 'Latest Cycle:')}")
        print(f"  {latest}")

    except Exception as exc:
        print(f"\n  {_c(RED, '✗')} Query error: {exc}")
        broken.append(f"Query error: {exc}")
    finally:
        await conn.close()

    # ── Verdict ───────────────────────────────────────────────────────────────
    status = "FAIL" if broken else "PASS"
    colour = RED if broken else GREEN
    print(f"\n{_c(BOLD, 'Pipeline Status:')}")
    print(f"  {_c(colour, status)}")
    if broken:
        for reason in broken:
            print(f"  {_c(RED, '✗')} {reason}")
    print()

    return 1 if broken else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_run()))
