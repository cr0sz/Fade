"""
Lightweight observability utilities for the market-intelligence platform.

Provides:
- timed_job: context manager for structured job lifecycle logging with timing.
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any


@contextmanager
def timed_job(log: Any, **context: Any):
    """
    Emit job_start / job_complete / job_error with duration_ms.

    Yields a mutable result dict. Populate it inside the block to include
    output fields (e.g. count) in the job_complete event.

    Re-raises any exception after emitting job_error.

    Usage::

        with timed_job(logger, table="etf_rotations") as r:
            count = await archive_table(...)
            r["count"] = count
    """
    result: dict[str, Any] = {}
    t0 = time.monotonic()
    log.info("job_start", **context)
    try:
        yield result
        log.info(
            "job_complete",
            duration_ms=round((time.monotonic() - t0) * 1000, 1),
            **context,
            **result,
        )
    except Exception:
        log.error(
            "job_error",
            duration_ms=round((time.monotonic() - t0) * 1000, 1),
            **context,
        )
        raise
