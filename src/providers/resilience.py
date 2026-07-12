"""
Provider resilience primitives: circuit breaker, retry with exponential backoff,
and latency logging. Applied as a mixin — no existing providers need modification.

Usage:
    class MyProvider(ResilientProviderMixin, BaseProvider):
        provider_name = "myprovider"

        async def fetch(self, **kwargs):
            async with self.latency_context("fetch"):
                return await self._with_retry(self._do_fetch, **kwargs)
"""
from __future__ import annotations

import asyncio
import random
import time
from contextlib import asynccontextmanager
from enum import Enum
from typing import Any, Callable, TypeVar

from src.core.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


# ── Circuit Breaker ────────────────────────────────────────────────────────────

class CircuitState(str, Enum):
    CLOSED = "closed"       # normal operation
    OPEN = "open"           # failing, reject calls fast
    HALF_OPEN = "half_open" # testing recovery


class CircuitBreaker:
    """
    Per-provider in-memory circuit breaker.
    CLOSED → OPEN after `failure_threshold` consecutive failures.
    OPEN → HALF_OPEN after `recovery_timeout` seconds.
    HALF_OPEN → CLOSED after `success_threshold` consecutive successes.
    HALF_OPEN → OPEN on any failure.
    """

    def __init__(
        self,
        provider_name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        success_threshold: int = 2,
    ) -> None:
        self.provider_name = provider_name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._opened_at: float = 0.0

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            if time.monotonic() - self._opened_at >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0
                logger.info(
                    "circuit_half_open",
                    provider=self.provider_name,
                    recovery_timeout=self.recovery_timeout,
                )
        return self._state

    def is_open(self) -> bool:
        return self.state == CircuitState.OPEN

    def record_success(self) -> None:
        state = self.state
        if state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.success_threshold:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                logger.info("circuit_closed", provider=self.provider_name)
        elif state == CircuitState.CLOSED:
            self._failure_count = 0

    def record_failure(self) -> None:
        state = self.state
        if state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            self._opened_at = time.monotonic()
            logger.warning(
                "circuit_reopened",
                provider=self.provider_name,
                reason="failure_in_half_open",
            )
        elif state == CircuitState.CLOSED:
            self._failure_count += 1
            if self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
                self._opened_at = time.monotonic()
                logger.error(
                    "circuit_opened",
                    provider=self.provider_name,
                    failure_count=self._failure_count,
                    recovery_timeout=self.recovery_timeout,
                )

    def get_status(self) -> dict:
        return {
            "provider": self.provider_name,
            "state": self.state.value,
            "failure_count": self._failure_count,
        }


# ── Global registry: one circuit breaker per provider ────────────────────────
_circuit_breakers: dict[str, CircuitBreaker] = {}


def get_circuit_breaker(provider_name: str) -> CircuitBreaker:
    if provider_name not in _circuit_breakers:
        _circuit_breakers[provider_name] = CircuitBreaker(provider_name)
    return _circuit_breakers[provider_name]


def get_all_circuit_statuses() -> list[dict]:
    return [cb.get_status() for cb in _circuit_breakers.values()]


# ── Retry with exponential backoff ────────────────────────────────────────────

class CircuitOpenError(RuntimeError):
    """Raised when a provider call is blocked by an open circuit."""


async def retry_with_backoff(
    func: Callable[..., Any],
    *args: Any,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0,
    jitter: bool = True,
    provider_name: str = "unknown",
    circuit_breaker: CircuitBreaker | None = None,
    **kwargs: Any,
) -> Any:
    """
    Execute `func(*args, **kwargs)` with exponential backoff retry.
    Integrates with circuit breaker: records success/failure on each attempt.
    """
    cb = circuit_breaker or (get_circuit_breaker(provider_name) if provider_name != "unknown" else None)

    if cb and cb.is_open():
        logger.warning(
            "circuit_blocked",
            provider=provider_name,
            state=cb.state.value,
        )
        raise CircuitOpenError(f"Circuit open for provider '{provider_name}' — skipping")

    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            result = await func(*args, **kwargs)
            if cb:
                cb.record_success()
            return result
        except CircuitOpenError:
            raise
        except Exception as exc:
            last_exc = exc
            if cb:
                cb.record_failure()

            if attempt == max_attempts:
                logger.error(
                    "retry_exhausted",
                    provider=provider_name,
                    attempts=max_attempts,
                    error=str(exc),
                )
                break

            delay = min(base_delay * (backoff_factor ** (attempt - 1)), max_delay)
            if jitter:
                delay *= (0.5 + random.random() * 0.5)

            logger.warning(
                "retry_attempt",
                provider=provider_name,
                attempt=attempt,
                max_attempts=max_attempts,
                delay_s=round(delay, 2),
                error=str(exc),
            )
            await asyncio.sleep(delay)

    raise last_exc  # type: ignore[misc]


# ── Latency logging context ────────────────────────────────────────────────────

@asynccontextmanager
async def latency_context(provider_name: str, operation: str):
    """
    Async context manager that logs structured latency for any provider operation.

    Usage:
        async with latency_context("newsapi", "fetch"):
            items = await session.get(...)
    """
    start = time.monotonic()
    try:
        yield
        elapsed_ms = (time.monotonic() - start) * 1000
        logger.info(
            "provider_latency",
            provider=provider_name,
            operation=operation,
            latency_ms=round(elapsed_ms, 2),
            status="ok",
        )
    except Exception as exc:
        elapsed_ms = (time.monotonic() - start) * 1000
        logger.warning(
            "provider_latency",
            provider=provider_name,
            operation=operation,
            latency_ms=round(elapsed_ms, 2),
            status="error",
            error=str(exc),
        )
        raise


# ── Mixin ─────────────────────────────────────────────────────────────────────

class ResilientProviderMixin:
    """
    Drop-in mixin for BaseProvider subclasses.
    Adds: circuit breaker, retry with backoff, latency logging.

    Provider subclasses call self._call_with_resilience(coro_func, *args) instead
    of calling the coroutine directly for any HTTP operation.
    """

    @property
    def _circuit_breaker(self) -> CircuitBreaker:
        return get_circuit_breaker(getattr(self, "provider_name", "unknown"))

    async def _call_with_resilience(
        self,
        func: Callable[..., Any],
        *args: Any,
        operation: str = "fetch",
        max_attempts: int = 3,
        base_delay: float = 1.0,
        **kwargs: Any,
    ) -> Any:
        pname = getattr(self, "provider_name", "unknown")
        async with latency_context(pname, operation):
            return await retry_with_backoff(
                func,
                *args,
                max_attempts=max_attempts,
                base_delay=base_delay,
                provider_name=pname,
                circuit_breaker=self._circuit_breaker,
                **kwargs,
            )
