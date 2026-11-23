"""Retry and throttle helpers for GA4 Data API quota limits."""

from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timedelta
from typing import Callable, TypeVar

from tap_google_analytics.error import (
    classify_quota_error,
    describe_error,
    is_quota_error,
    is_transient_error,
)

LOGGER = logging.getLogger(__name__)

T = TypeVar("T")

# Extra seconds past the hour boundary before retrying hourly-token errors.
_HOUR_RESET_BUFFER_SECONDS = 5.0
# Cap exponential backoff for concurrent / rate-limit errors.
_MAX_SHORT_BACKOFF_SECONDS = 60.0
_MAX_SHORT_RETRIES = 8
# Server errors and dropped connections (503 UNAVAILABLE, connection reset, 5xx, timeouts):
# exponential backoff from 2s, capped at 120s, with jitter. 8 retries wait about 4-8 minutes
# in all before the request is given up.
_TRANSIENT_BASE_SECONDS = 2.0
_MAX_TRANSIENT_BACKOFF_SECONDS = 120.0
DEFAULT_TRANSIENT_RETRIES = 8


class QuotaManager:
    """Throttle GA4 requests and retry when property/project tokens are exhausted."""

    def __init__(
        self,
        min_request_interval: float = 0.5,
        max_hourly_retries: int = 24,
        max_transient_retries: int = DEFAULT_TRANSIENT_RETRIES,
    ) -> None:
        self.min_request_interval = max(float(min_request_interval), 0.0)
        self.max_hourly_retries = int(max_hourly_retries)
        self.max_transient_retries = max(int(max_transient_retries), 0)
        self._last_request_at: float | None = None
        # The last call's attempts (1 = no retry) and seconds, for the caller's failure report.
        self.last_attempts = 0
        self.last_seconds = 0.0

    @staticmethod
    def seconds_until_next_hour(now: datetime | None = None) -> float:
        """Seconds until the next clock hour, plus a small reset buffer."""
        current = now or datetime.now()
        next_hour = current.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        wait_seconds = (next_hour - current).total_seconds() + _HOUR_RESET_BUFFER_SECONDS
        return max(wait_seconds, 1.0)

    def throttle(self) -> None:
        """Sleep so consecutive API calls respect ``min_request_interval``."""
        if self.min_request_interval <= 0:
            self._last_request_at = time.monotonic()
            return

        now = time.monotonic()
        if self._last_request_at is not None:
            elapsed = now - self._last_request_at
            remaining = self.min_request_interval - elapsed
            if remaining > 0:
                LOGGER.debug("Throttling GA4 request for %.2fs", remaining)
                time.sleep(remaining)
        self._last_request_at = time.monotonic()

    def wait_for_quota_reset(self) -> None:
        """Wait until the next hour when hourly GA4 quota tokens typically reset."""
        wait_seconds = self.seconds_until_next_hour()
        # Spread retries so many tenants sharing a GCP project do not stampede.
        wait_seconds += random.uniform(0, 10)
        LOGGER.info(
            "GA4 API hourly quota exhausted. Waiting %.0fs until the next hour before retrying.",
            wait_seconds,
        )
        time.sleep(wait_seconds)

    def wait_exponential(self, attempt: int) -> None:
        """Sleep with exponential backoff and jitter for transient 429s."""
        delay = min(2 ** attempt, _MAX_SHORT_BACKOFF_SECONDS) + random.uniform(0, 1)
        LOGGER.info(
            "GA4 API rate limited (attempt %s). Backing off for %.1fs.",
            attempt,
            delay,
        )
        time.sleep(delay)

    @staticmethod
    def transient_delay(attempt: int) -> float:
        """Return the seconds to wait before transient retry ``attempt`` (1-based).

        Exponential with jitter: half fixed, half random, so taps do not retry in step.
        """
        ceiling = min(_TRANSIENT_BASE_SECONDS * 2 ** (attempt - 1), _MAX_TRANSIENT_BACKOFF_SECONDS)
        return ceiling / 2 + random.uniform(0, ceiling / 2)  # noqa: S311 - jitter, not crypto

    def wait_transient(self, attempt: int, error: BaseException, label: str | None) -> None:
        """Sleep before retrying a request Google failed or whose connection dropped."""
        delay = self.transient_delay(attempt)
        found = describe_error(error)
        LOGGER.warning(
            "GA4 request failed (%s%s%s)%s; retry %s of %s in %.1fs: %s",
            found["cause"],
            f", {found['status']}" if found["status"] else "",
            f" {found['grpc_status']}" if found["grpc_status"] else "",
            f" for {label}" if label else "",
            attempt,
            self.max_transient_retries,
            delay,
            error,
        )
        time.sleep(delay)

    def observe_response(self, response) -> None:
        """Slow down further when the API reports low remaining quota tokens."""
        quota = getattr(response, "property_quota", None)
        if quota is None:
            return

        tokens = getattr(quota, "tokens_per_project_per_hour", None)
        if tokens is None:
            tokens = getattr(quota, "tokens_per_hour", None)

        if tokens is None:
            return

        remaining = getattr(tokens, "remaining", None)
        consumed = getattr(tokens, "consumed", None)
        try:
            remaining = int(remaining)
        except (TypeError, ValueError):
            return
        try:
            consumed = int(consumed)
        except (TypeError, ValueError):
            consumed = 0

        # Unset protobuf quota messages default to 0/0 — ignore those.
        if remaining == 0 and consumed == 0:
            return

        if remaining <= 20:
            extra = 30.0
        elif remaining <= 100:
            extra = 5.0
        elif remaining <= 300:
            extra = 2.0
        else:
            extra = 0.0

        if extra:
            LOGGER.warning(
                "GA4 quota tokens remaining=%s; adding %.1fs delay before the next request.",
                remaining,
                extra,
            )
            time.sleep(extra)

    def call(
        self,
        func: Callable[..., T],
        *args,
        max_hourly_retries: int | None = None,
        label: str | None = None,
        **kwargs,
    ) -> T:
        """Invoke ``func`` with throttling and quota-aware retries.

        Hourly token errors wait until the next clock hour.
        Concurrent / rate-limit errors use exponential backoff.
        Daily token errors are not retried (waiting will not recover them).
        Server errors and dropped connections (5xx, gRPC UNAVAILABLE / DEADLINE_EXCEEDED /
        INTERNAL, connection reset, timeouts) are retried with exponential backoff and jitter,
        up to ``max_transient_retries`` times. ``label`` names the request in the logs.
        ``last_attempts`` / ``last_seconds`` say how the call went, also when it raises.
        """
        hourly_retries = (
            self.max_hourly_retries if max_hourly_retries is None else int(max_hourly_retries)
        )
        hourly_attempts = 0
        short_attempts = 0
        transient_attempts = 0
        started = time.monotonic()
        self.last_attempts = 0
        self.last_seconds = 0.0

        while True:
            self.throttle()
            self.last_attempts += 1
            try:
                result = func(*args, **kwargs)
            except Exception as exc:
                self.last_seconds = time.monotonic() - started
                if is_transient_error(exc):
                    transient_attempts += 1
                    if transient_attempts > self.max_transient_retries:
                        raise
                    self.wait_transient(transient_attempts, exc, label)
                    continue
                if not is_quota_error(exc):
                    raise

                kind = classify_quota_error(exc)
                LOGGER.warning("GA4 quota error (%s): %s", kind, exc)

                if kind == "daily_tokens":
                    raise

                if kind == "hourly_tokens":
                    hourly_attempts += 1
                    if hourly_attempts > hourly_retries:
                        raise
                    self.wait_for_quota_reset()
                    continue

                short_attempts += 1
                if short_attempts > _MAX_SHORT_RETRIES:
                    raise
                self.wait_exponential(short_attempts)
                continue

            self.last_seconds = time.monotonic() - started
            self.observe_response(result)
            return result
