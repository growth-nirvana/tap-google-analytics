"""Retry and throttle helpers for GA4 Data API quota limits."""

from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timedelta
from typing import Callable, TypeVar

from tap_google_analytics.error import classify_quota_error, is_quota_error

LOGGER = logging.getLogger(__name__)

T = TypeVar("T")

# Extra seconds past the hour boundary before retrying hourly-token errors.
_HOUR_RESET_BUFFER_SECONDS = 5.0
# Cap exponential backoff for concurrent / rate-limit errors.
_MAX_SHORT_BACKOFF_SECONDS = 60.0
_MAX_SHORT_RETRIES = 8


class QuotaManager:
    """Throttle GA4 requests and retry when property/project tokens are exhausted."""

    def __init__(
        self,
        min_request_interval: float = 0.5,
        max_hourly_retries: int = 24,
    ) -> None:
        self.min_request_interval = max(float(min_request_interval), 0.0)
        self.max_hourly_retries = int(max_hourly_retries)
        self._last_request_at: float | None = None

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
        **kwargs,
    ) -> T:
        """Invoke ``func`` with throttling and quota-aware retries.

        Hourly token errors wait until the next clock hour.
        Concurrent / rate-limit errors use exponential backoff.
        Daily token errors are not retried (waiting will not recover them).
        """
        hourly_retries = (
            self.max_hourly_retries if max_hourly_retries is None else int(max_hourly_retries)
        )
        hourly_attempts = 0
        short_attempts = 0

        while True:
            self.throttle()
            try:
                result = func(*args, **kwargs)
            except Exception as exc:
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

            self.observe_response(result)
            return result
