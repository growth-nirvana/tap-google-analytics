"""GoogleAnalytics error classes."""

from __future__ import annotations

import contextlib
import json
import logging
import socket

try:
    from google.api_core.exceptions import PermissionDenied as GooglePermissionDenied
    from google.api_core.exceptions import ResourceExhausted as GoogleResourceExhausted
except ImportError:
    # Fallback so no exception is ever considered PermissionDenied / quota
    class GooglePermissionDenied(Exception):
        pass

    class GoogleResourceExhausted(Exception):
        pass


class TapGaApiError(Exception):
    """Base exception for API errors."""


class TapGaInvalidArgumentError(TapGaApiError):
    """Exception for errors on the report definition."""


class TapGaAuthenticationError(TapGaApiError):
    """Exception for UNAUTHENTICATED && PERMISSION_DENIED errors."""


class TapGaRateLimitError(TapGaApiError):
    """Exception for Rate Limit errors."""


class TapGaQuotaExceededError(TapGaApiError):
    """Exception for Quota Exceeded errors."""


class TapGaBackendServerError(TapGaApiError):
    """Exception for 500 and 503 backend errors that are Google's fault."""


class TapGaUnknownError(TapGaApiError):
    """Exception for unknown errors."""


NON_FATAL_ERRORS = [
    "userRateLimitExceeded",
    "rateLimitExceeded",
    "quotaExceeded",
    "dailyQuotaExceeded",
    "internalServerError",
    "backendError",
]

QUOTA_REASONS = [
    "quotaExceeded",
    "dailyQuotaExceeded",
    "userRateLimitExceeded",
    "rateLimitExceeded",
]

# Hourly project/property token exhaustion. Waiting until the next hour helps.
HOURLY_QUOTA_MARKERS = (
    "exhausted property tokens",
    "tokens for a project per hour",
    "tokens per hour",
    "per hour",
)

RATE_LIMIT_MARKERS = (
    "userratelimitexceeded",
    "ratelimitexceeded",
    "rate limit",
    "too many requests",
    "concurrent",
)


def error_reason(e):
    """Return parsed reason from error message."""
    # For a given HttpError object from the googleapiclient package, this returns the
    # first reason code from
    # https://developers.google.com/analytics/devguides/reporting/core/v4/errors if the
    # errors HTTP response
    # body is valid json. Note that the code samples for Python on that page are
    # actually incorrect, and that
    # e.resp.reason is the HTTP transport level reason associated with the status code,
    # like "Too Many Requests"
    # for a 429 response code, whereas we want the reason field of the first error in
    # the JSON response body.

    reason = ""
    with contextlib.suppress(Exception):
        data = json.loads(e.content.decode("utf-8"))
        reason = data["error"]["errors"][0]["reason"]
    return reason


# Silence the discovery_cache errors
LOGGER = logging.getLogger("googleapiclient.discovery_cache")
LOGGER.setLevel(logging.ERROR)


def is_permission_denied_error(error):
    """Return True if error indicates lack of access to a GA property."""
    if isinstance(error, GooglePermissionDenied):
        return True
    if isinstance(error, RuntimeError):
        msg = str(error).lower()
        return "property id" in msg and "permission" in msg
    return False


def is_quota_error(error):
    """Return True if error is related to quota exhaustion or rate limiting."""
    if isinstance(error, GoogleResourceExhausted):
        return True

    reason = error_reason(error)
    error_message = str(error).lower()
    status = getattr(error, "code", None)

    return (
        reason in QUOTA_REASONS
        or status == 429
        or "exhausted property tokens" in error_message
        or "quota exceeded" in error_message
        or "quota tokens" in error_message
        or "resource has been exhausted" in error_message
        or "resourceexhausted" in error_message
        or "rate limit" in error_message
        or "too many requests" in error_message
        or "concurrent request" in error_message
    )


def classify_quota_error(error) -> str:
    """Classify a quota error so callers can choose a wait strategy.

    Returns one of: ``daily_tokens``, ``hourly_tokens``, ``rate_limit``, ``unknown``.
    """
    reason = (error_reason(error) or "").lower()
    error_message = str(error).lower()
    combined = f"{reason} {error_message}"

    if reason == "dailyquotaexceeded" or "dailyquota" in combined or "tokens per day" in combined:
        return "daily_tokens"

    if any(marker in combined for marker in HOURLY_QUOTA_MARKERS):
        return "hourly_tokens"

    if reason in {"userratelimitexceeded", "ratelimitexceeded"} or any(
        marker in combined for marker in RATE_LIMIT_MARKERS
    ):
        return "rate_limit"

    if is_quota_error(error):
        # Default 429 / ResourceExhausted without a more specific message to hourly
        # tokens — that is the most common GA4 Data API exhaustion case.
        return "hourly_tokens"

    return "unknown"


def is_fatal_error(error):
    """Return a boolean value depending on if its a fatal error or not."""
    if isinstance(error, socket.timeout):
        return False

    if is_quota_error(error):
        return False

    # Google API exceptions have .message and .code; plain exceptions (e.g. RuntimeError)
    # do not - use getattr to avoid AttributeError.
    message = getattr(error, "message", None)
    status = getattr(error, "code", None) if message is not None else None
    if status in [429, 500, 503]:
        return False

    # Use list of errors defined in:
    # https://developers.google.com/analytics/devguides/reporting/core/v4/errors
    reason = error_reason(error)
    if reason in NON_FATAL_ERRORS:
        return False

    LOGGER.critical("Received fatal error %s, reason=%s, status=%s", error, reason, status)
    return True
