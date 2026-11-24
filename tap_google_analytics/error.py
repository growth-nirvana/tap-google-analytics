"""GoogleAnalytics error classes."""

from __future__ import annotations

import contextlib
import json
import logging
import socket

try:
    from google.api_core.exceptions import GoogleAPICallError
    from google.api_core.exceptions import PermissionDenied as GooglePermissionDenied
    from google.api_core.exceptions import ResourceExhausted as GoogleResourceExhausted
except ImportError:
    # Fallback so no exception is ever considered PermissionDenied / quota
    class GoogleAPICallError(Exception):  # type: ignore[no-redef]
        """Stand-in when google-api-core is missing."""

    class GooglePermissionDenied(Exception):  # type: ignore[no-redef]
        pass

    class GoogleResourceExhausted(Exception):  # type: ignore[no-redef]
        pass


try:
    from google.auth.exceptions import TransportError as GoogleAuthTransportError
except ImportError:

    class GoogleAuthTransportError(Exception):  # type: ignore[no-redef]
        """Stand-in when google-auth is missing."""


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


class TapGaQuotaExhaustedError(TapGaApiError):
    """The hourly GA4 token quota ran out and quota_hourly_action is "exit".

    Raised instead of sleeping until the next hour, after one ``QUOTA EXHAUSTED {json}`` line,
    so the tap stops and the caller can retry after ``retry_at``.
    """

    def __init__(self, message: str, *, property_id=None, stream=None, retry_at: str = "") -> None:
        """Keep the property, stream and retry time (UTC, ISO 8601) the error is about."""
        super().__init__(message)
        self.property_id = property_id
        self.stream = stream
        self.retry_at = retry_at


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
    if isinstance(error, TapGaQuotaExhaustedError):
        # Hourly quota with quota_hourly_action "exit": stop now, never retry.
        return True

    if isinstance(error, socket.timeout):
        return False

    if is_quota_error(error) or is_transient_error(error):
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


# HTTP statuses and gRPC status names Google says to retry with exponential backoff: the
# server failed or the connection dropped, and the same request can succeed when sent again.
TRANSIENT_HTTP_STATUSES = (500, 502, 503, 504)
TRANSIENT_GRPC_STATUSES = ("UNAVAILABLE", "DEADLINE_EXCEEDED", "INTERNAL")
HTTP_GATEWAY_TIMEOUT = 504
HTTP_CLIENT_ERRORS = range(400, 500)


def _chain(error: BaseException | None):
    """The error and the errors it was raised from (``raise ... from``), outermost first."""
    seen = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        yield error
        error = error.__cause__ or error.__context__


def _http_status(error: BaseException):
    code = getattr(error, "code", None)
    return code if isinstance(code, int) else None


def _grpc_status(error: BaseException) -> str | None:
    status = getattr(error, "grpc_status_code", None)
    if status is None and callable(getattr(error, "code", None)):
        # A raw grpc.RpcError: code() is the StatusCode
        try:
            status = error.code()  # type: ignore[operator]
        except Exception:  # noqa: BLE001
            status = None
    name = getattr(status, "name", None)
    return name if isinstance(name, str) else None


def is_transient_error(error: BaseException) -> bool:
    """Return True when sending the same request again can succeed.

    That is a 5xx from Google, a gRPC UNAVAILABLE / DEADLINE_EXCEEDED / INTERNAL, or a
    dropped or timed-out connection. Looks through ``raise ... from``, so a wrapped error is
    judged by what Google returned.
    Quota and rate-limit errors are not transient here: QuotaManager waits for those.
    """
    for err in _chain(error):
        if is_quota_error(err):
            return False
        if isinstance(
            err, (ConnectionError, TimeoutError, socket.timeout, GoogleAuthTransportError)
        ):
            return True
        if isinstance(err, GoogleAPICallError) and _http_status(err) in TRANSIENT_HTTP_STATUSES:
            return True
        if _grpc_status(err) in TRANSIENT_GRPC_STATUSES:
            return True
    return False


def describe_error(error: BaseException) -> dict:
    """Return what Google returned, as plain values.

    The HTTP status, gRPC status name, the error reason (ErrorInfo / API reason) and a short
    cause code, read through ``raise ... from``.
    """
    status = grpc_status = reason = None
    for err in _chain(error):
        if status is None:
            status = _http_status(err) if isinstance(err, GoogleAPICallError) else None
        grpc_status = grpc_status or _grpc_status(err)
        reason = reason or getattr(err, "reason", None) or error_reason(err) or None
    message = str(error).lower()
    if any(is_quota_error(err) for err in _chain(error)):
        cause = "quota"
    elif any(isinstance(err, GooglePermissionDenied) for err in _chain(error)):
        cause = "permission_denied"
    elif "connection reset" in message or any(
        isinstance(err, ConnectionResetError) for err in _chain(error)
    ):
        cause = "connection_reset"
    elif (
        grpc_status == "DEADLINE_EXCEEDED"
        or status == HTTP_GATEWAY_TIMEOUT
        or "timed out" in message
        or any(isinstance(err, (TimeoutError, socket.timeout)) for err in _chain(error))
    ):
        cause = "timeout"
    elif is_transient_error(error):
        cause = "server_error"
    else:
        cause = "request_rejected" if status in HTTP_CLIENT_ERRORS else "other"
    return {
        "status": status,
        "grpc_status": grpc_status,
        "reason": reason if isinstance(reason, str) else None,
        "cause": cause,
        "error": type(
            next((e for e in _chain(error) if isinstance(e, GoogleAPICallError)), error)
        ).__name__,
    }
