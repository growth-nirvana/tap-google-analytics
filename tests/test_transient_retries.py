"""Server errors and dropped connections are retried; a request given up is described."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import grpc
import pytest
from google.api_core.exceptions import (
    DeadlineExceeded,
    InternalServerError,
    InvalidArgument,
    PermissionDenied,
    ResourceExhausted,
    ServiceUnavailable,
)

from tap_google_analytics.client import GoogleAnalyticsStream
from tap_google_analytics.error import describe_error, is_fatal_error, is_transient_error
from tap_google_analytics.quota_manager import QuotaManager

# What run bf8cdcbb (Aria, 2026-11-23) got from GA4 on a geo page of property 328951053.
RESET = "recvmsg:Connection reset by peer"


class _RpcError(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE


def _reset():
    error = ServiceUnavailable(RESET)
    error._grpc_status_code = grpc.StatusCode.UNAVAILABLE
    return error


def _stream(run_report, retries=8):
    tap = MagicMock()
    tap.config = {"property_id": "328951053", "start_date": "2025-05-05", "end_date": "2026-11-23"}
    tap.quota_manager = QuotaManager(min_request_interval=0, max_transient_retries=retries)
    report = {"dimensions": [], "metrics": [], "metricFilter": None, "dimensionFilter": None}
    stream = GoogleAnalyticsStream(
        tap=tap,
        name="geo",
        ga_report=report,
        ga_dimensions_ref={},
        ga_metrics_ref={},
        ga_analytics_client=MagicMock(),
    )
    stream.property_id = "328951053"
    stream.end_date = "2026-11-23"
    stream.analytics.run_report.side_effect = run_report
    return stream, report


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    slept = []
    monkeypatch.setattr("tap_google_analytics.quota_manager.time.sleep", slept.append)
    return slept


@pytest.mark.parametrize(
    "error",
    [
        _reset(),
        ServiceUnavailable("503 The service is currently unavailable."),
        InternalServerError("500 Internal error encountered."),
        DeadlineExceeded("504 Deadline Exceeded"),
        ConnectionResetError(104, "Connection reset by peer"),
        TimeoutError("timed out"),
        _RpcError(),
        RuntimeError(f"Property ID '328951053': 503 {RESET}"),
    ],
)
def test_transient_errors(error):
    if isinstance(error, RuntimeError):
        error.__cause__ = _reset()
    assert is_transient_error(error) is True
    assert is_fatal_error(error) is False


@pytest.mark.parametrize(
    "error",
    [
        InvalidArgument("400 Field foo is not a valid dimension."),
        PermissionDenied("403 User does not have sufficient permissions"),
        ResourceExhausted("429 Exhausted property tokens for a project per hour."),
        RuntimeError("Loop detected in pagination."),
    ],
)
def test_not_transient(error):
    assert is_transient_error(error) is False


def test_wrapped_reset_was_fatal_before():
    """The bug: _query_api wrapped the 503 in a RuntimeError before backoff's giveup check,
    so the reset counted as fatal and was never retried. The wrapper is now seen through."""
    wrapped = RuntimeError(f"Property ID '328951053': 503 {RESET}")
    wrapped.__cause__ = _reset()
    assert is_fatal_error(wrapped) is False


def test_reset_is_retried_and_the_page_returned(_no_sleep):
    page = MagicMock(property_quota=None)
    stream, report = _stream([_reset(), _reset(), page])
    assert stream._query_api(report, "2025-05-05", 4) is page
    assert stream.analytics.run_report.call_count == 3
    assert stream.quota_manager.last_attempts == 3
    assert len(_no_sleep) == 2
    # exponential with jitter: retry 1 waits 1-2s, retry 2 waits 2-4s
    assert 1.0 <= _no_sleep[0] <= 2.0
    assert 2.0 <= _no_sleep[1] <= 4.0


def test_transient_delay_grows_and_is_capped():
    for attempt, (low, high) in {1: (1, 2), 4: (8, 16), 7: (60, 120), 12: (60, 120)}.items():
        delay = QuotaManager.transient_delay(attempt)
        assert low <= delay <= high


def _failure_lines(stream):
    return [
        call.args[0] % call.args[1:]
        for call in stream.logger.error.call_args_list
        if "REQUEST FAILED" in call.args[0]
    ]


def test_gives_up_after_the_retries_and_says_what_failed():
    stream, report = _stream([_reset() for _ in range(4)], retries=3)
    with pytest.raises(RuntimeError, match="328951053"):
        stream._query_api(report, "2025-05-05", 4)
    assert stream.analytics.run_report.call_count == 4
    (line,) = _failure_lines(stream)
    detail = json.loads(line.split("REQUEST FAILED ", 1)[1])
    assert detail == {
        "tap": "tap-google-analytics",
        "stream": "geo",
        "account_id": "328951053",
        "start_date": "2025-05-05",
        "end_date": "2026-11-23",
        "offset": 400000,
        "limit": 100000,
        "attempts": 4,
        "seconds": detail["seconds"],
        "status": 503,
        "grpc_status": "UNAVAILABLE",
        "reason": None,
        "cause": "connection_reset",
        "error": "ServiceUnavailable",
    }


def test_rejected_request_is_not_retried():
    stream, report = _stream([InvalidArgument("400 Field foo is not a valid dimension.")])
    with pytest.raises(RuntimeError):
        stream._query_api(report, "2025-05-05")
    assert stream.analytics.run_report.call_count == 1
    (line,) = _failure_lines(stream)
    assert '"cause": "request_rejected"' in line
    assert '"status": 400' in line


def test_permission_denied_is_not_reported_as_a_failure():
    stream, report = _stream([PermissionDenied("403 no access")])
    with pytest.raises(RuntimeError, match="Property ID"):
        stream._query_api(report, "2025-05-05")
    assert _failure_lines(stream) == []


def test_describe_quota():
    assert describe_error(ResourceExhausted("429 Exhausted property tokens"))["cause"] == "quota"
