"""Tests for quota management functionality."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import ResourceExhausted

from tap_google_analytics.client import GoogleAnalyticsStream
from tap_google_analytics.error import classify_quota_error, is_fatal_error, is_quota_error
from tap_google_analytics.quota_manager import QuotaManager
from tap_google_analytics.tap import TapGoogleAnalytics


HOURLY_TOKEN_MESSAGE = (
    "429 Exhausted property tokens for a project per hour. "
    "These quota tokens will return in under an hour. To learn more, see "
    "https://developers.google.com/analytics/devguides/reporting/data/v1/quotas"
)


def test_quota_error_detection_resource_exhausted():
    """ResourceExhausted from get_metadata should be treated as a quota error."""
    error = ResourceExhausted(HOURLY_TOKEN_MESSAGE)
    assert is_quota_error(error) is True
    assert is_fatal_error(error) is False
    assert classify_quota_error(error) == "hourly_tokens"


def test_quota_error_detection_message_only():
    """Plain exceptions with the GA4 quota text should still match."""
    error = RuntimeError(HOURLY_TOKEN_MESSAGE)
    assert is_quota_error(error) is True
    assert classify_quota_error(error) == "hourly_tokens"


def test_quota_error_detection_daily():
    """Daily quota errors should not be treated as hourly waits."""
    error = ResourceExhausted("429 DailyQuotaExceeded: tokens per day exhausted")
    assert is_quota_error(error) is True
    assert classify_quota_error(error) == "daily_tokens"


def test_quota_error_detection_rate_limit():
    """Concurrent / rate-limit errors should use short backoff."""
    error = RuntimeError("429 Too Many Requests: concurrent request limit")
    assert is_quota_error(error) is True
    assert classify_quota_error(error) == "rate_limit"


def test_regular_error_is_not_quota():
    """Unrelated errors should not be classified as quota failures."""
    error = RuntimeError("Property ID '123': PERMISSION_DENIED")
    assert is_quota_error(error) is False
    assert classify_quota_error(error) == "unknown"


def test_seconds_until_next_hour():
    """Hourly wait should cover the remainder of the hour plus a buffer."""
    now = datetime(2024, 1, 1, 14, 30, 0)
    assert QuotaManager.seconds_until_next_hour(now) == 1805.0


def test_quota_manager_wait(monkeypatch):
    """wait_for_quota_reset should sleep until the next hour."""
    slept = []

    monkeypatch.setattr("tap_google_analytics.quota_manager.time.sleep", slept.append)
    monkeypatch.setattr(
        "tap_google_analytics.quota_manager.random.uniform",
        lambda _a, _b: 0,
    )

    with patch.object(QuotaManager, "seconds_until_next_hour", return_value=120.0):
        QuotaManager().wait_for_quota_reset()

    assert slept == [120.0]


def test_throttle_enforces_min_interval(monkeypatch):
    """A second call should sleep the remaining request interval."""
    slept = []
    clock = {"now": 100.0}

    monkeypatch.setattr(
        "tap_google_analytics.quota_manager.time.monotonic",
        lambda: clock["now"],
    )
    monkeypatch.setattr("tap_google_analytics.quota_manager.time.sleep", slept.append)

    manager = QuotaManager(min_request_interval=2.0)
    manager.throttle()
    clock["now"] = 100.5
    manager.throttle()

    assert slept == [1.5]


def test_call_retries_hourly_quota_then_succeeds(monkeypatch):
    """Hourly token errors should wait for reset and then retry."""
    monkeypatch.setattr("tap_google_analytics.quota_manager.time.sleep", lambda _s: None)

    error = ResourceExhausted(HOURLY_TOKEN_MESSAGE)
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise error
        return "ok"

    manager = QuotaManager(min_request_interval=0, max_hourly_retries=2)
    with patch.object(manager, "wait_for_quota_reset") as wait:
        assert manager.call(flaky) == "ok"
        wait.assert_called_once()


def test_call_does_not_retry_daily_quota(monkeypatch):
    """Daily quota exhaustion should fail immediately."""
    monkeypatch.setattr("tap_google_analytics.quota_manager.time.sleep", lambda _s: None)
    error = ResourceExhausted("429 DailyQuotaExceeded: tokens per day exhausted")
    manager = QuotaManager(min_request_interval=0, max_hourly_retries=5)

    with pytest.raises(ResourceExhausted):
        manager.call(lambda: (_ for _ in ()).throw(error))


def test_call_gives_up_after_hourly_retries(monkeypatch):
    """Hourly retries should stop after the configured maximum."""
    monkeypatch.setattr("tap_google_analytics.quota_manager.time.sleep", lambda _s: None)
    error = ResourceExhausted(HOURLY_TOKEN_MESSAGE)
    manager = QuotaManager(min_request_interval=0, max_hourly_retries=1)

    with patch.object(manager, "wait_for_quota_reset"):
        with pytest.raises(ResourceExhausted):
            manager.call(lambda: (_ for _ in ()).throw(error), max_hourly_retries=1)


def test_observe_response_backs_off_when_tokens_are_low(monkeypatch):
    """Low remaining project tokens should add extra delay."""
    slept = []
    monkeypatch.setattr("tap_google_analytics.quota_manager.time.sleep", slept.append)

    response = MagicMock()
    response.property_quota.tokens_per_project_per_hour.remaining = 10
    response.property_quota.tokens_per_project_per_hour.consumed = 4990
    QuotaManager(min_request_interval=0).observe_response(response)

    assert slept == [30.0]


def test_infer_metric_types():
    """Fallback types should distinguish rates from counts."""
    assert TapGoogleAnalytics._infer_metric_type("engagementRate") == "number"
    assert TapGoogleAnalytics._infer_metric_type("totalRevenue") == "number"
    assert TapGoogleAnalytics._infer_metric_type("userEngagementDuration") == "number"
    assert TapGoogleAnalytics._infer_metric_type("sessions") == "integer"
    assert TapGoogleAnalytics._infer_metric_type("newUsers") == "integer"
    assert TapGoogleAnalytics._infer_metric_type("eventCount") == "integer"


def test_fetch_metadata_retries_then_falls_back(monkeypatch):
    """Discover should succeed with inferred types after metadata quota errors."""
    monkeypatch.setattr("tap_google_analytics.quota_manager.time.sleep", lambda _s: None)

    tap = MagicMock()
    tap.config = {
        "property_ids": "382954539",
        "quota_metadata_max_hourly_retries": 0,
    }
    tap.logger = MagicMock()
    tap.analytics = MagicMock()
    tap.analytics.get_metadata.side_effect = ResourceExhausted(HOURLY_TOKEN_MESSAGE)
    tap.quota_manager = QuotaManager(min_request_interval=0, max_hourly_retries=0)
    tap._infer_metric_type = TapGoogleAnalytics._infer_metric_type
    tap._infer_metadata_from_reports = (
        lambda: TapGoogleAnalytics._infer_metadata_from_reports(tap)
    )
    tap.reports_definition = [
        {
            "name": "user_acquisition_first_user_source_medium",
            "dimensions": ["date", "firstUserSource", "firstUserMedium"],
            "metrics": [
                "engagementRate",
                "newUsers",
                "totalUsers",
                "userEngagementDuration",
                "keyEvents",
                "engagedSessions",
                "eventCount",
                "totalRevenue",
            ],
        }
    ]

    dimensions, metrics = TapGoogleAnalytics._fetch_valid_api_metadata(tap)

    assert dimensions["date"] == "string"
    assert dimensions["firstUserSource"] == "string"
    assert metrics["engagementRate"] == "number"
    assert metrics["newUsers"] == "integer"
    assert metrics["totalRevenue"] == "number"
    tap.analytics.get_metadata.assert_called()


def test_query_api_retry_logic(monkeypatch):
    """Report queries should retry hourly quota errors via QuotaManager."""
    monkeypatch.setattr("tap_google_analytics.quota_manager.time.sleep", lambda _s: None)

    mock_tap = MagicMock()
    mock_tap.config = {
        "property_id": "123456",
        "start_date": "2024-01-01",
        "end_date": "2024-01-02",
        "request_delay_seconds": 0,
        "quota_max_hourly_retries": 2,
    }
    mock_tap.quota_manager = QuotaManager(min_request_interval=0, max_hourly_retries=2)

    mock_report = {
        "dimensions": [],
        "metrics": [],
        "metricFilter": None,
        "dimensionFilter": None,
    }

    stream = GoogleAnalyticsStream(
        tap=mock_tap,
        name="test_stream",
        ga_report=mock_report,
        ga_dimensions_ref={},
        ga_metrics_ref={},
        ga_analytics_client=MagicMock(),
    )
    stream.property_id = "123456"
    stream.end_date = "2024-01-02"

    error = ResourceExhausted(HOURLY_TOKEN_MESSAGE)
    stream.analytics.run_report.side_effect = [error, MagicMock()]

    with patch.object(stream.quota_manager, "wait_for_quota_reset"):
        stream._query_api(mock_report, "2024-01-01")

    assert stream.analytics.run_report.call_count == 2
