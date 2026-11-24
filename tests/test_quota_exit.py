"""quota_hourly_action "exit": an hourly-token 429 stops the tap with a QUOTA EXHAUSTED line
instead of sleeping until the next hour (run ee87cf08 slept ~55 min at a time in its job)."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import ResourceExhausted

from tap_google_analytics.client import GoogleAnalyticsStream
from tap_google_analytics.error import TapGaQuotaExhaustedError, is_fatal_error
from tap_google_analytics.quota_manager import QuotaManager
from tap_google_analytics.tap import TapGoogleAnalytics

HOURLY = (
    "429 Exhausted property tokens for a project per hour. These quota tokens will return "
    "in under an hour. To learn more, see https://developers.google.com/analytics/devguides/"
    "reporting/data/v1/quotas"
)


@pytest.fixture(autouse=True)
def _never_sleep(monkeypatch):
    def fail(seconds):
        raise AssertionError(f"slept {seconds}s")

    monkeypatch.setattr("tap_google_analytics.quota_manager.time.sleep", fail)


def _stream(action="exit", run_report=None):
    tap = MagicMock()
    tap.config = {"property_id": "328951053", "start_date": "2025-05-05", "end_date": "2026-11-23"}
    tap.quota_manager = QuotaManager(min_request_interval=0, hourly_action=action)
    report = {"dimensions": [], "metrics": [], "metricFilter": None, "dimensionFilter": None}
    stream = GoogleAnalyticsStream(
        tap=tap, name="geo", ga_report=report, ga_dimensions_ref={}, ga_metrics_ref={},
        ga_analytics_client=MagicMock(),
    )
    stream.property_id = "328951053"
    stream.end_date = "2026-11-23"
    stream.analytics.run_report.side_effect = run_report
    return stream, report


def _quota_lines(caplog):
    return [r for r in caplog.records if r.getMessage().startswith("QUOTA EXHAUSTED ")]


def test_next_hour_utc():
    assert QuotaManager.next_hour_utc(datetime(2026, 11, 23, 21, 59, 18, tzinfo=timezone.utc)) == datetime(
        2026, 11, 23, 22, 0, 5, tzinfo=timezone.utc
    )
    # A naive time is UTC; another zone is converted.
    assert QuotaManager.next_hour_utc(datetime(2026, 11, 23, 23, 30)).hour == 0


def test_exit_logs_one_line_and_raises_without_sleeping(caplog, monkeypatch):
    monkeypatch.setattr(
        QuotaManager, "next_hour_utc", staticmethod(lambda now=None: datetime(2026, 11, 23, 22, 0, 5, tzinfo=timezone.utc))
    )
    calls = []

    def run_report(request):
        calls.append(request)
        raise ResourceExhausted(HOURLY)

    stream, report = _stream(run_report=run_report)
    with caplog.at_level(logging.INFO), pytest.raises(TapGaQuotaExhaustedError) as raised:
        stream._query_api(report, "2025-05-05")

    assert len(calls) == 1
    lines = _quota_lines(caplog)
    assert len(lines) == 1
    assert lines[0].levelno == logging.ERROR
    payload = json.loads(lines[0].getMessage()[len("QUOTA EXHAUSTED "):])
    assert payload == {
        "kind": "hourly_tokens",
        "message": str(ResourceExhausted(HOURLY))[:300],
        "property_id": "328951053",
        "retry_at": "2026-11-23T22:00:05Z",
        "stream": "geo",
        "tap": "tap-google-analytics",
    }
    assert lines[0].getMessage() == "QUOTA EXHAUSTED " + json.dumps(payload, sort_keys=True)
    assert raised.value.retry_at == "2026-11-23T22:00:05Z"
    # Not described as a request that ran out of retries.
    assert not any("REQUEST FAILED" in r.getMessage() for r in caplog.records)
    assert "Exceeded maximum retries" not in str(raised.value)
    assert is_fatal_error(raised.value) is True


def test_wait_is_still_the_default(monkeypatch):
    slept = []
    monkeypatch.setattr("tap_google_analytics.quota_manager.time.sleep", slept.append)
    monkeypatch.setattr("tap_google_analytics.quota_manager.random.uniform", lambda _a, _b: 0)
    monkeypatch.setattr(QuotaManager, "seconds_until_next_hour", staticmethod(lambda now=None: 120.0))
    pages = [ResourceExhausted(HOURLY), "page"]

    def run_report(_request):
        item = pages.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    stream, report = _stream(action="wait", run_report=run_report)
    stream._tap.quota_manager = QuotaManager(min_request_interval=0)
    stream.quota_manager = stream._tap.quota_manager
    assert stream.quota_manager.hourly_action == "wait"
    stream.analytics.run_report.side_effect = run_report
    assert stream._query_api(report, "2025-05-05") is not None
    assert 120.0 in slept


def test_exit_keeps_daily_and_rate_limit_handling(monkeypatch):
    slept = []
    monkeypatch.setattr("tap_google_analytics.quota_manager.time.sleep", slept.append)
    manager = QuotaManager(min_request_interval=0, hourly_action="exit")
    daily = ResourceExhausted("429 DailyQuotaExceeded: tokens per day exhausted")
    with pytest.raises(ResourceExhausted):
        manager.call(lambda: (_ for _ in ()).throw(daily))
    flaky = [RuntimeError("429 Too Many Requests: concurrent request limit"), "ok"]

    def concurrent():
        item = flaky.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    assert manager.call(concurrent) == "ok"
    assert len(slept) == 1


def test_exit_with_no_hourly_retries_raises_the_original(caplog):
    """quota_max_hourly_retries 0 (and discovery's metadata default) raise as before: no line."""
    manager = QuotaManager(min_request_interval=0, max_hourly_retries=0, hourly_action="exit")
    with pytest.raises(ResourceExhausted):
        manager.call(lambda: (_ for _ in ()).throw(ResourceExhausted(HOURLY)))
    assert not _quota_lines(caplog)


def test_bad_action_is_refused():
    with pytest.raises(ValueError):
        QuotaManager(hourly_action="sleep")


def test_config_setting():
    setting = next(p for p in TapGoogleAnalytics.config_jsonschema["properties"].items() if p[0] == "quota_hourly_action")
    assert setting[1]["default"] == "wait"
    assert setting[1]["enum"] == ["wait", "exit"]
