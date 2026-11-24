"""Each property's report is requested one date window at a time, with a STATE after each, so a
run that stops resumes after the last window it finished (Aria run bf8cdcbb: a geo page failed
after 20 minutes of an 18-month backfill, and the retry started over)."""

from __future__ import annotations

import copy
from datetime import date

import pytest
from google.analytics.data_v1beta.types import RunReportResponse
from google.api_core.exceptions import InvalidArgument
from singer_sdk._singerlib import RecordMessage, StateMessage

from tap_google_analytics.client import PROGRESS_KEY
from tap_google_analytics.time_dimensions import date_windows
from tests.test_custom_reports import _make_tap, _metadata

REPORT = {"name": "r", "dimensions": ["date"], "metrics": ["sessions"]}


def test_daily_reports_go_by_calendar_month():
    assert date_windows("date", date(2025, 5, 5), date(2025, 7, 10)) == [
        (date(2025, 5, 5), date(2025, 5, 31)),
        (date(2025, 6, 1), date(2025, 6, 30)),
        (date(2025, 7, 1), date(2025, 7, 10)),
    ]
    assert date_windows("dateHour", date(2024, 2, 1), date(2024, 2, 29)) == [(date(2024, 2, 1), date(2024, 2, 29))]
    assert date_windows("yearMonth", date(2024, 11, 1), date(2025, 1, 5)) == [
        (date(2024, 11, 1), date(2024, 11, 30)),
        (date(2024, 12, 1), date(2024, 12, 31)),
        (date(2025, 1, 1), date(2025, 1, 5)),
    ]


def test_weekly_reports_go_by_whole_weeks():
    # yearWeek: Sunday weeks, and January 1st (a Wednesday in 2025) starts week 01.
    windows = date_windows("yearWeek", date(2024, 12, 1), date(2025, 2, 28))
    assert windows[0] == (date(2024, 12, 1), date(2024, 12, 31))  # 4 Sunday weeks, then week 53 ends Dec 31
    assert windows[1] == (date(2025, 1, 1), date(2025, 2, 1))  # week 01 (Wed-Sat), then 4 more
    assert all(start.weekday() == 6 for start, _ in windows[2:])
    # isoYearIsoWeek: Monday weeks, 5 at a time.
    assert date_windows("isoYearIsoWeek", date(2024, 1, 1), date(2024, 2, 20)) == [
        (date(2024, 1, 1), date(2024, 2, 4)),
        (date(2024, 2, 5), date(2024, 2, 20)),
    ]


def test_a_report_without_a_time_dimension_is_one_window():
    assert date_windows(None, date(2024, 1, 1), date(2025, 6, 30)) == [(date(2024, 1, 1), date(2025, 6, 30))]
    assert date_windows("date", date(2024, 2, 1), date(2024, 1, 31)) == []


def _sync(config, state=None, fail=None):
    """Sync stream r for properties 1 and 2; one row per request (its first day). Returns the
    messages written, the requests made and the error raised (if any)."""
    tap, streams, analytics = _make_tap({"reports_list": [REPORT], **config}, {"1": _metadata(), "2": _metadata()})
    requests = []

    def run_report(request):
        start, end = request.date_ranges[0].start_date, request.date_ranges[0].end_date
        requests.append((request.property, start, end))
        if fail and fail(request.property, start):
            raise InvalidArgument("400 nope")
        return RunReportResponse(
            dimension_headers=[{"name": "date"}],
            metric_headers=[{"name": "sessions"}],
            rows=[{"dimension_values": [{"value": start.replace("-", "")}], "metric_values": [{"value": "1"}]}],
            row_count=1,
        )

    analytics.run_report.side_effect = run_report
    messages = []
    # Copied as written: a STATE message holds the tap's live state dict.
    tap.write_message = lambda message: messages.append(copy.deepcopy(message))
    stream = streams["r"]
    stream._tap = tap
    if state is not None:
        tap.load_state({"bookmarks": {"r": state}})
    error = None
    try:
        stream.sync()
    except Exception as exc:  # noqa: BLE001 - returned for the test
        error = exc
    return messages, requests, error


def _states(messages):
    return [m.value["bookmarks"]["r"].get(PROGRESS_KEY) for m in messages if isinstance(m, StateMessage)]


def test_a_state_follows_each_window_and_the_finished_stream_drops_it():
    messages, requests, error = _sync({"start_date": "2024-01-15", "end_date": "2024-03-10"})
    assert error is None
    assert requests == [
        ("properties/1", "2024-01-15", "2024-01-31"),
        ("properties/1", "2024-02-01", "2024-02-29"),
        ("properties/1", "2024-03-01", "2024-03-10"),
        ("properties/2", "2024-01-15", "2024-01-31"),
        ("properties/2", "2024-02-01", "2024-02-29"),
        ("properties/2", "2024-03-01", "2024-03-10"),
    ]
    # Each window's row, then a STATE vouching for it.
    kinds = [(type(m).__name__, m.record["date"] if isinstance(m, RecordMessage) else None) for m in messages
             if isinstance(m, (RecordMessage, StateMessage))]
    assert kinds[:4] == [("RecordMessage", "20240115"), ("StateMessage", None), ("RecordMessage", "20240201"), ("StateMessage", None)]
    assert _states(messages)[:4] == [
        {"1": "2024-01-31"},
        {"1": "2024-02-29"},
        {"1": "2024-03-10"},
        {"1": "2024-03-10", "2": "2024-01-31"},
    ]
    # Finished: the SDK's bookmark stands alone.
    last = [m for m in messages if isinstance(m, StateMessage)][-1].value["bookmarks"]["r"]
    assert PROGRESS_KEY not in last and last["replication_key_value"] == "20240301"


def test_a_run_that_stops_keeps_the_windows_it_finished():
    messages, requests, error = _sync(
        {"start_date": "2024-01-15", "end_date": "2024-03-10"}, fail=lambda prop, start: prop == "properties/2" and start == "2024-02-01"
    )
    assert isinstance(error, Exception)
    assert _states(messages)[-1] == {"1": "2024-03-10", "2": "2024-01-31"}
    assert requests[-1] == ("properties/2", "2024-02-01", "2024-02-29")


def test_the_next_run_resumes_each_property_after_its_last_window():
    state = {"replication_key": "date", PROGRESS_KEY: {"1": "2024-03-10", "2": "2024-01-31"}}
    _, requests, error = _sync({"start_date": "2024-01-15", "end_date": "2024-03-10"}, state=state)
    assert error is None
    # Property 1 was done through the end; property 2 picks up in February.
    assert requests == [("properties/2", "2024-02-01", "2024-02-29"), ("properties/2", "2024-03-01", "2024-03-10")]


def test_progress_older_than_the_bookmark_window_is_ignored():
    """A property pulled through a date before where this run starts anyway starts there."""
    state = {"replication_key": "date", "replication_key_value": "20240401", PROGRESS_KEY: {"1": "2024-01-31"}}
    _, requests, _ = _sync({"start_date": "2024-01-01", "end_date": "2024-04-10"}, state=state)
    # Bookmark 2024-04-01 less the 30-day lookback: 2024-03-02, for both properties.
    assert [r for r in requests if r[0] == "properties/1"][0] == ("properties/1", "2024-03-02", "2024-03-31")


@pytest.mark.parametrize("windows", [False])
def test_date_windows_can_be_turned_off(windows):
    _, requests, _ = _sync({"start_date": "2024-01-15", "end_date": "2024-03-10", "date_windows": windows})
    assert requests == [("properties/1", "2024-01-15", "2024-03-10"), ("properties/2", "2024-01-15", "2024-03-10")]
