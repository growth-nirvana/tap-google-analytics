"""Tests for custom reports: merged defaults, per-property fields, time dimensions."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from google.analytics.data_v1beta.types import (
    DimensionMetadata,
    Metadata,
    MetricMetadata,
    MetricType,
    RunReportResponse,
)
from google.api_core.exceptions import PermissionDenied, ResourceExhausted

from tap_google_analytics.tap import TapGoogleAnalytics
from tap_google_analytics.time_dimensions import (
    get_time_dimension,
    period_start,
    snap_to_period_start,
)

STANDARD_DIMENSIONS = [
    "date",
    "dateHour",
    "yearWeek",
    "isoYearIsoWeek",
    "yearMonth",
    "sessionSource",
    "country",
    "source",
    "medium",
    "sourcePlatform",
    "countryId",
    "region",
    "city",
    "cityId",
    "deviceCategory",
    "deviceModel",
    "operatingSystem",
    "browser",
    "hostName",
    "pagePath",
    "sessionMedium",
]
STANDARD_METRICS = {
    "sessions": MetricType.TYPE_INTEGER,
    "activeUsers": MetricType.TYPE_INTEGER,
    "newUsers": MetricType.TYPE_INTEGER,
    "totalUsers": MetricType.TYPE_INTEGER,
    "sessionsPerUser": MetricType.TYPE_FLOAT,
    "averageSessionDuration": MetricType.TYPE_SECONDS,
    "screenPageViews": MetricType.TYPE_INTEGER,
    "screenPageViewsPerSession": MetricType.TYPE_FLOAT,
    "bounceRate": MetricType.TYPE_FLOAT,
    "engagementRate": MetricType.TYPE_FLOAT,
    "active28DayUsers": MetricType.TYPE_INTEGER,
    "active7DayUsers": MetricType.TYPE_INTEGER,
    "active1DayUsers": MetricType.TYPE_INTEGER,
    "transactions": MetricType.TYPE_INTEGER,
    "totalRevenue": MetricType.TYPE_CURRENCY,
    "purchaseRevenue": MetricType.TYPE_CURRENCY,
    "averageRevenuePerUser": MetricType.TYPE_CURRENCY,
    "ecommercePurchases": MetricType.TYPE_INTEGER,
    "itemsPurchased": MetricType.TYPE_INTEGER,
    "keyEvents": MetricType.TYPE_INTEGER,
    "engagedSessions": MetricType.TYPE_INTEGER,
    "eventCount": MetricType.TYPE_INTEGER,
    "screenPageViewsPerUser": MetricType.TYPE_FLOAT,
    "userEngagementDuration": MetricType.TYPE_SECONDS,
}

CUSTOM_REPORT = {
    "name": "custom_source",
    "dimensions": ["date", "sessionSource", "customEvent:plan"],
    "metrics": ["sessions", "keyEvents:purchase"],
}


def _metadata(custom_dimensions=(), custom_metrics=()):
    return Metadata(
        dimensions=[
            DimensionMetadata(api_name=name)
            for name in [*STANDARD_DIMENSIONS, *custom_dimensions]
        ],
        metrics=[
            MetricMetadata(api_name=name, type_=metric_type)
            for name, metric_type in {
                **STANDARD_METRICS,
                **{name: MetricType.TYPE_INTEGER for name in custom_metrics},
            }.items()
        ],
    )


def _make_tap(config, metadata_by_property):
    analytics = MagicMock()

    def get_metadata(request):
        property_id = request.name.split("/")[1]
        result = metadata_by_property[property_id]
        if isinstance(result, Exception):
            raise result
        return result

    analytics.get_metadata.side_effect = get_metadata
    base_config = {
        "property_ids": ",".join(metadata_by_property),
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
        "request_delay_seconds": 0,
    }
    with patch.object(TapGoogleAnalytics, "_initialize_credentials", return_value=None), patch.object(
        TapGoogleAnalytics, "_initialize_analytics", return_value=analytics
    ):
        tap = TapGoogleAnalytics(config={**base_config, **config})
        streams = dict(tap.streams)
    return tap, streams, analytics


def _response(dimension_names, metric_names, rows):
    return RunReportResponse(
        dimension_headers=[{"name": name} for name in dimension_names],
        metric_headers=[{"name": name} for name in metric_names],
        rows=[
            {
                "dimension_values": [{"value": v} for v in dims],
                "metric_values": [{"value": v} for v in metrics],
            }
            for dims, metrics in rows
        ],
        row_count=len(rows),
    )


# merge_default_reports


def test_reports_list_replaces_defaults_by_default():
    _, streams, _ = _make_tap({"reports_list": [CUSTOM_REPORT]}, {"1": _metadata(
        ["customEvent:plan"], ["keyEvents:purchase"]
    )})
    assert set(streams) == {"custom_source"}


def test_merge_default_reports_adds_and_replaces():
    replaced_overview = {
        "name": "website_overview",
        "dimensions": ["date"],
        "metrics": ["sessions"],
    }
    tap, streams, _ = _make_tap(
        {
            "reports_list": [replaced_overview, CUSTOM_REPORT],
            "merge_default_reports": True,
        },
        {"1": _metadata(["customEvent:plan"], ["keyEvents:purchase"])},
    )
    assert [report["name"] for report in tap.reports_definition] == [
        "website_overview",
        "traffic_sources",
        "locations",
        "four_weekly_active_users",
        "weekly_active_users",
        "daily_active_users",
        "devices",
        "transactions",
        "pages",
        "custom_source",
    ]
    assert streams["website_overview"].report["metrics"] == ["sessions"]


# per-property metadata


def test_metadata_is_union_across_properties():
    tap, streams, analytics = _make_tap(
        {"reports_list": [CUSTOM_REPORT]},
        {
            "1": _metadata(["customEvent:plan"], []),
            "2": _metadata([], ["keyEvents:purchase"]),
        },
    )
    assert analytics.get_metadata.call_count == 2
    assert "customEvent:plan" in tap.dimensions_ref
    assert tap.metrics_ref["keyEvents:purchase"] == "integer"
    assert set(tap.property_metadata) == {"1", "2"}
    schema = streams["custom_source"].schema
    assert "customEvent:plan" in schema["properties"]
    assert "keyEvents:purchase" in schema["properties"]
    # customEvent:plan is missing on property 2, so it is nullable
    assert "customEvent:plan" not in schema.get("required", [])
    assert "sessionSource" in schema["required"]


def test_field_missing_everywhere_fails_validation():
    with pytest.raises(SystemExit):
        _make_tap(
            {"reports_list": [CUSTOM_REPORT]},
            {"1": _metadata(["customEvent:plan"], []), "2": _metadata([], [])},
        )


def test_metadata_skips_inaccessible_property():
    tap, _, _ = _make_tap(
        {"reports_list": [CUSTOM_REPORT]},
        {
            "1": PermissionDenied("403 no access"),
            "2": _metadata(["customEvent:plan"], ["keyEvents:purchase"]),
        },
    )
    assert set(tap.property_metadata) == {"2"}


def test_quota_failed_property_keeps_report_fields_valid():
    quota_error = ResourceExhausted(
        "429 Exhausted property tokens for a project per hour."
    )
    tap, streams, _ = _make_tap(
        {"reports_list": [CUSTOM_REPORT]},
        {"1": _metadata([], []), "2": quota_error},
    )
    # Property 2 has no metadata, so its custom fields come from the report
    assert "customEvent:plan" in tap.dimensions_ref
    assert "keyEvents:purchase" in tap.metrics_ref
    assert streams["custom_source"]._report_for_property("2") == CUSTOM_REPORT


def test_merge_field_type():
    assert TapGoogleAnalytics._merge_field_type(None, "integer") == "integer"
    assert TapGoogleAnalytics._merge_field_type("integer", "integer") == "integer"
    assert TapGoogleAnalytics._merge_field_type("integer", "float") == "float"
    assert TapGoogleAnalytics._merge_field_type("integer", "currency") == "string"


def test_missing_fields_dropped_per_property_and_emitted_as_null():
    _, streams, analytics = _make_tap(
        {"reports_list": [CUSTOM_REPORT]},
        {
            "1": _metadata(["customEvent:plan"], ["keyEvents:purchase"]),
            "2": _metadata([], ["keyEvents:purchase"]),
        },
    )
    stream = streams["custom_source"]
    requests = []

    def run_report(request):
        requests.append(request)
        if request.property == "properties/1":
            return _response(
                ["date", "sessionSource", "customEvent:plan"],
                ["sessions", "keyEvents:purchase"],
                [(["20240102", "google", "pro"], ["5", "1"])],
            )
        return _response(
            ["date", "sessionSource"],
            ["sessions", "keyEvents:purchase"],
            [(["20240102", "bing"], ["3", "0"])],
        )

    analytics.run_report.side_effect = run_report
    with patch.object(stream.logger, "warning") as warning:
        records = list(stream.get_records(None))
        list(stream.get_records(None))

    assert [d.name for d in requests[0].dimensions] == [
        "date",
        "sessionSource",
        "customEvent:plan",
    ]
    assert [d.name for d in requests[1].dimensions] == ["date", "sessionSource"]
    assert records[0]["customEvent:plan"] == "pro"
    assert records[1]["customEvent:plan"] is None
    assert records[1]["property_id"] == "properties/2"
    assert records[1]["sessions"] == 3
    # Logged once per (report, property) even across repeated requests
    dropped_logs = [c for c in warning.call_args_list if "does not have" in c.args[0]]
    assert len(dropped_logs) == 1
    assert dropped_logs[0].args[2:] == ("2", "customEvent:plan")


def test_property_without_any_metric_is_skipped():
    report = {
        "name": "custom_only",
        "dimensions": ["date"],
        "metrics": ["keyEvents:purchase"],
    }
    _, streams, _ = _make_tap(
        {"reports_list": [report]},
        {"1": _metadata([], ["keyEvents:purchase"]), "2": _metadata([], [])},
    )
    assert streams["custom_only"]._report_for_property("2") is None


# time dimensions


def test_get_time_dimension():
    assert get_time_dimension(["country", "date"]) == "date"
    assert get_time_dimension(["yearMonth", "country"]) == "yearMonth"
    assert get_time_dimension(["country"]) is None


@pytest.mark.parametrize(
    ("dimension", "value", "expected"),
    [
        ("date", "20240305", date(2024, 3, 5)),
        ("date", "2024-03-05T00:00:00Z", date(2024, 3, 5)),
        ("dateHour", "2024030513", date(2024, 3, 5)),
        # 2024-01-01 is a Monday: week 01 is Jan 1-6, week 02 starts Sunday Jan 7
        ("yearWeek", "202401", date(2024, 1, 1)),
        ("yearWeek", "202402", date(2024, 1, 7)),
        # 2023-01-01 is a Sunday
        ("yearWeek", "202301", date(2023, 1, 1)),
        ("yearWeek", "202302", date(2023, 1, 8)),
        ("isoYearIsoWeek", "202401", date(2024, 1, 1)),
        ("isoYearIsoWeek", "202501", date(2024, 12, 30)),
        ("yearMonth", "202402", date(2024, 2, 1)),
    ],
)
def test_period_start(dimension, value, expected):
    assert period_start(dimension, value) == expected


def test_snap_to_period_start():
    wednesday = date(2024, 1, 10)
    assert snap_to_period_start("yearWeek", wednesday) == date(2024, 1, 7)
    assert snap_to_period_start("yearWeek", date(2024, 1, 3)) == date(2024, 1, 1)
    assert snap_to_period_start("isoYearIsoWeek", wednesday) == date(2024, 1, 8)
    assert snap_to_period_start("yearMonth", wednesday) == date(2024, 1, 1)
    assert snap_to_period_start("dateHour", wednesday) == wednesday


def test_more_than_one_time_dimension_is_rejected():
    report = {"name": "bad", "dimensions": ["date", "yearMonth"], "metrics": ["sessions"]}
    with pytest.raises(SystemExit):
        _make_tap({"reports_list": [report]}, {"1": _metadata()})


def test_too_many_dimensions_and_metrics_are_rejected():
    too_many_dimensions = {
        "name": "bad",
        "dimensions": STANDARD_DIMENSIONS[4:14],
        "metrics": ["sessions"],
    }
    with pytest.raises(SystemExit):
        _make_tap({"reports_list": [too_many_dimensions]}, {"1": _metadata()})

    too_many_metrics = {
        "name": "bad",
        "dimensions": ["date"],
        "metrics": list(STANDARD_METRICS)[:11],
    }
    with pytest.raises(SystemExit):
        _make_tap({"reports_list": [too_many_metrics]}, {"1": _metadata()})


@pytest.mark.parametrize(
    "time_dimension", ["date", "dateHour", "yearWeek", "isoYearIsoWeek", "yearMonth"]
)
def test_time_dimension_is_replication_key(time_dimension):
    report = {"name": "r", "dimensions": [time_dimension, "country"], "metrics": ["sessions"]}
    _, streams, _ = _make_tap({"reports_list": [report]}, {"1": _metadata()})
    stream = streams["r"]
    schema = stream.schema
    assert stream.replication_key == time_dimension
    assert stream.primary_keys == [time_dimension, "country", "property_id"]
    assert "date" in schema["properties"]
    assert "date" not in stream.primary_keys or time_dimension == "date"


def test_report_without_time_dimension_keeps_full_load_keys():
    report = {"name": "r", "dimensions": ["country"], "metrics": ["sessions"]}
    _, streams, _ = _make_tap({"reports_list": [report]}, {"1": _metadata()})
    stream = streams["r"]
    stream.schema  # noqa: B018
    assert stream.replication_key is None
    assert stream.primary_keys == [
        "country",
        "property_id",
        "report_start_date",
        "report_end_date",
    ]
    assert "date" not in stream.schema["properties"]


@pytest.mark.parametrize(
    ("time_dimension", "bookmark", "expected_start"),
    [
        # date: bookmark day minus 30 days (unchanged behaviour)
        ("date", "20240315", "2024-02-14"),
        # dateHour: bookmark day minus 30 days
        ("dateHour", "2024031513", "2024-02-14"),
        # yearWeek 202411 starts Sun 2024-03-10; -30 days = Fri 2024-02-09 -> Sun 2024-02-04
        ("yearWeek", "202411", "2024-02-04"),
        # isoYearIsoWeek 202411 starts Mon 2024-03-11; -30 = Sat 2024-02-10 -> Mon 2024-02-05
        ("isoYearIsoWeek", "202411", "2024-02-05"),
        # yearMonth 202403 starts 2024-03-01; -30 days = 2024-01-31 -> 2024-01-01
        ("yearMonth", "202403", "2024-01-01"),
    ],
)
def test_resume_from_bookmarked_period(time_dimension, bookmark, expected_start):
    report = {"name": "r", "dimensions": [time_dimension], "metrics": ["sessions"]}
    _, streams, _ = _make_tap({"reports_list": [report]}, {"1": _metadata()})
    stream = streams["r"]
    with patch.object(
        stream, "get_context_state", return_value={"replication_key_value": bookmark}
    ):
        assert stream._get_state_filter(None) == expected_start


def test_first_sync_starts_on_period_boundary():
    report = {"name": "r", "dimensions": ["yearMonth"], "metrics": ["sessions"]}
    _, streams, _ = _make_tap(
        {"reports_list": [report], "start_date": "2024-03-15"}, {"1": _metadata()}
    )
    stream = streams["r"]
    with patch.object(stream, "get_context_state", return_value={}):
        assert stream._get_state_filter(None) == "2024-03-01"


@pytest.mark.parametrize(
    ("time_dimension", "value", "expected_date"),
    [
        ("dateHour", "2024031513", "20240315"),
        ("yearWeek", "202402", "20240107"),
        ("isoYearIsoWeek", "202402", "20240108"),
        ("yearMonth", "202403", "20240301"),
    ],
)
def test_records_get_period_start_date(time_dimension, value, expected_date):
    report = {"name": "r", "dimensions": [time_dimension], "metrics": ["sessions"]}
    _, streams, analytics = _make_tap({"reports_list": [report]}, {"1": _metadata()})
    analytics.run_report.return_value = _response(
        [time_dimension], ["sessions"], [([value], ["7"])]
    )
    (record,) = list(streams["r"].get_records(None))
    assert record[time_dimension] == value
    assert record["date"] == expected_date
    assert record["sessions"] == 7
