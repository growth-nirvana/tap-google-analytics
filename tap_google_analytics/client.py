"""Custom client handling, including GoogleAnalyticsStream base class."""

from __future__ import annotations

import copy
import functools
import json
import sys
import typing as t
from datetime import date, datetime, timedelta, timezone
import uuid

from google.analytics.data_v1beta.types import (
    DateRange,
    Metric,
    RunReportRequest,
    RunReportResponse,
)
from singer_sdk import typing as th
from singer_sdk.streams import Stream

from tap_google_analytics.error import describe_error
from tap_google_analytics.error import is_permission_denied_error
from tap_google_analytics.error import is_quota_error
from tap_google_analytics.property_ids import get_configured_property_ids
from tap_google_analytics.quota_manager import DEFAULT_TRANSIENT_RETRIES, QuotaManager
from tap_google_analytics.time_dimensions import (
    get_time_dimension,
    period_start,
    snap_to_period_start,
)
if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

if sys.version_info < (3, 11):
    from backports.datetime_fromisoformat import MonkeyPatch

    MonkeyPatch.patch_fromisoformat()


class GoogleAnalyticsStream(Stream):
    """Stream class for GoogleAnalytics streams."""

    _shared_run_id = None  # Class variable to store the run_id shared across all streams

    def __init__(self, *args, **kwargs) -> None:
        """Init GoogleAnalyticsStream."""
        self.report = kwargs.pop("ga_report")
        self.dimensions_ref = kwargs.pop("ga_dimensions_ref")
        self.metrics_ref = kwargs.pop("ga_metrics_ref")
        # {property_id: (dimensions, metrics)}; missing properties get every field
        self.property_metadata = kwargs.pop("ga_property_metadata", None) or {}
        self.analytics = kwargs.pop("ga_analytics_client")
        self.time_dimension = get_time_dimension(self.report["dimensions"])
        self._logged_dropped_fields: set[str] = set()

        super().__init__(*args, **kwargs)

        self.end_date = self._get_end_date()
        self.property_ids = get_configured_property_ids(self.config)
        self.page_size = 100000
        tap = getattr(self, "_tap", None)
        self.quota_manager = getattr(tap, "quota_manager", None) or QuotaManager(
            min_request_interval=float(self.config.get("request_delay_seconds", 0.5)),
            max_hourly_retries=int(self.config.get("quota_max_hourly_retries", 24)),
            max_transient_retries=int(
                self.config.get("max_transient_retries", DEFAULT_TRANSIENT_RETRIES)
            ),
        )

        # Generate a unique run_id for this tap run if not already set
        if GoogleAnalyticsStream._shared_run_id is None:
            GoogleAnalyticsStream._shared_run_id = int(datetime.now().timestamp() * 1000)

    @property
    def run_id(self) -> str:
        """Return the unique run_id for this tap run."""
        return GoogleAnalyticsStream._shared_run_id

    def _get_end_date(self):
        end_date_config = self.config.get("end_date")
        if not end_date_config:
            return datetime.now(timezone.utc).strftime("%Y-%m-%d")
            
        try:
            # Try parsing as ISO format first
            parsed = datetime.fromisoformat(end_date_config.replace('Z', '+00:00'))
        except ValueError:
            # If that fails, try parsing as YYYY-MM-DD
            parsed = datetime.strptime(end_date_config, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            
        return parsed.strftime("%Y-%m-%d")

    def _parse_dimension_type(self, attribute, dimensions_ref):
        if attribute in dimensions_ref:
            return self._parse_other_attrb_type(dimensions_ref[attribute])
        self.logger.critical("Unsupported GA type: %s", type)
        sys.exit(1)

    def _parse_metric_type(self, attribute, metrics_ref):
        # Custom Google Analytics Metrics {ga:goalXXStarts, ga:metricXX, ... }
        # We always treat them as strings as we can not be sure of
        # their data type
        if (
            (attribute.startswith("goal"))
            and attribute.endswith(
                (
                    "Starts",
                    "Completions",
                    "Value",
                    "ConversionRate",
                    "Abandons",
                    "AbandonRate",
                )
            )
        ) or attribute.startswith(("metric", "calcMetric")):
            return "string"

        if attribute in metrics_ref:
            return self._parse_other_attrb_type(metrics_ref[attribute])

        self.logger.critical("Unsupported GA type: %s", type)
        sys.exit(1)

    def _parse_other_attrb_type(self, attr_type):
        data_type = "string"

        if attr_type in ["integer"]:
            data_type = "integer"
        elif attr_type in ["float", "percent", "time", "seconds"]:
            data_type = "number"

        return data_type

    def _lookup_data_type(self, field_type, attribute, dimensions_ref, metrics_ref):
        """Get the data type of a metric or a dimension."""
        if field_type == "dimension":
            return self._parse_dimension_type(attribute, dimensions_ref)

        if field_type == "metric":
            return self._parse_metric_type(attribute, metrics_ref)

        self.logger.critical("Unsupported GA type: %s", field_type)
        sys.exit(1)

    @staticmethod
    def _generate_report_definition(report_def_raw):
        report_definition = {
            "metrics": [],
            "dimensions": [],
            "metricFilter": None,
            "dimensionFilter": None,
        }

        for dimension in report_def_raw["dimensions"]:
            report_definition["dimensions"].append({"name": dimension})

        for metric in report_def_raw["metrics"]:
            report_definition["metrics"].append(Metric(name=metric))

        if "metricFilter" in report_def_raw:
            report_definition["metricFilter"] = report_def_raw["metricFilter"]

        if "dimensionFilter" in report_def_raw:
            report_definition["dimensionFilter"] = report_def_raw["dimensionFilter"]

        # Add segmentIds to the request if the stream contains them
        if "segments" in report_def_raw:
            report_definition["segments"] = [
                {"segmentId": segment_id} for segment_id in report_def_raw["segments"]
            ]
        return report_definition

    def _request_data(
        self, api_report_def, state_filter: str, next_page_token: t.Any | None
    ) -> RunReportResponse:
        return self._query_api(api_report_def, state_filter, next_page_token)

    def _get_state_filter(self, context: Context | None) -> str:
        state = self.get_context_state(context)
        state_bookmark = state.get("replication_key_value")

        if state_bookmark:
            # The bookmark is a value of the report's time dimension
            # (YYYYMMDD, YYYYMMDDHH, YYYYWW or YYYYMM): resume from its period start
            parsed = period_start(self.time_dimension or "date", state_bookmark)
            parsed = max(parsed, date(2019, 1, 1))
            # Only apply lookback window if we have a state bookmark
            parsed = parsed - timedelta(days=30)
        else:
            # Handles both YYYY-MM-DD and ISO format dates
            parsed = period_start("date", self.config["start_date"])
            parsed = max(parsed, date(2019, 1, 1))

        # Start on a week / month boundary so the first period is complete
        if self.time_dimension:
            parsed = snap_to_period_start(self.time_dimension, parsed)

        # state bookmarks need to be reformatted for API requests
        return date.strftime(parsed, "%Y-%m-%d")

    def _report_for_property(self, property_id: str) -> dict | None:
        """Return the report without the fields this property does not have.

        Custom dimensions/metrics only exist on some properties. Fields are
        dropped only when the property's metadata was fetched; they are emitted
        as null. Returns None when no metric is left to request.
        """
        metadata = self.property_metadata.get(property_id)
        if metadata is None:
            return self.report

        property_dimensions, property_metrics = metadata
        dimensions = [
            d
            for d in self.report["dimensions"]
            if d in property_dimensions or d not in self.dimensions_ref
        ]
        metrics = [
            m
            for m in self.report["metrics"]
            if m in property_metrics or m not in self.metrics_ref
        ]
        dropped = [d for d in self.report["dimensions"] if d not in dimensions] + [
            m for m in self.report["metrics"] if m not in metrics
        ]
        if not dropped:
            return self.report

        if property_id not in self._logged_dropped_fields:
            self._logged_dropped_fields.add(property_id)
            self.logger.warning(
                "Report %s: property %s does not have %s; requesting without them "
                "and emitting them as null.",
                self.name,
                property_id,
                ", ".join(dropped),
            )
        if not metrics:
            self.logger.warning(
                "Report %s: property %s has none of the report's metrics; skipping it.",
                self.name,
                property_id,
            )
            return None

        report = dict(self.report)
        report["dimensions"] = dimensions
        report["metrics"] = metrics
        return report

    def _request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Request records from multiple properties."""

        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.

        Raises:
            RuntimeError: If a loop in pagination is detected. That is, when two
                consecutive pagination tokens are identical.
        """

        property_ids = get_configured_property_ids(self.config)

        state_filter = self._get_state_filter(context)

        # Fetch data for each property; skip properties we no longer have access to
        for property_id in property_ids:
            self.property_id = property_id  # Set current property_id for API calls
            property_report = self._report_for_property(property_id)
            if property_report is None:
                continue
            api_report_def = self._generate_report_definition(property_report)
            next_page_token: t.Any = None
            finished = False

            try:
                while not finished:
                    resp = self._request_data(
                        api_report_def,
                        state_filter=state_filter,
                        next_page_token=next_page_token,
                    )

                    yield from self._parse_response(resp)

                    previous_token = copy.deepcopy(next_page_token)
                    next_page_token = self._get_next_page_token(
                        response=resp, previous_token=previous_token
                    )
                    if next_page_token and next_page_token == previous_token:
                        msg = (
                            f"Loop detected in pagination. "
                            f"Pagination token {next_page_token} is identical to prior token."
                        )
                        raise RuntimeError(msg)
                    finished = not next_page_token
            except Exception as e:
                if is_permission_denied_error(e):
                    self.logger.warning(
                        "Skipping property ID %s (no access): %s",
                        property_id,
                        e,
                    )
                    continue
                raise

    def _get_next_page_token(self, response: RunReportResponse, previous_token) -> t.Any:
        """Get the next page token from a response.

        Args:
            response: The response from the API.
            previous_token: The previous page token.

        Returns:
            The next page token, or None if there are no more pages.
        """
        previous_token = previous_token or 0
        next_token = previous_token + 1
        total_rows = response.row_count
        return next_token if total_rows >= next_token * self.page_size else None

    def _parse_response(self, response):
        if not response:
            return
        dimensionHeaders = [d.name for d in response.dimension_headers]  # noqa: N806
        metricHeaders = [mh.name for mh in response.metric_headers]  # noqa: N806

        for row in response.rows:
            record = {}
            dimensions = [d.value for d in row.dimension_values]
            dateRangeValues = row.metric_values  # noqa: N806

            for header, dimension in zip(dimensionHeaders, dimensions):
                data_type = self._lookup_data_type(
                    "dimension", header, self.dimensions_ref, self.metrics_ref
                )

                # Convert empty strings to "(no value)", preserve "(not set)" values
                if not dimension and dimension != "(not set)":
                    value = "(no value)"
                elif data_type == "integer":
                    value = int(dimension)
                elif data_type == "number":
                    value = float(dimension)
                else:
                    value = dimension

                record[header] = value

            for metric_name, value in zip(metricHeaders, dateRangeValues):
                metric_type = self._lookup_data_type(
                    "metric", metric_name, self.dimensions_ref, self.metrics_ref
                )

                if hasattr(value, "value"):
                    value = value.value  # noqa: PLW2901

                # Convert empty strings to "(no value)", preserve "(not set)" values for metrics
                if not value and value != "(not set)":
                    value = "(no value)"
                elif metric_type == "integer":
                    value = int(value)  # noqa: PLW2901
                elif metric_type == "number":
                    value = float(value)  # noqa: PLW2901

                record[metric_name] = value

            # Fields this property does not have were not requested
            for field in (*self.report["dimensions"], *self.report["metrics"]):
                record.setdefault(field, None)

            # Weekly / monthly / hourly reports also get the period's first day
            # as `date` so loaders can partition and replace by date
            if self.time_dimension and self.time_dimension != "date":
                try:
                    record["date"] = period_start(
                        self.time_dimension, record[self.time_dimension]
                    ).strftime("%Y%m%d")
                except (TypeError, ValueError):
                    record["date"] = None

            # Add the property_id to each record. This is used to identify the property in the API response so that we can load multiple properties in the same sync eventually.
            record["property_id"] = f"properties/{self.property_id}"

            # Also add the [start_date,end_date) used for the report
            record["report_start_date"] = self.config.get("start_date")
            record["report_end_date"] = self.end_date

            # Add run_id to every record
            record["run_id"] = self.run_id

            yield record

    def _query_api(self, report_definition, state_filter, pageToken=None) -> RunReportResponse:
        """Run one page of the report for the current property.

        QuotaManager retries quota waits, rate limits, server errors and dropped connections.
        When it gives up, a ``REQUEST FAILED {json}`` line says which request failed and what
        Google returned (property, stream, date range, offset, HTTP / gRPC status, reason,
        attempts), and the error raised names the property.
        """
        offset = (pageToken or 0) * self.page_size
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=report_definition["dimensions"],
            metrics=report_definition["metrics"],
            date_ranges=[DateRange(start_date=state_filter, end_date=self.end_date)],
            limit=self.page_size,
            metric_filter=report_definition["metricFilter"],
            dimension_filter=report_definition["dimensionFilter"],
            offset=offset,
            return_property_quota=True,
        )
        label = (
            f"property {self.property_id}, stream {self.name}, "
            f"{state_filter}..{self.end_date}, offset {offset}"
        )

        try:
            return self.quota_manager.call(self.analytics.run_report, request, label=label)
        except Exception as e:
            if is_permission_denied_error(e):
                # _request_records skips the property; not a failure
                raise RuntimeError(f"Property ID '{self.property_id}': {e}") from e  # noqa: TRY003
            detail = {
                "tap": "tap-google-analytics",
                "stream": self.name,
                "account_id": str(self.property_id),
                "start_date": state_filter,
                "end_date": self.end_date,
                "offset": offset,
                "limit": self.page_size,
                "attempts": self.quota_manager.last_attempts,
                "seconds": round(self.quota_manager.last_seconds, 1),
                **describe_error(e),
            }
            # One line with no traceback: the caller (e.g. Aria's singer-connector) reads it.
            self.logger.error("REQUEST FAILED %s", json.dumps(detail, sort_keys=True))  # noqa: TRY400
            if is_quota_error(e):
                raise RuntimeError(
                    f"Exceeded maximum retries waiting for GA4 quota reset on property "
                    f"'{self.property_id}'. Last error: {e}"
                ) from e
            raise RuntimeError(f"Property ID '{self.property_id}': {e}") from e

    @staticmethod
    def _get_datatype(string_type):
        mapping = {
            "string": th.StringType(),
            "integer": th.IntegerType(),
            "number": th.NumberType(),
        }
        return mapping.get(string_type, th.StringType())

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.

        """
        yield from self._request_records(context)

    @functools.cached_property
    def schema(self) -> dict:
        """Return dictionary of record schema.

        Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved.
        """
        properties: list[th.Property] = []
        primary_keys = []
        # : List[th.StringType] = []

        # The time dimension (date, dateHour, yearWeek, ...) is the replication key
        if self.time_dimension:
            self.replication_key = self.time_dimension

        # Fields missing on some property are null in that property's records
        partial_fields = set()
        for property_dimensions, _property_metrics in self.property_metadata.values():
            partial_fields.update(
                d for d in self.report["dimensions"] if d not in property_dimensions
            )

        # Add the dimensions to the schema and as key_properties
        for dimension in self.report["dimensions"]:
            data_type = self._lookup_data_type(
                "dimension", dimension, self.dimensions_ref, self.metrics_ref
            )
            properties.append(
                th.Property(
                    dimension,
                    self._get_datatype(data_type),
                    required=dimension not in partial_fields,
                )
            )
            primary_keys.append(dimension)

        # Add the metrics to the schema
        for metric in self.report["metrics"]:
            data_type = self._lookup_data_type(
                "metric", metric, self.dimensions_ref, self.metrics_ref
            )
            properties.append(th.Property(metric, self._get_datatype(data_type)))

        # First day of the period (YYYYMMDD) for non-daily time dimensions
        if self.time_dimension and self.time_dimension != "date":
            properties.append(th.Property("date", th.StringType()))

        # Add property_id to primary keys once
        primary_keys.append("property_id")

        properties.extend(
            (
                th.Property("property_id", th.StringType(), required=True),
                th.Property("report_start_date", th.StringType(), required=True),
                th.Property("report_end_date", th.StringType(), required=True),
                th.Property("run_id", th.IntegerType(), required=True),
            )
        )
        # Without a time dimension, add the {start_date, end_date} params as keys
        if not self.time_dimension:
            self.logger.warning(
                "Incremental sync not supported for stream %s: it has none of the time "
                "dimensions date, dateHour, yearWeek, isoYearIsoWeek, yearMonth.",
                self.tap_stream_id,
            )
            primary_keys.extend(("report_start_date", "report_end_date"))
        self.primary_keys = primary_keys
        return th.PropertiesList(*properties).to_dict()
