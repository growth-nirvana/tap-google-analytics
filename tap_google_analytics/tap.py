"""GoogleAnalytics tap class."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import GetMetadataRequest

# OAuth - Google Analytics Authorization
# from google.oauth2.credentials import Credentials as ServiceAccountCredentials  # noqa: ERA001
from google.oauth2 import service_account

# Service Account - Google Analytics Authorization
from google.oauth2.credentials import Credentials as OAuthCredentials
from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from datetime import datetime, timedelta, timezone

from tap_google_analytics.client import GoogleAnalyticsStream
from tap_google_analytics.error import is_permission_denied_error, is_quota_error
from tap_google_analytics.quota_manager import QuotaManager

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]

LOGGER = logging.getLogger(__name__)


class TapGoogleAnalytics(Tap):
    """Singer tap for extracting data from the Google Analytics Data API (GA4)."""

    name = "tap-google-analytics"
    _end_date = datetime.now(timezone.utc).date()
    _start_date = _end_date - timedelta(days=30)

    config_jsonschema = th.PropertiesList(
        th.Property(
            "property_id",
            th.StringType,
            description="Google Analytics Property ID",
        ),
        th.Property(
            "property_ids",
            th.StringType,
            description="List of Google Analytics Property IDs",
            required=True,
        ),
        # Service Account
        th.Property(
            "client_secrets",
            th.ObjectType(),
            description="Google Analytics Client Secrets Dictionary",
        ),
        th.Property(
            "key_file_location",
            th.StringType,
            description="File Path to Google Analytics Client Secrets",
        ),
        # OAuth
        th.Property(
            "oauth_credentials",
            th.ObjectType(
                th.Property(
                    "refresh_token",
                    th.StringType,
                    description="Google Analytics Refresh Token",
                ),
                th.Property(
                    "client_id",
                    th.StringType,
                    description="Google Analytics Client ID",
                ),
                th.Property(
                    "client_secret",
                    th.StringType,
                    description="Google Analytics Client Secret",
                ),
            ),
            description="Google Analytics OAuth Credentials",
        ),
        # Optional
        th.Property(
            "reports",
            th.StringType,
            description="A path to a file containing the Google Analytics reports definitions",
        ),
        th.Property(
            "reports_list",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "name",
                        th.StringType,
                        description="Google Analytics Report name",
                    ),
                    th.Property(
                        "dimensions",
                        th.ArrayType(th.StringType),
                        description="Google Analytics Dimensions",
                    ),
                    th.Property(
                        "metrics",
                        th.ArrayType(th.StringType),
                        description="Google Analytics Metrics",
                    ),
                ),
            ),
            description="List of Google Analytics Reports Definitions",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
            default=_start_date.isoformat()
        ),
        th.Property(
            "end_date",
            th.StringType,
            description="The last record date to sync",
            default=_end_date.isoformat(),
        ),
        th.Property(
            "request_delay_seconds",
            th.NumberType,
            description=(
                "Minimum seconds to wait between GA4 API requests. Helps avoid "
                "burning the shared project hourly token quota."
            ),
            default=0.5,
        ),
        th.Property(
            "quota_max_hourly_retries",
            th.IntegerType,
            description=(
                "How many times to wait for the next clock hour when the GA4 "
                "hourly token quota is exhausted during sync."
            ),
            default=24,
        ),
        th.Property(
            "quota_metadata_max_hourly_retries",
            th.IntegerType,
            description=(
                "How many hourly quota waits to attempt while fetching GA4 "
                "metadata during discovery. Default 0 keeps discover fast: after "
                "the first hourly-token error, field types are inferred from the "
                "report definition so catalog generation can still succeed. "
                "Set to 1 to wait until the next hour for live metadata instead."
            ),
            default=0,
        ),
    ).to_dict()

    def _initialize_credentials(self):
        if self.config.get("client_id") and self.config.get("client_secret") and self.config.get("refresh_token"):
            return OAuthCredentials.from_authorized_user_info(
                {
                    "client_id": self.config["client_id"],
                    "client_secret": self.config["client_secret"],
                    "refresh_token": self.config["refresh_token"],
                }
            )

        if self.config.get("key_file_location"):
            with open(self.config["key_file_location"]) as f:  # noqa: PTH123
                return service_account.Credentials.from_service_account_info(json.load(f))

        if self.config.get("client_secrets"):
            return service_account.Credentials.from_service_account_info(
                self.config["client_secrets"]
            )

        raise RuntimeError("No valid credentials provided.")  # noqa: TRY003

    def _initialize_analytics(self):
        """Initialize an Analytics Reporting API V4 service object.

        Returns:
          An authorized Analytics Reporting API V4 service object.

        """
        return BetaAnalyticsDataClient(credentials=self.credentials)

    def _get_reports_config(self):
        if self.config.get("reports_list"):
            return self.config["reports_list"]

        default_reports = Path(__file__).parent.joinpath(
            "defaults", "default_report_definition.json"
        )

        report_def_file = self.config.get("reports", default_reports)
        if Path(report_def_file).is_file():
            try:
                with open(report_def_file) as f:  # noqa: PTH123
                    return json.load(f)
            except ValueError:
                self.logger.critical("The JSON definition in '%s' has errors", report_def_file)
                sys.exit(1)
        else:
            self.logger.critical("'%s' file not found", report_def_file)
            sys.exit(1)

    def _fetch_valid_api_metadata(self) -> tuple[dict, dict]:
        """Fetch the valid (dimensions, metrics) for the Analytics Reporting API.

        Tries each configured property in turn; skips properties without access
        and uses the first one that succeeds. Metadata is the same for all GA4
        properties.

        Returns:
          A map of (dimensions, metrics) hashes

          Each available dimension can be found in dimensions with its data type
            as the value. e.g. dimensions['userType'] == STRING

          Each available metric can be found in metrics with its data type
            as the value. e.g. metrics['sessions'] == INTEGER

        """
        property_ids = []
        if self.config.get("property_ids"):
            property_ids = [pid.strip() for pid in self.config["property_ids"].split(",")]
        if self.config.get("property_id"):
            pid = self.config["property_id"].strip()
            if pid not in property_ids:
                property_ids.append(pid)
        if not property_ids:
            raise RuntimeError("No valid property ID provided")

        metadata_hourly_retries = int(self.config.get("quota_metadata_max_hourly_retries", 0))
        last_error = None
        for property_id in property_ids:
            request = GetMetadataRequest(name=f"properties/{property_id}/metadata")
            try:
                results = self.quota_manager.call(
                    self.analytics.get_metadata,
                    request,
                    max_hourly_retries=metadata_hourly_retries,
                )
            except Exception as e:
                last_error = e
                if is_permission_denied_error(e):
                    self.logger.warning(
                        "Skipping property ID %s (no access): %s",
                        property_id,
                        e,
                    )
                    continue
                if is_quota_error(e):
                    self.logger.warning(
                        "GA4 quota exhausted fetching metadata for property %s: %s",
                        property_id,
                        e,
                    )
                    continue
                raise RuntimeError(
                    f"Property ID '{property_id}': {e}"
                ) from e

            LOGGER.debug("**** metadata for %s", property_id)
            LOGGER.debug(results)

            metrics = {
                metric.api_name: metric.type_.name.replace("TYPE_", "").lower()
                for metric in results.metrics
            }
            dimensions = {dimension.api_name: "string" for dimension in results.dimensions}
            return dimensions, metrics

        if last_error is not None and is_quota_error(last_error):
            self.logger.warning(
                "Could not fetch GA4 metadata due to quota limits (tried %s). "
                "Inferring field types from the report definition so discovery can continue. "
                "Last error: %s",
                ", ".join(property_ids),
                last_error,
            )
            return self._infer_metadata_from_reports()

        raise RuntimeError(
            "No property accessible. Tried: %s. Last error: %s"
            % (", ".join(property_ids), last_error)
        )

    @staticmethod
    def _infer_metric_type(metric_name: str) -> str:
        """Best-effort GA4 metric type when the Metadata API is unavailable."""
        normalized = metric_name.lower().replace("_", "")
        number_markers = (
            "rate",
            "revenue",
            "value",
            "average",
            "avg",
            "duration",
            "percent",
            "ratio",
            "peruser",
            "persession",
        )
        if any(marker in normalized for marker in number_markers):
            return "number"
        return "integer"

    def _infer_metadata_from_reports(self) -> tuple[dict, dict]:
        """Build dimension/metric type maps from the configured report definitions."""
        reports = getattr(self, "reports_definition", None) or self._get_reports_config()
        dimensions: dict[str, str] = {}
        metrics: dict[str, str] = {}
        for report in reports:
            for dimension in report.get("dimensions", []):
                dimensions[dimension] = "string"
            for metric in report.get("metrics", []):
                metrics[metric] = self._infer_metric_type(metric)
        return dimensions, metrics

    def _validate_report_def(self, reports_definition):
        for report in reports_definition:
            try:
                name = report["name"]
                dimensions = report["dimensions"]
                metrics = report["metrics"]
            except KeyError:
                self.logger.critical(
                    "Report definition is missing one of the required properties \
                    (name, dimensions, metrics)"
                )
                sys.exit(1)

            # Check that not too many metrics && dimensions have been requested
            if len(metrics) == 0:
                self.logger.critical(
                    "'%s' has no metrics defined. GA reports must specify at least one metric.",
                    name,
                )
                sys.exit(1)
            elif len(metrics) > 10:  # noqa: PLR2004
                self.logger.critical(
                    "'%s' has too many metrics defined. GA reports can have maximum 10 metrics.",
                    name,
                )
                sys.exit(1)

            if len(dimensions) > 9:  # noqa: PLR2004
                self.logger.critical(
                    "'%s' has too many dimensions defined. GA reports can have maximum 9 "
                    "dimensions.",
                    name,
                )
                sys.exit(1)

            self._validate_dimensions(dimensions)
            self._validate_metrics(metrics)

            # ToDo: We should also check that the given metrics can be used
            #  with the given dimensions
            # Not all dimensions and metrics can be queried together. Only certain
            #  dimensions and metrics can be used together to create valid combinations.

    def _validate_dimensions(self, dimensions):
        # check that all the dimensions are proper Google Analytics Dimensions
        for dimension in dimensions:
            if dimension not in self.dimensions_ref:
                self.logger.critical("'%s' is not a valid Google Analytics dimension", dimension)
                self.logger.info(
                    "For details see https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema"
                )
                sys.exit(1)

    def _validate_metrics(self, metrics):
        # check that all the metrics are proper Google Analytics metrics
        for metric in metrics:
            if metric.startswith("goal") and metric.endswith(
                (
                    "Starts",
                    "Completions",
                    "Value",
                    "ConversionRate",
                    "Abandons",
                    "AbandonRate",
                )
            ):
                # Custom Google Analytics Metrics {goalXXStarts, goalXXValue, ...}
                continue

            if metric.startswith("searchGoal") and metric.endswith("ConversionRate"):
                # Custom Google Analytics Metrics searchGoalXXConversionRate
                continue

            if not metric.startswith(("metric", "calcMetric")) and metric not in self.metrics_ref:
                self.logger.critical("'%s' is not a valid Google Analytics metric", metric)
                self.logger.info(
                    "For details see https://ga-dev-tools.google/ga4/\
                        dimensions-metrics-explorer/"
                )
                sys.exit(1)

    def _custom_initialization(self):
        # init GA client
        self.credentials = self._initialize_credentials()
        self.analytics = self._initialize_analytics()
        self.quota_manager = QuotaManager(
            min_request_interval=float(self.config.get("request_delay_seconds", 0.5)),
            max_hourly_retries=int(self.config.get("quota_max_hourly_retries", 24)),
        )
        # Load reports first so metadata fetch can fall back to inferred types.
        self.reports_definition = self._get_reports_config()
        self.dimensions_ref, self.metrics_ref = self._fetch_valid_api_metadata()
        self._validate_report_def(self.reports_definition)

    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        # Generate and return the catalog
        self._custom_initialization()
        stream_list: list[Stream] = []

        for report in self.reports_definition:
            stream = GoogleAnalyticsStream(
                tap=self,
                name=report["name"],
                ga_report=report,
                ga_dimensions_ref=self.dimensions_ref,
                ga_metrics_ref=self.metrics_ref,
                ga_analytics_client=self.analytics,
            )
            stream_list.append(stream)
        return stream_list


if __name__ == "__main__":
    TapGoogleAnalytics.cli()
