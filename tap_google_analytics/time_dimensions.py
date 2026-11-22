"""GA4 time dimensions used as replication keys."""

from __future__ import annotations

from datetime import date, timedelta

# In priority order: a report's time dimension is the first of these it requests.
TIME_DIMENSIONS = ("date", "dateHour", "yearWeek", "isoYearIsoWeek", "yearMonth")


def get_time_dimension(dimensions: list[str]) -> str | None:
    """Return the report's time dimension, or None if it has none."""
    for time_dimension in TIME_DIMENSIONS:
        if time_dimension in dimensions:
            return time_dimension
    return None


def _year_week_start(year: int, week: int) -> date:
    """First day of a GA4 ``yearWeek`` week.

    GA4 weeks start on Sunday and January 1st is always in week 01, so the
    first week of the year usually has fewer than 7 days.
    """
    jan_1 = date(year, 1, 1)
    # Sunday on or before Jan 1 (weekday(): Monday == 0, Sunday == 6)
    first_sunday = jan_1 - timedelta(days=(jan_1.weekday() + 1) % 7)
    return max(jan_1, first_sunday + timedelta(weeks=week - 1))


def period_start(time_dimension: str, value: str) -> date:
    """Return the first day of the period a time dimension value stands for.

    Raises:
        ValueError: if the value does not match the dimension's format.
    """
    value = str(value)
    if time_dimension == "date":
        if len(value) == 8 and value.isdigit():  # noqa: PLR2004
            return date(int(value[:4]), int(value[4:6]), int(value[6:8]))
        # ISO dates / datetimes (config start_date, older bookmarks)
        return date.fromisoformat(value[:10])
    if not value.isdigit():
        raise ValueError(f"Unexpected {time_dimension} value: {value!r}")  # noqa: TRY003
    if time_dimension == "dateHour":
        return date(int(value[:4]), int(value[4:6]), int(value[6:8]))
    if time_dimension == "yearWeek":
        return _year_week_start(int(value[:4]), int(value[4:6]))
    if time_dimension == "isoYearIsoWeek":
        return date.fromisocalendar(int(value[:4]), int(value[4:6]), 1)
    if time_dimension == "yearMonth":
        return date(int(value[:4]), int(value[4:6]), 1)
    raise ValueError(f"Unsupported time dimension: {time_dimension}")  # noqa: TRY003


def snap_to_period_start(time_dimension: str, day: date) -> date:
    """Move a date back to the first day of the period that contains it.

    Requests must start on a period boundary, otherwise GA4 returns a partial
    first week / month that would overwrite the complete one downstream.
    """
    if time_dimension == "yearWeek":
        week_start = day - timedelta(days=(day.weekday() + 1) % 7)
        return max(week_start, date(day.year, 1, 1))
    if time_dimension == "isoYearIsoWeek":
        return day - timedelta(days=day.weekday())
    if time_dimension == "yearMonth":
        return day.replace(day=1)
    return day
