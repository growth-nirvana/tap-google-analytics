"""Each property keeps its own bookmark, so a property added to a connection starts at start_date
instead of the stream's bookmark (Aria gn-947291-ga4-8a53c1, 2026-11-24: Lightyear was swapped in
for Bluescape and only got the stream bookmark's 30-day lookback, from 2026-10-24)."""

from __future__ import annotations

from singer_sdk._singerlib import StateMessage

from tap_google_analytics.client import BOOKMARKS_KEY, PROGRESS_KEY
from tests.test_date_windows import _sync


def _first_requests(requests):
    firsts = {}
    for prop, start, _ in requests:
        firsts.setdefault(prop, start)
    return firsts


def _last_state(messages):
    return [m for m in messages if isinstance(m, StateMessage)][-1].value["bookmarks"]["r"]


def test_a_new_property_starts_at_start_date_and_a_known_one_at_its_bookmark():
    state = {"replication_key": "date", "replication_key_value": "20240401", BOOKMARKS_KEY: {"1": "2024-04-01"}}
    _, requests, error = _sync({"start_date": "2024-01-01", "end_date": "2024-04-10"}, state=state)
    assert error is None
    # Property 1: its bookmark less the 30-day lookback. Property 2 has none: start_date.
    assert _first_requests(requests) == {"properties/1": "2024-03-02", "properties/2": "2024-01-01"}


def test_every_property_gets_its_own_bookmark_and_keeps_it_after_the_stream_finishes():
    messages, _, error = _sync({"start_date": "2024-01-15", "end_date": "2024-03-10"})
    assert error is None
    last = _last_state(messages)
    assert last[BOOKMARKS_KEY] == {"1": "2024-03-10", "2": "2024-03-10"}
    assert PROGRESS_KEY not in last


def test_state_from_before_per_property_bookmarks_pulls_nothing_again():
    """The upgrade run gives the stream's bookmark to every property configured now."""
    state = {"replication_key": "date", "replication_key_value": "20240401"}
    messages, requests, error = _sync({"start_date": "2024-01-01", "end_date": "2024-04-10"}, state=state)
    assert error is None
    assert _first_requests(requests) == {"properties/1": "2024-03-02", "properties/2": "2024-03-02"}
    # The first STATE already holds both, so a run that stops early doesn't re-pull either.
    first = [m for m in messages if isinstance(m, StateMessage)][0].value["bookmarks"]["r"]
    assert set(first[BOOKMARKS_KEY]) == {"1", "2"}


def test_backfill_properties_start_at_start_date_whatever_their_bookmark():
    state = {"replication_key": "date", "replication_key_value": "20240401"}
    _, requests, _ = _sync(
        {"start_date": "2024-01-01", "end_date": "2024-04-10", "backfill_properties": ["properties/2"]}, state=state
    )
    assert _first_requests(requests) == {"properties/1": "2024-03-02", "properties/2": "2024-01-01"}

    state = {"replication_key": "date", BOOKMARKS_KEY: {"1": "2024-04-01", "2": "2024-04-01"}}
    _, requests, _ = _sync({"start_date": "2024-01-01", "end_date": "2024-04-10", "backfill_properties": ["1"]}, state=state)
    assert _first_requests(requests) == {"properties/1": "2024-01-01", "properties/2": "2024-03-02"}


def test_a_property_removed_from_the_config_keeps_its_bookmark_for_when_it_comes_back():
    state = {"replication_key": "date", BOOKMARKS_KEY: {"1": "2024-04-01", "2": "2024-04-01", "9": "2024-02-01"}}
    messages, _, _ = _sync({"start_date": "2024-01-01", "end_date": "2024-04-10"}, state=state)
    assert _last_state(messages)[BOOKMARKS_KEY]["9"] == "2024-02-01"
