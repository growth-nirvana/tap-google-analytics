"""Tests for configured property ID resolution."""

from __future__ import annotations

from tap_google_analytics.property_ids import get_configured_property_ids


def test_single_property_id_only():
    assert get_configured_property_ids({"property_id": "382954539"}) == ["382954539"]


def test_property_ids_only():
    assert get_configured_property_ids({"property_ids": "111, 222"}) == ["111", "222"]


def test_same_value_in_both_fields_deduped():
    config = {"property_ids": "382954539", "property_id": "382954539"}
    assert get_configured_property_ids(config) == ["382954539"]


def test_property_id_appended_when_not_in_list():
    config = {"property_ids": "111", "property_id": "222"}
    assert get_configured_property_ids(config) == ["111", "222"]


def test_duplicates_within_property_ids_deduped():
    assert get_configured_property_ids({"property_ids": "111,111, 222"}) == ["111", "222"]


def test_strips_whitespace_on_property_id():
    config = {"property_ids": "111", "property_id": " 222 "}
    assert get_configured_property_ids(config) == ["111", "222"]
