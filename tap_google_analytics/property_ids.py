"""Resolve configured GA4 property IDs from tap config."""

from __future__ import annotations


def get_configured_property_ids(config: dict) -> list[str]:
    """Return unique property IDs from ``property_ids`` and/or ``property_id``.

    Order is preserved: IDs from ``property_ids`` (comma-separated) first, then
    ``property_id`` if it was not already listed. Empty segments are ignored.
    """
    property_ids: list[str] = []
    seen: set[str] = set()

    def add(raw: str) -> None:
        cleaned = raw.strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            property_ids.append(cleaned)

    raw_list = config.get("property_ids")
    if raw_list:
        for part in str(raw_list).split(","):
            add(part)

    raw_single = config.get("property_id")
    if raw_single:
        add(str(raw_single))

    return property_ids
