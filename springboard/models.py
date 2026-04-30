"""Data transformations for Springboard (Heartland Retail) connector.

Pure input/output: maps raw API ticket JSON to flat row dicts matching
the Fivetran schema. No side effects, no HTTP, no Fivetran operations.
"""

from typing import Any, Optional


def _safe_str(value: Any) -> Optional[str]:
    """Cast to string if not None. IDs should be STRING per repo convention."""
    return str(value) if value is not None else None


def format_ticket(raw: dict) -> dict:
    """Transform a raw sales ticket from the API into a Fivetran-compatible row.

    Maps top-level ticket fields. Line items are deferred to a future table.

    Args:
        raw: Single ticket dict from API results array.

    Returns:
        Flat dict matching the 'tickets' schema columns.
    """
    return {
        "id": _safe_str(raw.get("id")),
        "type": raw.get("type"),
        "status": raw.get("status"),
        "total": raw.get("total"),
        "customer_id": _safe_str(raw.get("customer_id")),
        "source_location_id": _safe_str(raw.get("source_location_id")),
        "station_id": _safe_str(raw.get("station_id")),
        "parent_transaction_id": _safe_str(raw.get("parent_transaction_id")),
        "created_by_user_id": _safe_str(raw.get("created_by_user_id")),
        "updated_by_user_id": _safe_str(raw.get("updated_by_user_id")),
        "coupon_id": _safe_str(raw.get("coupon_id")),
        "completed_at": raw.get("completed_at"),
        "local_created_at": raw.get("local_created_at"),
        "local_updated_at": raw.get("local_updated_at"),
    }
