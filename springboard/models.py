"""Data transformations for Springboard (Jackson River) connector.

Pure input/output: maps raw API JSON to flat row dicts matching
the Fivetran schema. No side effects, no HTTP, no Fivetran operations.
"""

import json
from typing import Any, Optional


def _safe_str(value: Any) -> Optional[str]:
    """Cast to string if not None. IDs should be STRING per repo convention."""
    return str(value) if value is not None else None


def _serialize_nested(obj: Any) -> Optional[str]:
    """Serialize nested objects to JSON string for storage in STRING columns."""
    if obj is None:
        return None
    if isinstance(obj, str):
        return obj
    return json.dumps(obj)


def format_donation_form(raw: dict) -> dict:
    """Transform a donation form summary from the list endpoint into a row.

    Args:
        raw: Single form dict from the list endpoint.
             Shape: { nid, type, title, internal_name }

    Returns:
        Flat dict matching the 'donation_forms' schema columns.
    """
    return {
        "nid": _safe_str(raw.get("nid")),
        "type": raw.get("type"),
        "title": raw.get("title"),
        "internal_name": raw.get("internal_name"),
    }


def format_donation_form_detail(raw: dict) -> dict:
    """Transform a full form detail response into a row.

    Includes the summary fields plus body text and serialized fields metadata.

    Args:
        raw: Full form detail dict from the detail endpoint.

    Returns:
        Flat dict matching the 'donation_forms' schema columns.
    """
    body_data = raw.get("body", {})
    body_und = body_data.get("und", [{}]) if isinstance(body_data, dict) else [{}]
    body_text = body_und[0].get("safe_value") if body_und else None

    return {
        "nid": _safe_str(raw.get("nid")),
        "type": raw.get("type"),
        "title": raw.get("title"),
        "internal_name": raw.get("internal_name"),
        "body": body_text,
        "fields": _serialize_nested(raw.get("fields")),
        "token": raw.get("token"),
    }
