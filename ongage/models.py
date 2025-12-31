"""Data models and transformations for OnGage connector."""

import csv
import io
from datetime import datetime
from typing import Optional


def parse_datetime(value) -> Optional[str]:
    """Parse datetime from various formats (unix timestamp or datetime string)."""
    if value is None or value == "" or value == "null":
        return None
    try:
        # Try parsing as datetime string first (YYYY-MM-DD HH:MM:SS)
        if isinstance(value, str) and "-" in value:
            dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            return dt.isoformat() + "Z"
        # Try as unix timestamp
        return datetime.utcfromtimestamp(int(value)).isoformat() + "Z"
    except (ValueError, TypeError):
        return None


def parse_contact_row(row: dict, list_id: str) -> dict:
    """Transform a CSV row into a contact record."""
    return {
        # List identifier
        "list_id": list_id,
        # Core identifiers
        "id": row.get("ocx_contact_id", row.get("id", "")),
        "email": row.get("email", ""),
        # Personal info
        "first_name": row.get("first_name", ""),
        "last_name": row.get("last_name", ""),
        "gender": row.get("gender", ""),
        "age_range": row.get("age_range", ""),
        "birth_year": row.get("birth_year", ""),
        # Location
        "address": row.get("address", ""),
        "city": row.get("city", ""),
        "state": row.get("state", ""),
        "zip_code": row.get("zip_code", ""),
        "country": row.get("country", ""),
        # Contact info
        "phone": row.get("phone", ""),
        "language": row.get("language", ""),
        # Technical
        "ip": row.get("ip", ""),
        "os": row.get("os", ""),
        "product_id": row.get("product_id", ""),
        # OnGage system fields (ocx_*)
        "status": row.get("ocx_status", ""),
        "created_date": parse_datetime(row.get("ocx_created_date")),
        "unsubscribe_date": parse_datetime(row.get("ocx_unsubscribe_date")),
        "resubscribe_date": parse_datetime(row.get("ocx_resubscribe_date")),
        "bounce_date": parse_datetime(row.get("ocx_bounce_date")),
        "complaint_date": parse_datetime(row.get("ocx_complaint_date")),
        "import_id": row.get("ocx_import_id", ""),
    }


def parse_contacts_csv(csv_content: str, list_id: str) -> list[dict]:
    """Parse CSV content into a list of contact records."""
    reader = csv.DictReader(io.StringIO(csv_content))
    return [parse_contact_row(row, list_id) for row in reader]


def format_list_data(lst: dict) -> dict:
    """Transform a list API response into a list record for upsert."""
    return {
        "id": str(lst.get("id", "")),
        "name": lst.get("name", ""),
        "description": lst.get("description", ""),
        "type": lst.get("type", ""),
        "scope": lst.get("scope", ""),
        "last_count": lst.get("last_count"),
        "last_active_count": lst.get("last_active_count"),
        "unsubscribes": lst.get("unsubscribes"),
        "complaints": lst.get("complaints"),
        "bounces": lst.get("bounces"),
        "archive": lst.get("archive", False),
        "default": lst.get("default", False),
        "account_id": str(lst.get("account_id", "")),
        "created": parse_datetime(lst.get("created")),
        "modified": parse_datetime(lst.get("modified")),
    }
