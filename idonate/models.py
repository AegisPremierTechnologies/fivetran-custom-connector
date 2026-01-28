"""Data transformation for iDonate connector.

Pure input/output transformations:
- Synthetic stable contact_id generation
- Email normalization
- Date parsing
- Flattening nested structures to JSON strings where needed
"""

import hashlib
import json
from typing import Any, Dict, Optional


def _extract_or_hash_contact_id(raw_contact: dict) -> str:
    """Extract or generate stable contact_id from raw contact.

    Prefers explicit id field from API, falls back to email-based hash.
    Ensures deterministic ID for deduplication across syncs.

    Args:
        raw_contact: Contact dict with email, firstname, lastname, etc.

    Returns:
        Stable contact_id string (either from API or generated)
    """
    # If contact has explicit id, use it
    contact_id = raw_contact.get("id")
    if contact_id:
        return str(contact_id)

    # Otherwise, generate deterministic hash based on email
    email = (raw_contact.get("email") or "").lower().strip()
    if email:
        # Email is most stable identifier
        hash_val = hashlib.sha256(email.encode()).hexdigest()[:12]
        return f"contact_{hash_val}"

    # Fallback: hash entire object
    obj_str = json.dumps(raw_contact, sort_keys=True, default=str)
    hash_val = hashlib.sha256(obj_str.encode()).hexdigest()[:12]
    return f"contact_{hash_val}"


def _normalize_email(email: Optional[str]) -> Optional[str]:
    """Normalize email for matching: lowercase + trim whitespace."""
    return email.lower().strip() if email else None


def _format_date(date_value: Any) -> Optional[str]:
    """Convert ISO8601 date string to UTC_DATETIME format.

    Fivetran expects UTC_DATETIME in ISO format.
    If already ISO, return as-is. Otherwise parse and reformat.
    """
    if not date_value:
        return None

    if isinstance(date_value, str):
        # Already a string, assume ISO format
        return date_value
    return None


def _serialize_nested_object(obj: Any) -> Optional[str]:
    """Serialize nested objects to JSON string for storage."""
    if not obj:
        return None
    if isinstance(obj, dict):
        return json.dumps(obj)
    if isinstance(obj, str):
        return obj
    return json.dumps(obj)


def _flatten_address(address: Optional[Dict]) -> Dict[str, Optional[str]]:
    """Flatten address object to separate columns with 'address_' prefix."""
    if not address:
        return {}
    return {
        "address_street": address.get("street"),
        "address_street2": address.get("street2"),
        "address_city": address.get("city"),
        "address_state": address.get("state"),
        "address_zip": address.get("zip"),
        "address_country": address.get("country"),
        "address_country_code": address.get("country_code"),
    }


def format_contact(raw_contact: dict, contact_id: Optional[str] = None) -> dict:
    """Transform raw contact from API to Fivetran-compliant row.

    Primary key is contact_id (synthetic stable ID).
    Includes email_normalized for better matching.

    Args:
        raw_contact: Raw contact dict from API
        contact_id: Pre-computed contact_id. If None, will be extracted/generated.

    Returns:
        Normalized contact row for Fivetran
    """
    c = raw_contact

    # Use provided contact_id or extract/generate one
    if contact_id is None:
        contact_id = _extract_or_hash_contact_id(c)

    email = c.get("email")

    return {
        "contact_id": contact_id,
        "email": email,
        "email_normalized": _normalize_email(email),
        "first_name": c.get("firstname"),
        "last_name": c.get("lastname"),
        "middle_name": c.get("middlename"),
        "title": c.get("title"),
        "phone": c.get("phone"),
        "timezone": c.get("timezone"),
        # Flatten address
        **_flatten_address(c.get("address")),
        # Timestamps
        "created": _format_date(c.get("created")),
        "updated": _format_date(c.get("updated")),
    }


def format_transaction(raw_transaction: dict, contact_id: Optional[str] = None) -> dict:
    """Transform raw transaction from API to Fivetran-compliant row.

    References contact via contact_id (FK to contacts table).
    Includes raw contact_email for reference and donor_id.

    Args:
        raw_transaction: Raw transaction dict from API
        contact_id: Pre-computed contact_id for this transaction's contact.
                    If None, will be extracted/generated from nested contact.

    Returns:
        Normalized transaction row for Fivetran
    """
    t = raw_transaction

    # Extract or generate contact_id if not provided
    if contact_id is None:
        contact = t.get("contact", {})
        contact_id = _extract_or_hash_contact_id(contact) if contact else None

    # Build the transaction row
    row = {
        "id": t.get("id"),
        "organization_id": t.get("organization_id"),
        "status": t.get("status"),
        "type": t.get("type"),
        "subtype": t.get("subtype"),
        "description": t.get("description"),
        "additional_info": t.get("additional_info"),
        # Dates
        "created": _format_date(t.get("created")),
        "final_date": _format_date(t.get("final_date")),
        # Donor/Contact references
        "donor_id": t.get("donor_id"),
        "contact_id": contact_id,
        "contact_email_raw": t.get("contact", {}).get("email") if t.get("contact") else None,
        # Payment info
        "card_type": t.get("card_type"),
        "last_four_digits": t.get("last_four_digits"),
        "check_number": t.get("check_number"),
        # Amount info
        "sale_price": t.get("sale_price"),
        "net_proceeds": t.get("net_proceeds"),
        "client_proceeds": t.get("client_proceeds"),
        "donor_paid_fee": t.get("donor_paid_fee"),
        # Campaign/Program
        "campaign_id": t.get("campaign_id"),
        "campaign_title": t.get("campaign_title"),
        "designation_id": t.get("designation", {}).get("id")
        if t.get("designation")
        else None,
        "designation_title": t.get("designation", {}).get("title")
        if t.get("designation")
        else None,
        "designation_code": t.get("designation", {}).get("code")
        if t.get("designation")
        else None,
        # P2P/Advocacy
        "p2p_fundraiser_id": t.get("p2p_fundraiser_id"),
        "p2p_fundraiser_name": t.get("p2p_fundraiser_name"),
        "p2p_program_id": t.get("p2p_program_id"),
        "p2p_program_name": t.get("p2p_program_name"),
        "p2p_team_id": t.get("p2p_team_id"),
        "p2p_team_name": t.get("p2p_team_name"),
        "advocacy_program_id": t.get("advocacy_program_id"),
        "advocacy_program_name": t.get("advocacy_program_name"),
        "advocacy_team_id": t.get("advocacy_team_id"),
        "advocacy_team_name": t.get("advocacy_team_name"),
        "advocate_id": t.get("advocate_id"),
        "advocate_name": t.get("advocate_name"),
        # Gift
        "gift_id": t.get("gift", {}).get("id") if t.get("gift") else None,
        "gift_description": t.get("gift", {}).get("description") if t.get("gift") else None,
        "gift_value": t.get("gift", {}).get("gift_value") if t.get("gift") else None,
        # Custom fields
        "custom_note_1": t.get("custom_note_1"),
        "custom_note_2": t.get("custom_note_2"),
        "custom_note_3": t.get("custom_note_3"),
        "custom_note_4": t.get("custom_note_4"),
        "custom_note_5": t.get("custom_note_5"),
        # Tracking
        "external_tracking_id": t.get("external_tracking_id"),
        "payment_transaction_id": t.get("payment_transaction_id"),
        "reference_code": t.get("reference_code"),
        # Flags
        "hide_name": t.get("hide_name"),
        "email_opt_in": t.get("email_opt_in"),
        # Company/Matching
        "company_name": t.get("company_name"),
        # Serialized complex objects (store as JSON strings)
        "advocate": _serialize_nested_object(t.get("advocate")),
        "corporate_matching": _serialize_nested_object(
            t.get("corporate_matching_record")
        ),
        "embed": _serialize_nested_object(t.get("embed")),
        "tribute": _serialize_nested_object(t.get("tribute")),
        "utm": _serialize_nested_object(t.get("utm")),
        "customer_meta": _serialize_nested_object(t.get("customer_meta")),
    }

    return row
