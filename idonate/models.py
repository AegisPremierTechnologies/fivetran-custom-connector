"""Data transformation for iDonate connector.

Pure input/output transformations:
- Date parsing
- Type casting
- Flattening nested structures to JSON strings where needed
"""

import json
from typing import Any, Dict, Optional


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


def format_transaction(raw_transaction: dict) -> dict:
    """Transform raw transaction from API to Fivetran-compliant row.

    Flattens some nested structures and serializes complex ones.
    """
    t = raw_transaction

    # Build the base transaction row
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
        # Donor/Contact info
        "donor_id": t.get("donor_id"),
        "contact_email": t.get("contact", {}).get("email") if t.get("contact") else None,
        "contact_first_name": t.get("contact", {}).get("firstname")
        if t.get("contact")
        else None,
        "contact_last_name": t.get("contact", {}).get("lastname")
        if t.get("contact")
        else None,
        "contact_phone": t.get("contact", {}).get("phone") if t.get("contact") else None,
        # Flatten primary address
        **_flatten_address(t.get("address")),
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
        "designation_id": t.get("designation", {}).get("id") if t.get("designation") else None,
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
        "contact_data": _serialize_nested_object(t.get("contact")),
        "corporate_matching": _serialize_nested_object(t.get("corporate_matching_record")),
        "embed": _serialize_nested_object(t.get("embed")),
        "tribute": _serialize_nested_object(t.get("tribute")),
        "utm": _serialize_nested_object(t.get("utm")),
    }

    return row
