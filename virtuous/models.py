"""Data models and transformations for Virtuous connector."""

import json
from datetime import datetime
from typing import Optional

from dateutil import parser as date_parser
from fivetran_connector_sdk import Logging as log


def parse_datetime(value) -> Optional[str]:
    """Parse datetime from various formats to ISO 8601 with Z suffix."""
    if value is None or value == "" or value == "null":
        return None
    try:
        if isinstance(value, str):
            dt = date_parser.parse(value)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        return None
    except (ValueError, TypeError):
        return None


def safe_float(value) -> Optional[float]:
    """Safely convert a value to float, handling currency formatting."""
    if value is None or value == "" or value == "null":
        return None
    try:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            # Strip currency symbols and commas
            cleaned = value.replace("$", "").replace(",", "").replace(" ", "").strip()
            if cleaned == "":
                return None
            return float(cleaned)
        return None
    except (ValueError, TypeError):
        return None


def format_gift(raw: dict, debug: bool = False) -> dict:
    """Transform API gift response to Fivetran schema row.

    Maps all scalar fields from API response directly to snake_case columns.
    Excludes array fields: giftDesignations, giftPremiums, pledgePayments,
    recurringGiftPayments, customFields.
    """
    if debug:
        log.info(f"RAW GIFT: {json.dumps(raw, default=str)}")

    def safe_str(val):
        """Convert value to string, return None for null values."""
        return str(val) if val is not None else None

    result = {
        # Core identifiers
        "id": safe_str(raw.get("id")),
        "transaction_source": raw.get("transactionSource"),
        "transaction_id": raw.get("transactionId"),
        "contact_id": safe_str(raw.get("contactId")),
        "contact_name": raw.get("contactName"),
        # Gift details
        "gift_type": raw.get("giftType"),
        "gift_date": parse_datetime(raw.get("giftDate")),
        "amount": safe_float(raw.get("amount")),
        "currency_code": raw.get("currencyCode"),
        "exchange_rate": safe_float(raw.get("exchangeRate")),
        "base_currency_code": raw.get("baseCurrencyCode"),
        "batch": raw.get("batch"),
        # Timestamps & users
        "created_date": parse_datetime(raw.get("createDateTimeUtc")),
        "created_by_user": raw.get("createdByUser"),
        "modified_date": parse_datetime(raw.get("modifiedDateTimeUtc")),
        "modified_by_user": raw.get("modifiedByUser"),
        # Segment
        "segment_id": safe_str(raw.get("segmentId")),
        "segment": raw.get("segment"),
        "segment_code": raw.get("segmentCode"),
        # Media outlet
        "media_outlet_id": safe_str(raw.get("mediaOutletId")),
        "media_outlet": raw.get("mediaOutlet"),
        # Grant
        "grant_id": safe_str(raw.get("grantId")),
        "grant": raw.get("grant"),
        # Notes & tribute
        "notes": raw.get("notes"),
        "tribute": raw.get("tribute"),
        "tribute_id": safe_str(raw.get("tributeId")),
        "tribute_type": raw.get("tributeType"),
        # Acknowledgement
        "acknowledgee_individual_id": safe_str(raw.get("acknowledgeeIndividualId")),
        "is_acknowledged_gift": raw.get("isAcknowledgedGift"),
        "acknowledgement_date": parse_datetime(raw.get("acknowledgementDate")),
        "acknowledgement_notes": raw.get("acknowledgementNotes"),
        # Receipt
        "receipt_date": parse_datetime(raw.get("receiptDate")),
        # Passthrough & related IDs
        "contact_passthrough_id": safe_str(raw.get("contactPassthroughId")),
        "contact_individual_id": safe_str(raw.get("contactIndividualId")),
        "cash_accounting_code": raw.get("cashAccountingCode"),
        "gift_ask_id": safe_str(raw.get("giftAskId")),
        "contact_membership_id": safe_str(raw.get("contactMembershipId")),
        # Flags
        "is_private": raw.get("isPrivate"),
        "is_tax_deductible": raw.get("isTaxDeductible"),
    }

    if debug:
        log.info(
            f"FORMATTED GIFT: id={result['id']}, is_tax_deductible={result['is_tax_deductible']}, segment_id={result['segment_id']}"
        )

    return result


def safe_str(val):
    """Convert value to string, return None for null values."""
    return str(val) if val is not None else None


def safe_int(val):
    """Safely convert value to int, return None for null values."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def format_contact(raw: dict, debug: bool = False) -> dict:
    """Transform API contact response to Fivetran schema row.

    Maps scalar fields only. Arrays (contactIndividuals, address, etc.)
    are handled by separate formatters.
    """
    if debug:
        log.info(f"RAW CONTACT: {json.dumps(raw, default=str)}")

    return {
        # Core identifiers
        "id": safe_str(raw.get("id")),
        "contact_type": raw.get("contactType"),
        "is_private": raw.get("isPrivate"),
        # Name fields
        "name": raw.get("name"),
        "informal_name": raw.get("informalName"),
        "formal_contact_name": raw.get("formalContactName"),
        "alternate_contact_name": raw.get("alternateContactName"),
        "preferred_salutation_name": raw.get("preferredSalutationName"),
        "preferred_addressee_name": raw.get("preferredAddresseeName"),
        "name_fields_locked": raw.get("nameFieldsLocked"),
        # Profile
        "description": raw.get("description"),
        "website": raw.get("website"),
        "marital_status": raw.get("maritalStatus"),
        "anniversary_month": safe_int(raw.get("anniversaryMonth")),
        "anniversary_day": safe_int(raw.get("anniversaryDay")),
        "anniversary_year": safe_int(raw.get("anniversaryYear")),
        "merged_into_contact_id": safe_str(raw.get("mergedIntoContactId")),
        # Giving stats (kept as strings since they have currency formatting)
        "gift_ask_amount": raw.get("giftAskAmount"),
        "gift_ask_type": raw.get("giftAskType"),
        "life_to_date_giving": raw.get("lifeToDateGiving"),
        "year_to_date_giving": raw.get("yearToDateGiving"),
        "last_gift_amount": raw.get("lastGiftAmount"),
        "last_gift_date": raw.get("lastGiftDate"),
        # Avatar & origin
        "primary_avatar_url": raw.get("primaryAvatarUrl"),
        "origin_segment_id": safe_str(raw.get("originSegmentId")),
        "origin_segment": raw.get("originSegment"),
        # Timestamps & users
        "created_date": parse_datetime(raw.get("createDateTimeUtc")),
        "created_by_user": raw.get("createdByUser"),
        "modified_date": parse_datetime(raw.get("modifiedDateTimeUtc")),
        "modified_by_user": raw.get("modifiedByUser"),
    }


def format_individual(raw: dict, contact_id: str) -> dict:
    """Transform API individual response to Fivetran schema row.

    Args:
        raw: Individual object from contactIndividuals array
        contact_id: Parent contact ID for foreign key
    """
    return {
        # Identifiers
        "id": safe_str(raw.get("id")),
        "contact_id": contact_id,
        # Name fields
        "prefix": raw.get("prefix"),
        "prefix2": raw.get("prefix2"),
        "first_name": raw.get("firstName"),
        "middle_name": raw.get("middleName"),
        "last_name": raw.get("lastName"),
        "pre_marriage_name": raw.get("preMarriageName"),
        "suffix": raw.get("suffix"),
        "nickname": raw.get("nickname"),
        # Demographics
        "gender": raw.get("gender"),
        "birth_month": safe_int(raw.get("birthMonth")),
        "birth_day": safe_int(raw.get("birthDay")),
        "birth_year": safe_int(raw.get("birthYear")),
        "approximate_age": safe_int(raw.get("approximateAge")),
        # Status flags
        "is_primary": raw.get("isPrimary"),
        "can_be_primary": raw.get("canBePrimary"),
        "is_secondary": raw.get("isSecondary"),
        "can_be_secondary": raw.get("canBeSecondary"),
        "is_deceased": raw.get("isDeceased"),
        "deceased_date": raw.get("deceasedDate"),
        # Other
        "passion": raw.get("passion"),
        "avatar_url": raw.get("avatarUrl"),
        # Timestamps & users
        "created_date": parse_datetime(raw.get("createDateTimeUtc")),
        "created_by_user": raw.get("createdByUser"),
        "modified_date": parse_datetime(raw.get("modifiedDateTimeUtc")),
        "modified_by_user": raw.get("modifiedByUser"),
    }


def format_address(raw: dict, contact_id: str) -> dict:
    """Transform API address response to Fivetran schema row.

    Args:
        raw: Address object (either contact.address or from contactAddresses array)
        contact_id: Parent contact ID for foreign key
    """
    return {
        # Identifiers
        "id": safe_str(raw.get("id")),
        "contact_id": contact_id,
        # Address fields
        "label": raw.get("label"),
        "address1": raw.get("address1"),
        "address2": raw.get("address2"),
        "city": raw.get("city"),
        "state": raw.get("state"),
        "postal": raw.get("postal"),
        "country": raw.get("country"),
        # Status flags
        "is_primary": raw.get("isPrimary"),
        "can_be_primary": raw.get("canBePrimary"),
        # Date range
        "start_month": safe_int(raw.get("startMonth")),
        "start_day": safe_int(raw.get("startDay")),
        "end_month": safe_int(raw.get("endMonth")),
        "end_day": safe_int(raw.get("endDay")),
        # Timestamps & users
        "created_date": parse_datetime(raw.get("createDateTimeUtc")),
        "created_by_user": raw.get("createdByUser"),
        "modified_date": parse_datetime(raw.get("modifiedDateTimeUtc")),
        "modified_by_user": raw.get("modifiedByUser"),
    }


def format_contact_method(raw: dict, contact_id: str, individual_id: str) -> dict:
    """Transform API contact method response to Fivetran schema row.

    Args:
        raw: Contact method object from individual.contactMethods array
        contact_id: Parent contact ID for foreign key
        individual_id: Parent individual ID for foreign key
    """
    return {
        # Identifiers
        "id": safe_str(raw.get("id")),
        "contact_id": contact_id,
        "individual_id": individual_id,
        # Method details
        "type": raw.get("type"),
        "value": raw.get("value"),
        # Status flags
        "is_opted_in": raw.get("isOptedIn"),
        "is_primary": raw.get("isPrimary"),
        "can_be_primary": raw.get("canBePrimary"),
        # Timestamps & users
        "created_date": parse_datetime(raw.get("createDateTimeUtc")),
        "created_by_user": raw.get("createdByUser"),
        "modified_date": parse_datetime(raw.get("modifiedDateTimeUtc")),
        "modified_by_user": raw.get("modifiedByUser"),
    }
