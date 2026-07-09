"""Data transformations for Springboard MongoDB documents.

Pure input/output: converts BSON documents to Fivetran-compatible row dicts.
No side effects, no database access, no Fivetran operations.
"""

import json
from datetime import datetime, timezone
from typing import Any

from bson import ObjectId


def parse_php_datetime(obj) -> str | None:
    if obj is None:
        return None
    if isinstance(obj, dict):
        date_str = obj.get("date")
        if date_str is None:
            return None
        tz_str = obj.get("timezone", "+00:00")
        if tz_str == "UTC":
            tz_str = "+00:00"
        return date_str.replace(" ", "T", 1) + tz_str
    if isinstance(obj, str):
        if obj.isdigit():
            dt = datetime.fromtimestamp(int(obj), tz=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
        return obj
    return None


def _json_default(o: Any) -> Any:
    if isinstance(o, ObjectId):
        return str(o)
    if isinstance(o, datetime):
        return o.isoformat()
    if isinstance(o, bytes):
        return o.hex()
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def safe_json(obj) -> str | None:
    if obj is None or obj == "":
        return None
    return json.dumps(obj, default=_json_default)


def extract_key_value(obj) -> str | None:
    if obj is None or obj == "":
        return None
    if isinstance(obj, str):
        return obj
    if isinstance(obj, list) and len(obj) > 0:
        first = obj[0]
        if isinstance(first, dict):
            return first.get("key")
    return None


def _safe_str(val) -> str | None:
    if val is None:
        return None
    return str(val)


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_bool(val) -> bool | None:
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() == "true"
    return bool(val)


def format_cart_abandonment(doc: dict) -> dict:
    return {
        "_id": str(doc.get("_id")),
        "caid": doc.get("caid"),
        "abandonment_type": doc.get("abandonment_type"),
        "address": doc.get("address"),
        "address_line_2": doc.get("address_line_2"),
        "cid": doc.get("cid"),
        "city": doc.get("city"),
        "country": doc.get("country"),
        "created_at": parse_php_datetime(doc.get("created_at")),
        "dw_created_at": parse_php_datetime(doc.get("dw_created_at")),
        "dw_updated_at": parse_php_datetime(doc.get("dw_updated_at")),
        "email": doc.get("email"),
        "external_form_name": doc.get("external_form_name"),
        "first_name": doc.get("first_name"),
        "form_id": doc.get("form_id"),
        "internal_form_name": doc.get("internal_form_name"),
        "ip_address": doc.get("ip_address"),
        "last_name": doc.get("last_name"),
        "market_source": doc.get("market_source"),
        "postal_code": doc.get("postal_code"),
        "state": doc.get("state"),
        "unique_id": doc.get("unique_id"),
        "utm_campaign": doc.get("utm_campaign"),
        "utm_content": doc.get("utm_content"),
        "utm_medium": doc.get("utm_medium"),
        "utm_source": doc.get("utm_source"),
        "utm_term": doc.get("utm_term"),
        "warehouse_type": doc.get("warehouse_type"),
    }


def format_contact(doc: dict) -> dict:
    return {
        "_id": str(doc.get("_id")),
        "contact_id": doc.get("contact_id"),
        "address": doc.get("address"),
        "address_line_2": doc.get("address_line_2"),
        "cid": doc.get("cid"),
        "city": doc.get("city"),
        "country": doc.get("country"),
        "created_at": parse_php_datetime(doc.get("created_at")),
        "dw_created_at": parse_php_datetime(doc.get("dw_created_at")),
        "dw_updated_at": parse_php_datetime(doc.get("dw_updated_at")),
        "email": doc.get("email"),
        "first_name": doc.get("first_name"),
        "last_name": doc.get("last_name"),
        "last_access": parse_php_datetime(doc.get("last_access")),
        "last_login": parse_php_datetime(doc.get("last_login")),
        "last_updated_source_form_id": doc.get("last_updated_source_form_id"),
        "last_updated_source_form_name": doc.get("last_updated_source_form_name"),
        "ms": doc.get("ms"),
        "originating_content_type": doc.get("originating_content_type"),
        "p2p_campaigner": _safe_bool(doc.get("p2p_campaigner")),
        "sbp_device_browser": doc.get("sbp_device_browser"),
        "sbp_device_os": doc.get("sbp_device_os"),
        "sbp_device_type": doc.get("sbp_device_type"),
        "sbp_membership_id": doc.get("sbp_membership_id"),
        "sbp_phone": doc.get("sbp_phone"),
        "sbp_renewal_date": doc.get("sbp_renewal_date"),
        "state": doc.get("state"),
        "timezone": doc.get("timezone"),
        "unique_id": doc.get("unique_id"),
        "updated_at": parse_php_datetime(doc.get("updated_at")),
        "user_agent": doc.get("user_agent"),
        "username": doc.get("username"),
        "warehouse_type": doc.get("warehouse_type"),
        "zip": doc.get("zip"),
    }


def format_donation(doc: dict) -> dict:
    return {
        "_id": str(doc.get("_id")),
        "donation_id": doc.get("donation_id"),
        "account_number_last_4": doc.get("account_number_last_4"),
        "account_type": doc.get("account_type"),
        "address": doc.get("address"),
        "address_line_2": doc.get("address_line_2"),
        "address_preferred_mailing": doc.get("address_preferred_mailing"),
        "address_preferred_other": doc.get("address_preferred_other"),
        "amount": _safe_float(doc.get("amount")),
        "autofilled": _safe_bool(doc.get("autofilled")),
        "bank_account_mask": doc.get("bank_account_mask"),
        "bank_name": doc.get("bank_name"),
        "cancellation_reason": doc.get("cancellation_reason"),
        "card_expiration_month": doc.get("card_expiration_month"),
        "card_expiration_year": doc.get("card_expiration_year"),
        "card_last_4": doc.get("card_last_4"),
        "card_type": doc.get("card_type"),
        "cid": doc.get("cid"),
        "city": doc.get("city"),
        "contact_id": doc.get("contact_id"),
        "content_override_id": doc.get("content_override_id"),
        "country": doc.get("country"),
        "created_at": parse_php_datetime(doc.get("created_at")),
        "currency": doc.get("currency"),
        "custom_gift_string_rendered": _safe_bool(doc.get("custom_gift_string_rendered")),
        "device_browser": doc.get("device_browser"),
        "device_name": doc.get("device_name"),
        "device_os": doc.get("device_os"),
        "device_type": doc.get("device_type"),
        "dw_created_at": parse_php_datetime(doc.get("dw_created_at")),
        "dw_updated_at": parse_php_datetime(doc.get("dw_updated_at")),
        "email": doc.get("email"),
        "eml_id": doc.get("eml_id"),
        "eml_name": doc.get("eml_name"),
        "field_form": doc.get("field_form"),
        "field_form_url": doc.get("field_form_url"),
        "field_sbp_initial_referrer_long": doc.get("field_sbp_initial_referrer_long"),
        "field_sbp_referrer_long": doc.get("field_sbp_referrer_long"),
        "first_name": doc.get("first_name"),
        "form_id": doc.get("form_id"),
        "form_internal_name": doc.get("form_internal_name"),
        "form_url": doc.get("form_url"),
        "gateway": doc.get("gateway"),
        "gateway_machine_name": doc.get("gateway_machine_name"),
        "gateway_response": doc.get("gateway_response"),
        "giving_record_type": doc.get("giving_record_type"),
        "groups": doc.get("groups"),
        "gs_flag": doc.get("gs_flag"),
        "initial_referrer": doc.get("initial_referrer"),
        "initms": doc.get("initms"),
        "ip_address": doc.get("ip_address"),
        "last_name": doc.get("last_name"),
        "ms": doc.get("ms"),
        "ocd_opt_in": doc.get("ocd_opt_in"),
        "ocd_opt_out": doc.get("ocd_opt_out"),
        "offline_donation": _safe_bool(doc.get("offline_donation")),
        "offline_donation_entered_by": doc.get("offline_donation_entered_by"),
        "one_click_donate_optin": _safe_bool(doc.get("one_click_donate_optin")),
        "one_click_donation": _safe_bool(doc.get("one_click_donation")),
        "onetime_gift_amounts": safe_json(doc.get("onetime_gift_amounts")),
        "organization_issues": doc.get("organization_issues"),
        "origin_form_name": doc.get("origin_form_name"),
        "origin_nid": doc.get("origin_nid"),
        "p2p_cid": doc.get("p2p_cid"),
        "p2p_pcid": doc.get("p2p_pcid"),
        "parent_id": doc.get("parent_id"),
        "payment_method": doc.get("payment_method"),
        "payment_token": doc.get("payment_token"),
        "payment_token_created_at": parse_php_datetime(doc.get("payment_token_created_at")),
        "phone": doc.get("phone"),
        "premium_sku": doc.get("premium_sku"),
        "processing_fee_amount": doc.get("processing_fee_amount"),
        "quantity": doc.get("quantity"),
        "recurring": _safe_bool(doc.get("recurring")),
        "recurring_amount": doc.get("recurring_amount"),
        "recurring_frequency": doc.get("recurring_frequency"),
        "recurring_gift_amounts": safe_json(doc.get("recurring_gift_amounts")),
        "recurring_other_amount": doc.get("recurring_other_amount"),
        "recurs_monthly": _safe_bool(doc.get("recurs_monthly")),
        "referrer": doc.get("referrer"),
        "routing_number": doc.get("routing_number"),
        "sbp_zip_plus_four": doc.get("sbp_zip_plus_four"),
        "search_engine": doc.get("search_engine"),
        "search_string": doc.get("search_string"),
        "secure_prepop_autofilled": doc.get("secure_prepop_autofilled"),
        "social_referer_transaction": doc.get("social_referer_transaction"),
        "springboard_cookie_autofilled": doc.get("springboard_cookie_autofilled"),
        "state": doc.get("state"),
        "status": doc.get("status"),
        "submission_id": doc.get("submission_id"),
        "transaction_date": parse_php_datetime(doc.get("transaction_date")),
        "transaction_id": doc.get("transaction_id"),
        "type": doc.get("type"),
        "unique_id": doc.get("unique_id"),
        "updated_at": parse_php_datetime(doc.get("updated_at")),
        "upsell_appeal_accepted": _safe_bool(doc.get("upsell_appeal_accepted")),
        "upsell_donation": _safe_bool(doc.get("upsell_donation")),
        "upsell_originating_donation": doc.get("upsell_originating_donation"),
        "user_agent": doc.get("user_agent"),
        "utm_campaign": doc.get("utm_campaign"),
        "utm_content": doc.get("utm_content"),
        "utm_medium": doc.get("utm_medium"),
        "utm_source": doc.get("utm_source"),
        "utm_term": doc.get("utm_term"),
        "warehouse_type": doc.get("warehouse_type"),
        "your_recognition_name": doc.get("your_recognition_name"),
        "zip": doc.get("zip"),
    }


def format_event_log(doc: dict) -> dict:
    return {
        "_id": str(doc.get("_id")),
        "event_id": doc.get("event_id"),
        "event_type": doc.get("event_type"),
        "event_data": safe_json(doc.get("event_data")),
        "event_date": parse_php_datetime(doc.get("event_date")),
        "unique_id": doc.get("unique_id"),
        "created_at": parse_php_datetime(doc.get("created_at")),
        "updated_at": parse_php_datetime(doc.get("updated_at")),
        "dw_created_at": parse_php_datetime(doc.get("dw_created_at")),
        "dw_updated_at": parse_php_datetime(doc.get("dw_updated_at")),
    }


def format_form(doc: dict) -> dict:
    return {
        "_id": str(doc.get("_id")),
        "form_id": doc.get("form_id"),
        "form_type": doc.get("form_type"),
        "form_url": doc.get("form_url"),
        "body": safe_json(doc.get("body")),
        "created_at": parse_php_datetime(doc.get("created_at")),
        "dw_created_at": parse_php_datetime(doc.get("dw_created_at")),
        "dw_updated_at": parse_php_datetime(doc.get("dw_updated_at")),
        "expire": doc.get("expire"),
        "field_fundraiser_currency": doc.get("field_fundraiser_currency"),
        "field_image": safe_json(doc.get("field_image")),
        "field_image_style": doc.get("field_image_style"),
        "field_no_gift_message": safe_json(doc.get("field_no_gift_message")),
        "fr_drives_drive": safe_json(doc.get("fr_drives_drive")),
        "fr_premiums_bonusgift": safe_json(doc.get("fr_premiums_bonusgift")),
        "fr_premiums_premium": safe_json(doc.get("fr_premiums_premium")),
        "group_id": doc.get("group_id"),
        "groups": doc.get("groups"),
        "internal_name": doc.get("internal_name"),
        "language": doc.get("language"),
        "legislative_issues": doc.get("legislative_issues"),
        "name": doc.get("name"),
        "organization_issues": doc.get("organization_issues"),
        "status": doc.get("status"),
        "unique_id": doc.get("unique_id"),
        "updated_at": parse_php_datetime(doc.get("updated_at")),
        "url": doc.get("url"),
        "warehouse_type": doc.get("warehouse_type"),
    }


def format_line_item(doc: dict) -> dict:
    return {
        "_id": str(doc.get("_id")),
        "line_item_id": doc.get("line_item_id"),
        "donation_id": doc.get("donation_id"),
        "created_at": parse_php_datetime(doc.get("created_at")),
        "dw_created_at": parse_php_datetime(doc.get("dw_created_at")),
        "dw_updated_at": parse_php_datetime(doc.get("dw_updated_at")),
        "groups": doc.get("groups"),
        "label": doc.get("label"),
        "price": _safe_float(doc.get("price")),
        "price_formatted": doc.get("price_formatted"),
        "product_id": doc.get("product_id"),
        "quantity": doc.get("quantity"),
        "sku": doc.get("sku"),
        "sustainer_event_id": doc.get("sustainer_event_id"),
        "sustainer_upgrade_event_id": doc.get("sustainer_upgrade_event_id"),
        "total": _safe_float(doc.get("total")),
        "total_formatted": doc.get("total_formatted"),
        "type": doc.get("type"),
        "unique_id": doc.get("unique_id"),
        "updated_at": parse_php_datetime(doc.get("updated_at")),
        "warehouse_type": doc.get("warehouse_type"),
    }


def format_payment(doc: dict) -> dict:
    return {
        "_id": str(doc.get("_id")),
        "donation_id": doc.get("donation_id"),
        "payment_method": doc.get("payment_method"),
        "gateway_id": doc.get("gateway_id"),
        "gateway_machine_name": doc.get("gateway_machine_name"),
        "transaction_id": doc.get("transaction_id"),
        "amount": _safe_float(doc.get("amount")),
        "currency": doc.get("currency"),
        "status": doc.get("status"),
        "gateway_status": doc.get("gateway_status"),
        "unique_id": doc.get("unique_id"),
        "groups": doc.get("groups"),
        "warehouse_type": doc.get("warehouse_type"),
        "created_at": parse_php_datetime(doc.get("created_at")),
        "updated_at": parse_php_datetime(doc.get("updated_at")),
        "dw_created_at": parse_php_datetime(doc.get("dw_created_at")),
        "dw_updated_at": parse_php_datetime(doc.get("dw_updated_at")),
    }


def format_submission(doc: dict) -> dict:
    return {
        "_id": str(doc.get("_id")),
        "submission_id": doc.get("submission_id"),
        "address": doc.get("address"),
        "address_line_2": doc.get("address_line_2"),
        "address_preferred_mailing": doc.get("address_preferred_mailing"),
        "address_preferred_other": doc.get("address_preferred_other"),
        "amount": _safe_float(doc.get("amount")),
        "autofilled": _safe_bool(doc.get("autofilled")),
        "bonusgift": doc.get("bonusgift"),
        "cid": doc.get("cid"),
        "city": doc.get("city"),
        "contact_id": doc.get("contact_id"),
        "content_override_id": doc.get("content_override_id"),
        "country": extract_key_value(doc.get("country")),
        "created_at": parse_php_datetime(doc.get("created_at")),
        "currency": doc.get("currency"),
        "device_browser": doc.get("device_browser"),
        "device_name": doc.get("device_name"),
        "device_os": doc.get("device_os"),
        "device_type": doc.get("device_type"),
        "dw_created_at": parse_php_datetime(doc.get("dw_created_at")),
        "dw_updated_at": parse_php_datetime(doc.get("dw_updated_at")),
        "eml_id": doc.get("eml_id"),
        "eml_name": doc.get("eml_name"),
        "field_form": doc.get("field_form"),
        "field_form_url": doc.get("field_form_url"),
        "field_sbp_initial_referrer_long": doc.get("field_sbp_initial_referrer_long"),
        "field_sbp_referrer_long": doc.get("field_sbp_referrer_long"),
        "first_name": doc.get("first_name"),
        "form_id": doc.get("form_id"),
        "groups": doc.get("groups"),
        "gs_flag": doc.get("gs_flag"),
        "initial_referrer": doc.get("initial_referrer"),
        "initms": doc.get("initms"),
        "ip_address": doc.get("ip_address"),
        "last_name": doc.get("last_name"),
        "mail": doc.get("mail"),
        "ms": doc.get("ms"),
        "ocd_opt_in": doc.get("ocd_opt_in"),
        "ocd_opt_out": doc.get("ocd_opt_out"),
        "organization_issues": doc.get("organization_issues"),
        "origin_form_name": doc.get("origin_form_name"),
        "origin_nid": doc.get("origin_nid"),
        "other_amount": doc.get("other_amount"),
        "p2p_pcid": doc.get("p2p_pcid"),
        "payment_fields": doc.get("payment_fields"),
        "payment_method": doc.get("payment_method"),
        "phone": doc.get("phone"),
        "premiums_box": doc.get("premiums_box"),
        "processing_fee_amount": doc.get("processing_fee_amount"),
        "recurring_amount": extract_key_value(doc.get("recurring_amount")),
        "recurring_other_amount": doc.get("recurring_other_amount"),
        "recurs_monthly": extract_key_value(doc.get("recurs_monthly")),
        "referrer": doc.get("referrer"),
        "sbp_zip_plus_four": doc.get("sbp_zip_plus_four"),
        "search_engine": doc.get("search_engine"),
        "search_string": doc.get("search_string"),
        "secure_prepop_autofilled": doc.get("secure_prepop_autofilled"),
        "social_referer_transaction": doc.get("social_referer_transaction"),
        "state": extract_key_value(doc.get("state")),
        "unique_id": doc.get("unique_id"),
        "updated_at": parse_php_datetime(doc.get("updated_at")),
        "user_agent": doc.get("user_agent"),
        "utm_campaign": doc.get("utm_campaign"),
        "utm_content": doc.get("utm_content"),
        "utm_medium": doc.get("utm_medium"),
        "utm_source": doc.get("utm_source"),
        "utm_term": doc.get("utm_term"),
        "warehouse_type": doc.get("warehouse_type"),
        "zip": doc.get("zip"),
    }


def format_sync_failure_counts(doc: dict) -> dict:
    return {
        "_id": str(doc.get("_id")),
        "item_id": doc.get("item_id"),
        "count": doc.get("count"),
    }


def format_vendor_mapping(doc: dict) -> dict:
    return {
        "_id": str(doc.get("_id")),
        "external_id": doc.get("external_id"),
        "external_system": doc.get("external_system"),
        "external_type": doc.get("external_type"),
        "dw_id": doc.get("dw_id"),
        "dw_type": doc.get("dw_type"),
        "groups": doc.get("groups"),
        "unique_id": doc.get("unique_id"),
        "warehouse_type": doc.get("warehouse_type"),
        "created_at": parse_php_datetime(doc.get("created_at")),
        "updated_at": parse_php_datetime(doc.get("updated_at")),
        "dw_created_at": parse_php_datetime(doc.get("dw_created_at")),
        "dw_updated_at": parse_php_datetime(doc.get("dw_updated_at")),
    }


def format_generic(doc: dict) -> dict:
    return {
        "_id": str(doc.get("_id")),
        "dw_created_at": parse_php_datetime(doc.get("dw_created_at")),
        "dw_updated_at": parse_php_datetime(doc.get("dw_updated_at")),
        "unique_id": doc.get("unique_id"),
        "warehouse_type": doc.get("warehouse_type"),
    }


_COLLECTION_FORMATTERS = {
    "sb_cart_abandonment": format_cart_abandonment,
    "sb_contact": format_contact,
    "sb_donation": format_donation,
    "sb_event_log": format_event_log,
    "sb_form": format_form,
    "sb_line_item": format_line_item,
    "sb_payment": format_payment,
    "sb_submission": format_submission,
    "sb_sync_failure_counts": format_sync_failure_counts,
    "sb_vendor_mapping": format_vendor_mapping,
    "sb_action": format_generic,
    "sb_message": format_generic,
}


def format_document(collection_name: str, doc: dict) -> dict:
    formatter = _COLLECTION_FORMATTERS.get(collection_name, format_generic)
    return formatter(doc)
