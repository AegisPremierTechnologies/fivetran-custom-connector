"""Virtuous Fivetran Custom Connector.

Syncs Gifts and Contacts from Virtuous CRM API.
- Supports incremental sync via modifiedDate
- Paginated queries (max 1000 per request)
"""

from datetime import datetime

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from sync import sync_gifts, sync_contacts


def schema(configuration: dict):
    """Define the schema for Fivetran tables."""
    return [
        {
            "table": "gifts",
            "primary_key": ["id"],
            "columns": {
                # Core identifiers
                "id": "STRING",
                "transaction_source": "STRING",
                "transaction_id": "STRING",
                "contact_id": "STRING",
                "contact_name": "STRING",
                # Gift details
                "gift_type": "STRING",
                "gift_date": "UTC_DATETIME",
                "amount": "FLOAT",
                "currency_code": "STRING",
                "exchange_rate": "FLOAT",
                "base_currency_code": "STRING",
                "batch": "STRING",
                # Timestamps & users
                "created_date": "UTC_DATETIME",
                "created_by_user": "STRING",
                "modified_date": "UTC_DATETIME",
                "modified_by_user": "STRING",
                # Segment
                "segment_id": "STRING",
                "segment": "STRING",
                "segment_code": "STRING",
                # Media outlet
                "media_outlet_id": "STRING",
                "media_outlet": "STRING",
                # Grant
                "grant_id": "STRING",
                "grant": "STRING",
                # Notes & tribute
                "notes": "STRING",
                "tribute": "STRING",
                "tribute_id": "STRING",
                "tribute_type": "STRING",
                # Acknowledgement
                "acknowledgee_individual_id": "STRING",
                "is_acknowledged_gift": "BOOLEAN",
                "acknowledgement_date": "UTC_DATETIME",
                "acknowledgement_notes": "STRING",
                # Receipt
                "receipt_date": "UTC_DATETIME",
                # Passthrough & related IDs
                "contact_passthrough_id": "STRING",
                "contact_individual_id": "STRING",
                "cash_accounting_code": "STRING",
                "gift_ask_id": "STRING",
                "contact_membership_id": "STRING",
                # Flags
                "is_private": "BOOLEAN",
                "is_tax_deductible": "BOOLEAN",
            },
        },
        {
            "table": "contacts",
            "primary_key": ["id"],
            "columns": {
                # Core identifiers
                "id": "STRING",
                "contact_type": "STRING",
                "is_private": "BOOLEAN",
                # Name fields
                "name": "STRING",
                "informal_name": "STRING",
                "formal_contact_name": "STRING",
                "alternate_contact_name": "STRING",
                "preferred_salutation_name": "STRING",
                "preferred_addressee_name": "STRING",
                "name_fields_locked": "BOOLEAN",
                # Profile
                "description": "STRING",
                "website": "STRING",
                "marital_status": "STRING",
                "anniversary_month": "INT",
                "anniversary_day": "INT",
                "anniversary_year": "INT",
                "merged_into_contact_id": "STRING",
                # Giving stats
                "gift_ask_amount": "STRING",
                "gift_ask_type": "STRING",
                "life_to_date_giving": "STRING",
                "year_to_date_giving": "STRING",
                "last_gift_amount": "STRING",
                "last_gift_date": "STRING",
                # Avatar & origin
                "primary_avatar_url": "STRING",
                "origin_segment_id": "STRING",
                "origin_segment": "STRING",
                # Timestamps & users
                "created_date": "UTC_DATETIME",
                "created_by_user": "STRING",
                "modified_date": "UTC_DATETIME",
                "modified_by_user": "STRING",
            },
        },
        {
            "table": "individuals",
            "primary_key": ["id"],
            "columns": {
                # Identifiers
                "id": "STRING",
                "contact_id": "STRING",  # FK to contacts
                # Name fields
                "prefix": "STRING",
                "prefix2": "STRING",
                "first_name": "STRING",
                "middle_name": "STRING",
                "last_name": "STRING",
                "pre_marriage_name": "STRING",
                "suffix": "STRING",
                "nickname": "STRING",
                # Demographics
                "gender": "STRING",
                "birth_month": "INT",
                "birth_day": "INT",
                "birth_year": "INT",
                "approximate_age": "INT",
                # Status flags
                "is_primary": "BOOLEAN",
                "can_be_primary": "BOOLEAN",
                "is_secondary": "BOOLEAN",
                "can_be_secondary": "BOOLEAN",
                "is_deceased": "BOOLEAN",
                "deceased_date": "STRING",
                # Other
                "passion": "STRING",
                "avatar_url": "STRING",
                # Timestamps & users
                "created_date": "UTC_DATETIME",
                "created_by_user": "STRING",
                "modified_date": "UTC_DATETIME",
                "modified_by_user": "STRING",
            },
        },
        {
            "table": "addresses",
            "primary_key": ["id"],
            "columns": {
                # Identifiers
                "id": "STRING",
                "contact_id": "STRING",  # FK to contacts
                # Address fields
                "label": "STRING",
                "address1": "STRING",
                "address2": "STRING",
                "city": "STRING",
                "state": "STRING",
                "postal": "STRING",
                "country": "STRING",
                # Status flags
                "is_primary": "BOOLEAN",
                "can_be_primary": "BOOLEAN",
                # Date range
                "start_month": "INT",
                "start_day": "INT",
                "end_month": "INT",
                "end_day": "INT",
                # Timestamps & users
                "created_date": "UTC_DATETIME",
                "created_by_user": "STRING",
                "modified_date": "UTC_DATETIME",
                "modified_by_user": "STRING",
            },
        },
        {
            "table": "contact_methods",
            "primary_key": ["id"],
            "columns": {
                # Identifiers
                "id": "STRING",
                "contact_id": "STRING",  # FK to contacts
                "individual_id": "STRING",  # FK to individuals
                # Method details
                "type": "STRING",
                "value": "STRING",
                # Status flags
                "is_opted_in": "BOOLEAN",
                "is_primary": "BOOLEAN",
                "can_be_primary": "BOOLEAN",
                # Timestamps & users
                "created_date": "UTC_DATETIME",
                "created_by_user": "STRING",
                "modified_date": "UTC_DATETIME",
                "modified_by_user": "STRING",
            },
        },
    ]


def update(configuration: dict, state: dict):
    """Main sync function called by Fivetran."""
    log.info("Virtuous Connector: Starting sync")

    # Check for debug mode override
    debug_start = configuration.get("debug_start_date")
    debug_end = configuration.get("debug_end_date")
    is_debug_mode = debug_start is not None

    if is_debug_mode:
        log.info(
            f"Debug mode: syncing records from {debug_start} to {debug_end or 'now'}"
        )
        modified_since = debug_start
    else:
        # Get last sync time from state
        modified_since = state.get("last_sync_time")
        if modified_since:
            log.info(f"Performing incremental sync (since {modified_since})")
        else:
            log.info("Performing initial sync (all records)")

    # Sync gifts
    log.info("Syncing gifts...")
    yield from sync_gifts(
        configuration, modified_since=modified_since, modified_until=debug_end
    )

    # Checkpoint after gifts
    yield op.checkpoint(
        state={"last_sync_time": modified_since, "gifts_complete": True}
    )

    # Sync contacts
    log.info("Syncing contacts...")
    yield from sync_contacts(
        configuration, modified_since=modified_since, modified_until=debug_end
    )

    # Final checkpoint with new sync time
    # If debug mode with end date specified, use that as the cursor so next run picks up from there
    if is_debug_mode and debug_end:
        next_sync_time = debug_end
        log.info(
            f"Debug mode: setting next sync time to debug_end_date: {next_sync_time}"
        )
    else:
        next_sync_time = datetime.utcnow().strftime("%Y-%m-%d")

    yield op.checkpoint(state={"last_sync_time": next_sync_time})

    log.info(
        f"Sync completed. Next sync will fetch records modified since {next_sync_time}"
    )


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
