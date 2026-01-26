"""iDonate Fivetran Custom Connector.

Syncs transactions from iDonate API.
- Supports incremental sync via modifiedDate
- Paginated queries with date-based batching for large datasets
"""

from datetime import datetime

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from sync import sync_organization


def schema(_configuration: dict):
    """Define the schema for Fivetran tables."""
    return [
        {
            "table": "contacts",
            "primary_key": ["contact_id"],
            "columns": {
                # Core identifiers (synthetic stable key)
                "contact_id": "STRING",
                "email": "STRING",
                "email_normalized": "STRING",
                # Name fields
                "first_name": "STRING",
                "last_name": "STRING",
                "middle_name": "STRING",
                "title": "STRING",
                # Contact methods
                "phone": "STRING",
                "timezone": "STRING",
                # Address
                "address_street": "STRING",
                "address_street2": "STRING",
                "address_city": "STRING",
                "address_state": "STRING",
                "address_zip": "STRING",
                "address_country": "STRING",
                "address_country_code": "STRING",
                # Timestamps
                "created": "UTC_DATETIME",
                "updated": "UTC_DATETIME",
            },
        },
        {
            "table": "transactions",
            "primary_key": ["id"],
            "columns": {
                # Core identifiers
                "id": "STRING",
                "organization_id": "STRING",
                # Status & type
                "status": "STRING",
                "type": "STRING",
                "subtype": "STRING",
                "description": "STRING",
                "additional_info": "STRING",
                # Dates
                "created": "UTC_DATETIME",
                "final_date": "UTC_DATETIME",
                # Donor/Contact references
                "donor_id": "STRING",
                "contact_id": "STRING",
                "contact_email_raw": "STRING",
                # Payment info
                "card_type": "STRING",
                "last_four_digits": "STRING",
                "check_number": "STRING",
                # Amounts
                "sale_price": "FLOAT",
                "net_proceeds": "FLOAT",
                "client_proceeds": "FLOAT",
                "donor_paid_fee": "FLOAT",
                # Campaign & designation
                "campaign_id": "STRING",
                "campaign_title": "STRING",
                "designation_id": "STRING",
                "designation_title": "STRING",
                "designation_code": "STRING",
                # P2P/Advocacy
                "p2p_fundraiser_id": "STRING",
                "p2p_fundraiser_name": "STRING",
                "p2p_program_id": "STRING",
                "p2p_program_name": "STRING",
                "p2p_team_id": "STRING",
                "p2p_team_name": "STRING",
                "advocacy_program_id": "STRING",
                "advocacy_program_name": "STRING",
                "advocacy_team_id": "STRING",
                "advocacy_team_name": "STRING",
                "advocate_id": "STRING",
                "advocate_name": "STRING",
                # Gift
                "gift_id": "STRING",
                "gift_description": "STRING",
                "gift_value": "FLOAT",
                # Custom fields
                "custom_note_1": "STRING",
                "custom_note_2": "STRING",
                "custom_note_3": "STRING",
                "custom_note_4": "STRING",
                "custom_note_5": "STRING",
                # Tracking
                "external_tracking_id": "STRING",
                "payment_transaction_id": "STRING",
                "reference_code": "STRING",
                # Flags
                "hide_name": "BOOLEAN",
                "email_opt_in": "BOOLEAN",
                # Company
                "company_name": "STRING",
                # Serialized complex objects (stored as JSON strings)
                "advocate": "STRING",
                "corporate_matching": "STRING",
                "embed": "STRING",
                "tribute": "STRING",
                "utm": "STRING",
            },
        },
    ]


def update(configuration: dict, state: dict):
    """Main sync function called by Fivetran."""
    log.info("iDonate Connector: Starting sync")

    # Get organization ID from configuration
    organization_id = configuration.get("organization_id")
    if not organization_id:
        raise ValueError("organization_id is required in configuration")

    # Check for debug mode override
    debug_start = configuration.get("debug_start_date")
    debug_end = configuration.get("debug_end_date")
    is_debug_mode = debug_start is not None

    if is_debug_mode:
        log.info(
            f"Debug mode: syncing records from {debug_start} to {debug_end or 'now'}"
        )

    # Get last sync time from state
    last_sync_time = state.get("last_sync_time")
    if last_sync_time and not is_debug_mode:
        log.info(f"Performing incremental sync (since {last_sync_time})")
    elif not is_debug_mode:
        log.info("Performing initial sync")

    # Sync transactions and contacts
    log.info("Syncing transactions and contacts...")
    yield from sync_organization(
        configuration=configuration,
        organization_id=organization_id,
        last_sync_time=last_sync_time,
        state=state,
    )

    # Final checkpoint with new sync time
    if is_debug_mode and debug_end:
        next_sync_time = debug_end
        log.info(
            f"Debug mode: setting next sync time to debug_end_date: {next_sync_time}"
        )
    else:
        # Store in API format (YYYYMMDDTHHmmss) for next sync
        next_sync_time = datetime.utcnow().strftime("%Y%m%dT%H%M%S")

    final_state = {
        "last_sync_time": next_sync_time,
    }
    yield op.checkpoint(state=final_state)

    log.info(
        f"Sync completed. Next sync will fetch records modified since {next_sync_time}"
    )


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
