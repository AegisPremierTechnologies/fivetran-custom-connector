"""OnGage Fivetran Custom Connector.

Syncs contacts from OnGage platform using the Contact Search API.
- Auto-discovers all lists
- Per-list checkpointing for resume on crash
- Adaptive batch retry for large lists
"""

from datetime import datetime

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api import get_all_lists
from models import format_list_data
from sync import sync_list


def schema(configuration: dict):
    """Define the schema for Fivetran tables."""
    return [
        {
            "table": "lists",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "description": "STRING",
                "type": "STRING",
                "scope": "STRING",
                "last_count": "INT",
                "last_active_count": "INT",
                "unsubscribes": "INT",
                "complaints": "INT",
                "bounces": "INT",
                "archive": "BOOLEAN",
                "default": "BOOLEAN",
                "account_id": "STRING",
                "created": "UTC_DATETIME",
                "modified": "UTC_DATETIME",
            },
        },
        {
            "table": "contacts",
            "primary_key": ["id", "list_id"],
            "columns": {
                # List identifier
                "list_id": "STRING",
                # Core identifiers
                "id": "STRING",
                "email": "STRING",
                # Personal info
                "first_name": "STRING",
                "last_name": "STRING",
                "gender": "STRING",
                "age_range": "STRING",
                "birth_year": "STRING",
                # Location
                "address": "STRING",
                "city": "STRING",
                "state": "STRING",
                "zip_code": "STRING",
                "country": "STRING",
                # Contact info
                "phone": "STRING",
                "language": "STRING",
                # Technical
                "ip": "STRING",
                "os": "STRING",
                "product_id": "STRING",
                # OnGage system fields
                "status": "STRING",
                "created_date": "UTC_DATETIME",
                "unsubscribe_date": "UTC_DATETIME",
                "resubscribe_date": "UTC_DATETIME",
                "bounce_date": "UTC_DATETIME",
                "complaint_date": "UTC_DATETIME",
                "import_id": "STRING",
            },
        },
    ]


def update(configuration: dict, state: dict):
    """Main sync function called by Fivetran."""
    log.info("OnGage Connector: Starting sync")

    # Get all available lists
    lists = get_all_lists(configuration)
    _list_ids = sorted([int(lst.get("id")) for lst in lists if lst.get("id")])
    list_ids = [str(lst_id) for lst_id in _list_ids]

    log.info(f"Lists to sync: {list_ids}")

    # Get cursors from state
    completed_lists = state.get("completed_lists", [])
    last_sync_time = state.get("last_sync_time")
    is_initial = last_sync_time is None

    if is_initial:
        log.info("Performing initial sync (all contacts)")
    else:
        log.info(f"Performing incremental sync (since {last_sync_time})")

    # Upsert all lists first
    for lst in lists:
        yield op.upsert(table="lists", data=format_list_data(lst))

    # Process each list
    for list_id in list_ids:
        # Skip already completed lists (for resume after crash)
        if list_id in completed_lists:
            log.info(f"Skipping already completed list {list_id}")
            continueZ

        # Find the list info to get count
        list_info = next((lst for lst in lists if str(lst.get("id")) == list_id), {})
        list_count = list_info.get("last_count", 0) or 0

        # Sync this list
        yield from sync_list(configuration, list_id, last_sync_time, list_count)

        # Checkpoint after each list
        completed_lists.append(list_id)
        yield op.checkpoint(
            state={
                "completed_lists": completed_lists,
                "last_sync_time": last_sync_time,
            }
        )

    # Final checkpoint: reset for next sync
    current_sync_time = int(datetime.utcnow().timestamp())
    yield op.checkpoint(
        state={
            "completed_lists": [],
            "last_sync_time": current_sync_time,
        }
    )

    log.info(f"Sync completed. Processed {len(list_ids)} lists.")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
