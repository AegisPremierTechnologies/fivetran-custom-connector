# OnGage Fivetran Custom Connector
# Syncs contacts from OnGage platform using the Contact Search API.
# Supports initial sync (all contacts) and incremental sync (new/updated since last sync).
# Auto-discovers all lists and syncs each with per-list checkpointing.

import csv
import io
import time
from datetime import datetime

import requests as rq

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


# Schema definition for tables
def schema(configuration: dict):
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
                # OnGage system fields (ocx_*)
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


def get_headers(configuration: dict) -> dict:
    """Build request headers with username/password/account_code authentication."""
    return {
        "x_username": configuration["x_username"],
        "x_password": configuration["x_password"],
        "x_account_code": configuration["x_account_code"],
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def get_base_url(list_id: str = "") -> str:
    """Build the base API URL with optional list_id."""
    if list_id:
        return f"https://api.ongage.net/{list_id}/api"
    return "https://api.ongage.net/api"


def parse_datetime(value) -> str | None:
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


def get_all_lists(configuration: dict) -> list[dict]:
    """Fetch all available lists from the OnGage API."""
    headers = get_headers(configuration)
    base_url = get_base_url()

    response = rq.get(f"{base_url}/lists", headers=headers)
    response.raise_for_status()

    data = response.json()
    lists = data.get("payload", [])
    log.info(f"Found {len(lists)} lists")
    return lists


def create_contact_search(
    configuration: dict, list_id: str, last_sync_time: int | None
) -> str:
    """Create a contact search and return the search ID."""
    base_url = get_base_url(list_id)
    headers = get_headers(configuration)

    # Base criteria: email is not empty (to get all contacts)
    criteria = [
        {
            "type": "email",
            "field_name": "email",
            "operator": "notempty",
            "operand": [""],
            "case_sensitive": 0,
            "condition": "and",
        }
    ]

    # Determine the date filter to apply
    start_filter_time = last_sync_time
    end_filter_time = None

    # Debug mode: use explicit date range for testing (overrides state)
    debug_start = configuration.get("debug_start_date")  # Format: YYYY-MM-DD
    debug_end = configuration.get("debug_end_date")  # Format: YYYY-MM-DD
    if debug_start:
        start_dt = datetime.strptime(debug_start, "%Y-%m-%d")
        start_filter_time = int(start_dt.timestamp())
        log.info(f"Debug mode: filtering contacts from {debug_start}")
        if debug_end:
            end_dt = datetime.strptime(debug_end, "%Y-%m-%d")
            end_filter_time = int(end_dt.timestamp())
            log.info(f"Debug mode: filtering contacts until {debug_end}")

    # Add date filter for incremental sync or debug mode
    if start_filter_time is not None:
        criteria.append(
            {
                "type": "date_absolute",
                "field_name": "ocx_created_date",
                "operator": ">=",
                "operand": [start_filter_time],
                "case_sensitive": 0,
                "condition": "and",
            }
        )
    if end_filter_time is not None:
        criteria.append(
            {
                "type": "date_absolute",
                "field_name": "ocx_created_date",
                "operator": "<=",
                "operand": [end_filter_time],
                "case_sensitive": 0,
                "condition": "and",
            }
        )

    payload = {
        "title": f"Fivetran Sync {datetime.utcnow().isoformat()}",
        "include_behavior": False,
        "filters": {
            "type": "Active",
            "criteria": criteria,
            "user_type": "all",
        },
        "combined_as_and": True,
    }

    log.info(f"Creating contact search for list {list_id} with criteria: {criteria}")
    response = rq.post(f"{base_url}/contact_search", headers=headers, json=payload)
    response.raise_for_status()

    data = response.json()
    search_id = data["payload"]["id"]
    log.info(f"Created contact search with ID: {search_id}")
    return search_id


def wait_for_search_completion(
    configuration: dict, list_id: str, search_id: str, max_wait: int = 300
) -> bool:
    """Poll the contact search status until completed or timeout."""
    base_url = get_base_url(list_id)
    headers = get_headers(configuration)

    start_time = time.time()
    while time.time() - start_time < max_wait:
        response = rq.get(f"{base_url}/contact_search/{search_id}", headers=headers)
        response.raise_for_status()

        data = response.json()
        status = data["payload"].get("status")
        desc = data["payload"].get("desc", "")
        log.fine(f"Contact search status: {status} ({desc})")

        # Status codes: 1 = Pending, 2 = Completed, 3 = Failed (based on API response)
        if status == 2 or str(status).lower() == "completed":
            return True
        elif status == 3 or str(status).lower() == "failed":
            log.warning(f"Contact search failed: {data}")
            return False

        time.sleep(5)  # Poll every 5 seconds

    log.warning("Contact search timed out")
    return False


def export_contacts(configuration: dict, list_id: str, search_id: str):
    """Download and parse the contact search export CSV."""
    base_url = get_base_url(list_id)
    headers = get_headers(configuration)

    response = rq.get(f"{base_url}/contact_search/{search_id}/export", headers=headers)
    response.raise_for_status()

    # Parse CSV content
    csv_content = response.text
    reader = csv.DictReader(io.StringIO(csv_content))

    contacts = []
    for row in reader:
        contact = {
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
        contacts.append(contact)

    log.info(f"Exported {len(contacts)} contacts from list {list_id}")
    return contacts


def sync_list_batch(configuration: dict, list_id: str, start_time: int, end_time: int):
    """Sync contacts for a single list within a date range. Yields upsert operations."""
    log.info(f"Syncing list {list_id} from {start_time} to {end_time}")

    # Create contact search with date range
    search_id = create_contact_search_with_range(
        configuration, list_id, start_time, end_time
    )

    # Wait for search to complete
    if not wait_for_search_completion(configuration, list_id, search_id):
        log.warning(f"Contact search did not complete successfully for list {list_id}")
        return 0

    # Export and process contacts
    contacts = export_contacts(configuration, list_id, search_id)

    for contact in contacts:
        yield op.upsert(table="contacts", data=contact)

    log.info(f"Synced {len(contacts)} contacts from list {list_id}")
    return len(contacts)


def create_contact_search_with_range(
    configuration: dict, list_id: str, start_time: int | None, end_time: int | None
) -> str:
    """Create a contact search with explicit date range and return the search ID."""
    base_url = get_base_url(list_id)
    headers = get_headers(configuration)

    # Base criteria: email is not empty (to get all contacts)
    criteria = [
        {
            "type": "email",
            "field_name": "email",
            "operator": "notempty",
            "operand": [""],
            "case_sensitive": 0,
            "condition": "and",
        }
    ]

    # Add date filters
    if start_time is not None:
        criteria.append(
            {
                "type": "date_absolute",
                "field_name": "ocx_created_date",
                "operator": ">=",
                "operand": [start_time],
                "case_sensitive": 0,
                "condition": "and",
            }
        )
    if end_time is not None:
        criteria.append(
            {
                "type": "date_absolute",
                "field_name": "ocx_created_date",
                "operator": "<",
                "operand": [end_time],
                "case_sensitive": 0,
                "condition": "and",
            }
        )

    payload = {
        "title": f"Fivetran Sync {datetime.utcnow().isoformat()}",
        "include_behavior": False,
        "filters": {
            "type": "Active",
            "criteria": criteria,
            "user_type": "all",
        },
        "combined_as_and": True,
    }

    log.info(f"Creating contact search for list {list_id}")
    response = rq.post(f"{base_url}/contact_search", headers=headers, json=payload)
    response.raise_for_status()

    data = response.json()
    search_id = data["payload"]["id"]
    log.info(f"Created contact search with ID: {search_id}")
    return search_id


# Constants for batching
BATCH_MONTHS = 6
SECONDS_PER_MONTH = 30 * 24 * 60 * 60  # ~30 days
LARGE_LIST_THRESHOLD = (
    200000  # Lists with more contacts than this will use batched sync
)


def sync_list(
    configuration: dict, list_id: str, last_sync_time: int | None, list_count: int = 0
):
    """Sync contacts for a single list. Yields upsert operations.

    For initial sync of large lists (> 200k), uses 6-month batches working backwards.
    For smaller lists or incremental sync, fetches all at once.
    """
    log.info(f"Syncing list {list_id}")

    # Check for debug mode override
    debug_start = configuration.get("debug_start_date")
    if debug_start:
        start_dt = datetime.strptime(debug_start, "%Y-%m-%d")
        start_time = int(start_dt.timestamp())
        end_time = None
        debug_end = configuration.get("debug_end_date")
        if debug_end:
            end_dt = datetime.strptime(debug_end, "%Y-%m-%d")
            end_time = int(end_dt.timestamp())
        log.info(f"Debug mode: filtering contacts from {debug_start} to {debug_end}")

        # Single batch for debug mode
        gen = sync_list_batch(configuration, list_id, start_time, end_time)
        count = 0
        for item in gen:
            if isinstance(item, int):
                count = item
            else:
                yield item
        return

    if last_sync_time is not None:
        # Incremental sync: fetch all since last sync
        gen = sync_list_batch(configuration, list_id, last_sync_time, None)
        for item in gen:
            if not isinstance(item, int):
                yield item
        return

    # Initial sync: check if list is large enough to require batching
    if list_count < LARGE_LIST_THRESHOLD:
        log.info(f"List {list_id} has {list_count} contacts, syncing all at once")
        gen = sync_list_batch(configuration, list_id, None, None)
        for item in gen:
            if not isinstance(item, int):
                yield item
        return

    # Large list: batch by 6-month windows working backwards
    log.info(f"List {list_id} has {list_count} contacts, using batched sync")
    current_time = int(datetime.utcnow().timestamp())
    batch_size = BATCH_MONTHS * SECONDS_PER_MONTH

    end_time = current_time
    batch_num = 0

    while True:
        start_time = end_time - batch_size
        batch_num += 1

        log.info(f"Processing batch {batch_num} for list {list_id}")

        gen = sync_list_batch(configuration, list_id, start_time, end_time)
        count = 0
        for item in gen:
            if isinstance(item, int):
                count = item
            else:
                yield item

        log.info(f"Batch {batch_num} returned {count} contacts")

        # Stop if we got 0 contacts (reached the beginning of data)
        if count == 0:
            log.info(f"No more contacts found for list {list_id}, stopping batches")
            break

        # Move window backwards
        end_time = start_time


def update(configuration: dict, state: dict):
    log.info("OnGage Connector: Starting sync")

    # Get all available lists
    lists = get_all_lists(configuration)
    list_ids = sorted([str(lst.get("id")) for lst in lists if lst.get("id")])

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
        list_data = {
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
        yield op.upsert(table="lists", data=list_data)

    # Process each list
    for list_id in list_ids:
        # Skip already completed lists (for resume after crash)
        if list_id in completed_lists:
            log.info(f"Skipping already completed list {list_id}")
            continue

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
                "last_sync_time": last_sync_time,  # Keep original sync time during batch
            }
        )

    # Final checkpoint: reset completed_lists for next sync, update last_sync_time
    current_sync_time = int(datetime.utcnow().timestamp())
    yield op.checkpoint(
        state={
            "completed_lists": [],  # Reset for next sync
            "last_sync_time": current_sync_time,
        }
    )

    log.info(f"Sync completed. Processed {len(list_ids)} lists.")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
