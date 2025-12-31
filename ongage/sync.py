"""Sync logic for OnGage connector with adaptive batching."""

import time
from datetime import datetime
from typing import Optional

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api import (
    SearchResult,
    create_contact_search,
    export_contacts_csv,
    wait_for_search_completion,
)
from models import parse_contacts_csv


# Batching configuration
INITIAL_BATCH_MONTHS = 3
SECONDS_PER_MONTH = 30 * 24 * 60 * 60  # ~30 days
MIN_BATCH_SECONDS = 14 * 24 * 60 * 60  # 2 weeks minimum
LARGE_LIST_THRESHOLD = 200000  # Lists with more contacts use batched sync
MAX_RETRIES_PER_BATCH = 3
RETRY_WAIT_SECONDS = 30


def fetch_contacts(
    configuration: dict,
    list_id: str,
    start_time: Optional[int],
    end_time: Optional[int],
) -> tuple[list[dict], str]:
    """Fetch contacts for a date range.

    Returns: (contacts list, result status: 'success', 'timeout', 'failed')
    """
    search_id = create_contact_search(configuration, list_id, start_time, end_time)
    result = wait_for_search_completion(configuration, list_id, search_id)

    if result == SearchResult.SUCCESS:
        csv_content = export_contacts_csv(configuration, list_id, search_id)
        contacts = parse_contacts_csv(csv_content, list_id)
        log.info(f"Fetched {len(contacts)} contacts from list {list_id}")
        return contacts, SearchResult.SUCCESS

    return [], result


def fetch_contacts_with_adaptive_retry(
    configuration: dict,
    list_id: str,
    start_time: int,
    end_time: int,
) -> list[dict]:
    """Fetch contacts with adaptive retry - halves batch size on timeout.

    If a batch times out, waits and retries with half the batch size.
    Continues recursively splitting until MIN_BATCH_SECONDS or MAX_RETRIES.
    """
    batch_size = end_time - start_time
    retries = 0

    while retries < MAX_RETRIES_PER_BATCH:
        log.info(
            f"Attempting fetch for list {list_id}: {start_time} to {end_time} (batch size: {batch_size // (24*60*60)} days)"
        )

        contacts, result = fetch_contacts(configuration, list_id, start_time, end_time)

        if result == SearchResult.SUCCESS:
            return contacts

        if result == SearchResult.FAILED:
            log.warning(f"Search failed for list {list_id}, skipping batch")
            return []

        # Timeout - try smaller batch
        if batch_size <= MIN_BATCH_SECONDS:
            log.warning(
                f"Batch already at minimum size for list {list_id}, giving up on this batch"
            )
            return []

        retries += 1
        new_batch_size = batch_size // 2
        log.warning(
            f"Timeout on list {list_id}, retrying with smaller batch ({new_batch_size // (24*60*60)} days) after {RETRY_WAIT_SECONDS}s wait"
        )
        time.sleep(RETRY_WAIT_SECONDS)

        # Split into two halves and fetch each
        mid_time = start_time + new_batch_size

        # Fetch first half
        first_half = fetch_contacts_with_adaptive_retry(
            configuration, list_id, start_time, mid_time
        )

        # Fetch second half
        second_half = fetch_contacts_with_adaptive_retry(
            configuration, list_id, mid_time, end_time
        )

        return first_half + second_half

    log.warning(f"Max retries exceeded for list {list_id}")
    return []


def sync_list_simple(
    configuration: dict,
    list_id: str,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
):
    """Sync a list with a single fetch (no batching). Yields upsert operations."""
    contacts, result = fetch_contacts(configuration, list_id, start_time, end_time)

    for contact in contacts:
        yield op.upsert(table="contacts", data=contact)

    log.info(f"Synced {len(contacts)} contacts from list {list_id}")


def sync_list_batched(configuration: dict, list_id: str):
    """Sync a large list using time-based batching with adaptive retry.

    Works backwards from current time in INITIAL_BATCH_MONTHS chunks.
    Uses adaptive retry on timeout.
    """
    current_time = int(datetime.utcnow().timestamp())
    batch_size = INITIAL_BATCH_MONTHS * SECONDS_PER_MONTH

    end_time = current_time
    batch_num = 0

    while True:
        start_time = end_time - batch_size
        batch_num += 1

        log.info(f"Processing batch {batch_num} for list {list_id}")

        contacts = fetch_contacts_with_adaptive_retry(
            configuration, list_id, start_time, end_time
        )

        # for contact in contacts:
        # yield op.upsert(table="contacts", data=contact)

        count = len(contacts)
        log.info(f"Batch {batch_num} returned {count} contacts")

        # Stop if we got 0 contacts (reached the beginning of data)
        if count == 0:
            log.info(f"No more contacts found for list {list_id}, stopping batches")
            break

        # Move window backwards
        end_time = start_time


def sync_list(
    configuration: dict,
    list_id: str,
    last_sync_time: Optional[int],
    list_count: int = 0,
):
    """Sync contacts for a single list. Yields upsert operations.

    - Debug mode: uses explicit date range from config
    - Incremental sync: fetches all since last_sync_time
    - Initial sync (small list): fetches all at once
    - Initial sync (large list): uses batched sync with adaptive retry
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

        yield from sync_list_simple(configuration, list_id, start_time, end_time)
        return

    if last_sync_time is not None:
        # Incremental sync: fetch all since last sync
        log.info(f"Incremental sync for list {list_id} since {last_sync_time}")
        yield from sync_list_simple(configuration, list_id, last_sync_time, None)
        return

    # Initial sync: check if list is large enough to require batching
    if list_count < LARGE_LIST_THRESHOLD:
        log.info(f"List {list_id} has {list_count} contacts, syncing all at once")
        yield from sync_list_simple(configuration, list_id, None, None)
        return

    # Large list: use batched sync with adaptive retry
    log.info(f"List {list_id} has {list_count} contacts, using batched sync")
    yield from sync_list_batched(configuration, list_id)
