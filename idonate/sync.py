"""Sync logic for iDonate connector with date-based batching and pagination."""

from datetime import datetime, timedelta
from typing import Optional

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api import query_transactions
from models import (
    _extract_or_hash_contact_id,
    format_contact,
    format_transaction,
)


# Batching configuration
INITIAL_BATCH_DAYS = 30  # Start with 30-day batches
MIN_BATCH_DAYS = 1  # Minimum batch size: 1 day
MAX_RETRIES_PER_BATCH = 3
PAGE_SIZE = 100  # iDonate max is 100 per page


def format_date_for_api(dt: datetime) -> str:
    """Format datetime for iDonate API: YYYYMMDDTHHmmss"""
    return dt.strftime("%Y%m%dT%H%M%S")


def parse_api_date(date_str: str) -> datetime:
    """Parse iDonate API date format or ISO format to datetime."""
    # Try API format first (YYYYMMDDTHHmmss)
    try:
        return datetime.strptime(date_str, "%Y%m%dT%H%M%S")
    except ValueError:
        pass
    
    # Try ISO format with Z
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError:
        pass
    
    # Try ISO format without timezone
    return datetime.fromisoformat(date_str)


def fetch_transactions_for_date_range(
    configuration: dict,
    organization_id: str,
    start_date: str,
    end_date: str,
) -> list[dict]:
    """Fetch all transactions within a date range using pagination.

    Handles pagination across all pages in the date range.

    Returns:
        List of transaction dicts (raw from API)
    """
    all_transactions = []
    page = 1

    while True:
        try:
            response = query_transactions(
                configuration=configuration,
                organization_id=organization_id,
                start_date=start_date,
                end_date=end_date,
                page=page,
                per_page=PAGE_SIZE,
            )

            result = response.get("result", {})
            items = result.get("items", [])
            count = result.get("count", 0)
            # total_count = result.get("total_count", 0)

            if not items:
                log.info(
                    f"No transactions found for {start_date} to {end_date} on page {page}"
                )
                break

            all_transactions.extend(items)
            log.info(
                f"Fetched {len(items)} transactions (page {page}, total so far: {len(all_transactions)})"
            )

            # Check if there are more pages
            if not result.get("more", False) or count < PAGE_SIZE:
                break

            page += 1

        except Exception as e:
            log.warning(
                f"Error fetching page {page} for range {start_date} to {end_date}: {e}"
            )
            # Return what we have so far and let upstream decide whether to retry
            break

    return all_transactions


def fetch_transactions_with_retry(
    configuration: dict,
    organization_id: str,
    start_date: str,
    end_date: str,
) -> list[dict]:
    """Fetch transactions with retry logic. Currently a simple wrapper.

    In future, could add exponential backoff or batch-size reduction on timeout.
    """
    return fetch_transactions_for_date_range(
        configuration=configuration,
        organization_id=organization_id,
        start_date=start_date,
        end_date=end_date,
    )


def sync_transactions_date_range(
    configuration: dict,
    organization_id: str,
    start_date: str,
    end_date: str,
):
    """Sync transactions and contacts with contact_id caching for performance.
    
    Maintains in-memory cache of contact_id -> contact to deduplicate lookups.
    O(n) complexity with single pass through transactions.
    """
    log.info(f"Syncing transactions from {start_date} to {end_date}")

    transactions = fetch_transactions_with_retry(
        configuration=configuration,
        organization_id=organization_id,
        start_date=start_date,
        end_date=end_date,
    )

    # Contact cache: contact_id -> contact data (enables O(1) dedup)
    contact_cache = {}

    # First pass: extract unique contacts and upsert
    for transaction in transactions:
        contact = transaction.get("contact")
        if contact:
            contact_id = _extract_or_hash_contact_id(contact)
            if contact_id not in contact_cache:
                contact_cache[contact_id] = contact
                formatted_contact = format_contact(contact, contact_id=contact_id)
                yield op.upsert(table="contacts", data=formatted_contact)

    # Second pass: upsert transactions with contact_id reference
    for transaction in transactions:
        contact = transaction.get("contact")
        contact_id = _extract_or_hash_contact_id(contact) if contact else None
        formatted = format_transaction(transaction, contact_id=contact_id)
        yield op.upsert(table="transactions", data=formatted)

    log.info(
        f"Synced {len(transactions)} transactions and {len(contact_cache)} unique contacts"
    )


def sync_transactions_batched(
    configuration: dict,
    organization_id: str,
    start_date: str,
    end_date: str,
):
    """Sync transactions using time-based batching.

    Breaks date range into chunks (days) and fetches each with pagination.
    Checkpoints after each batch.
    """
    # Parse dates
    start_dt = parse_api_date(start_date)
    end_dt = parse_api_date(end_date)

    batch_days = INITIAL_BATCH_DAYS
    current_dt = start_dt

    batch_num = 0
    contact_cache = {}  # Persists across batches for O(1) dedup

    while current_dt < end_dt:
        batch_num += 1

        # Calculate batch end (don't exceed overall end_date)
        batch_end_dt = min(current_dt + timedelta(days=batch_days), end_dt)

        batch_start = format_date_for_api(current_dt)
        batch_end = format_date_for_api(batch_end_dt)

        log.info(
            f"Batch {batch_num}: Syncing transactions from {batch_start} to {batch_end}"
        )

        transactions = fetch_transactions_with_retry(
            configuration=configuration,
            organization_id=organization_id,
            start_date=batch_start,
            end_date=batch_end,
        )

        # First pass: extract unique contacts and upsert (using contact_id cache)
        batch_contacts = 0
        for transaction in transactions:
            contact = transaction.get("contact")
            if contact:
                contact_id = _extract_or_hash_contact_id(contact)
                if contact_id not in contact_cache:
                    contact_cache[contact_id] = contact
                    batch_contacts += 1
                    formatted_contact = format_contact(contact, contact_id=contact_id)
                    yield op.upsert(table="contacts", data=formatted_contact)

        # Second pass: upsert transactions with contact_id reference
        for transaction in transactions:
            contact = transaction.get("contact")
            contact_id = _extract_or_hash_contact_id(contact) if contact else None
            formatted = format_transaction(transaction, contact_id=contact_id)
            yield op.upsert(table="transactions", data=formatted)

        count = len(transactions)
        log.info(
            f"Batch {batch_num} returned {count} transactions, {batch_contacts} new contacts, {len(contact_cache)} total unique contacts"
        )

        # Move to next batch
        current_dt = batch_end_dt


def sync_organization(
    configuration: dict,
    organization_id: str,
    last_sync_time: Optional[str],
    state: dict,
):
    """Sync transactions for a single organization.

    Handles incremental and initial syncs based on last_sync_time.
    Yields upsert operations and checkpoints progress.
    """
    log.info(f"Syncing organization {organization_id}")

    # Check for debug mode override
    debug_start = configuration.get("debug_start_date")
    debug_end = configuration.get("debug_end_date")

    if debug_start:
        start_date = debug_start
        end_date = debug_end or format_date_for_api(datetime.utcnow())
        log.info(f"Debug mode: syncing from {start_date} to {end_date}")
        yield from sync_transactions_date_range(
            configuration, organization_id, start_date, end_date
        )
        return

    if last_sync_time:
        # Incremental sync: fetch from last sync to now
        now = format_date_for_api(datetime.utcnow())
        log.info(f"Incremental sync from {last_sync_time} to {now}")
        yield from sync_transactions_date_range(
            configuration, organization_id, last_sync_time, now
        )
    else:
        # Initial sync: fetch all historical data using batched approach
        # Assume we start from 1 year ago (configurable)
        days_back = configuration.get("initial_sync_days_back", 365)
        start_dt = datetime.utcnow() - timedelta(days=int(days_back))
        start_date = format_date_for_api(start_dt)
        end_date = format_date_for_api(datetime.utcnow())

        log.info(
            f"Initial sync using batching, fetching last {days_back} days of data"
        )
        yield from sync_transactions_batched(
            configuration, organization_id, start_date, end_date
        )
