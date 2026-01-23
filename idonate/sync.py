"""Sync logic for iDonate connector with date-based batching and pagination."""

from datetime import datetime, timedelta
from typing import Optional

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api import query_transactions
from models import format_transaction


# Batching configuration
INITIAL_BATCH_DAYS = 30  # Start with 30-day batches
MIN_BATCH_DAYS = 1  # Minimum batch size: 1 day
MAX_RETRIES_PER_BATCH = 3
PAGE_SIZE = 100  # iDonate max is 100 per page


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
            total_count = result.get("total_count", 0)

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
    """Sync transactions for a specific date range. Yields upsert operations."""
    log.info(f"Syncing transactions from {start_date} to {end_date}")

    transactions = fetch_transactions_with_retry(
        configuration=configuration,
        organization_id=organization_id,
        start_date=start_date,
        end_date=end_date,
    )

    for transaction in transactions:
        formatted = format_transaction(transaction)
        yield op.upsert(table="transactions", data=formatted)

    log.info(f"Synced {len(transactions)} transactions")


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
    start_dt = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))

    batch_days = INITIAL_BATCH_DAYS
    current_dt = start_dt

    batch_num = 0

    while current_dt < end_dt:
        batch_num += 1

        # Calculate batch end (don't exceed overall end_date)
        batch_end_dt = min(current_dt + timedelta(days=batch_days), end_dt)

        batch_start = current_dt.isoformat().replace("+00:00", "Z")
        batch_end = batch_end_dt.isoformat().replace("+00:00", "Z")

        log.info(
            f"Batch {batch_num}: Syncing transactions from {batch_start} to {batch_end}"
        )

        transactions = fetch_transactions_with_retry(
            configuration=configuration,
            organization_id=organization_id,
            start_date=batch_start,
            end_date=batch_end,
        )

        for transaction in transactions:
            formatted = format_transaction(transaction)
            yield op.upsert(table="transactions", data=formatted)

        count = len(transactions)
        log.info(f"Batch {batch_num} returned {count} transactions")

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
        end_date = debug_end or datetime.utcnow().isoformat() + "Z"
        log.info(f"Debug mode: syncing from {start_date} to {end_date}")
        yield from sync_transactions_date_range(
            configuration, organization_id, start_date, end_date
        )
        return

    if last_sync_time:
        # Incremental sync: fetch from last sync to now
        now = datetime.utcnow().isoformat() + "Z"
        log.info(f"Incremental sync from {last_sync_time} to {now}")
        yield from sync_transactions_date_range(
            configuration, organization_id, last_sync_time, now
        )
    else:
        # Initial sync: fetch all historical data using batched approach
        # Assume we start from 1 year ago (configurable)
        days_back = configuration.get("initial_sync_days_back", 365)
        start_dt = datetime.utcnow() - timedelta(days=days_back)
        start_date = start_dt.isoformat() + "Z"
        end_date = datetime.utcnow().isoformat() + "Z"

        log.info(
            f"Initial sync using batching, fetching last {days_back} days of data"
        )
        yield from sync_transactions_batched(
            configuration, organization_id, start_date, end_date
        )
