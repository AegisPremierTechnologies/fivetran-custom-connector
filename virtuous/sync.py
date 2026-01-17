"""Sync logic for Virtuous connector with parallel fetching and batch checkpointing.

Implements parallel API requests and 20k row batch syncing with state-based
checkpoints for resume capability.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Generator, Any, List, Tuple

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api import query_gifts, query_contacts
from models import (
    format_gift,
    format_contact,
    format_individual,
    format_address,
    format_contact_method,
)


# Pagination configuration
PAGE_SIZE = 500  # Max allowed by Virtuous API
BATCH_SIZE = (
    4000  # Number of rows to accumulate before yielding upserts and checkpointing
)
PARALLEL_REQUESTS = 4  # Number of concurrent API requests


def _fetch_gifts_page(
    configuration: dict,
    skip: int,
    modified_since: Optional[str],
    modified_until: Optional[str],
) -> Tuple[int, list]:
    """Fetch a single page of gifts. Returns (skip, gifts_list)."""
    response = query_gifts(
        configuration,
        skip=skip,
        take=PAGE_SIZE,
        modified_since=modified_since,
        modified_until=modified_until,
    )
    # Handle response structure - may be {"list": [...]} or just [...]
    gifts = response.get("list", response) if isinstance(response, dict) else response
    return (skip, gifts or [])


def _fetch_contacts_page(
    configuration: dict,
    skip: int,
    modified_since: Optional[str],
    modified_until: Optional[str],
) -> Tuple[int, list]:
    """Fetch a single page of contacts. Returns (skip, contacts_list)."""
    response = query_contacts(
        configuration,
        skip=skip,
        take=PAGE_SIZE,
        modified_since=modified_since,
        modified_until=modified_until,
    )
    # Handle response structure - may be {"list": [...]} or just [...]
    contacts = (
        response.get("list", response) if isinstance(response, dict) else response
    )
    return (skip, contacts or [])


def sync_gifts(
    configuration: dict,
    state: dict,
    modified_since: Optional[str] = None,
    modified_until: Optional[str] = None,
) -> Generator[Any, None, dict]:
    """Sync all gifts with parallel fetching and batch checkpointing.

    Fetches PARALLEL_REQUESTS pages concurrently, accumulates rows in batches
    of BATCH_SIZE (20k), yields all upserts, then checkpoints with the current
    skip position for resume capability.

    Args:
        configuration: Connector configuration
        state: Current sync state (may contain gifts_skip for resume)
        modified_since: ISO datetime string for incremental sync start (None for full sync)
        modified_until: ISO datetime string for incremental sync end (debug mode)

    Yields:
        Upsert operations and checkpoint operations

    Returns:
        Updated state dict
    """
    # Resume from state if available
    skip = state.get("gifts_skip", 0)
    total_synced = state.get("gifts_total_synced", 0)

    if skip > 0:
        log.info(f"Resuming gifts sync from skip={skip}, total_synced={total_synced}")

    # Buffer for accumulating rows before batch yield
    batch_buffer: List[dict] = []
    is_first_record = total_synced == 0
    reached_end = False

    while not reached_end:
        # Submit parallel requests
        with ThreadPoolExecutor(max_workers=PARALLEL_REQUESTS) as executor:
            futures = {}
            for i in range(PARALLEL_REQUESTS):
                page_skip = skip + (i * PAGE_SIZE)
                future = executor.submit(
                    _fetch_gifts_page,
                    configuration,
                    page_skip,
                    modified_since,
                    modified_until,
                )
                futures[future] = page_skip

            # Collect results and sort by skip to maintain order
            results: List[Tuple[int, list]] = []
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    log.severe(f"Error fetching gifts page: {e}")
                    raise

        # Sort results by skip position to maintain order
        results.sort(key=lambda x: x[0])

        log.info(f"Fetched {len(results)} pages in parallel starting at skip={skip}")

        # Process results in order
        for page_skip, gifts in results:
            if not gifts:
                log.info(f"Empty page at skip={page_skip}, reached end")
                reached_end = True
                break

            for i, gift in enumerate(gifts):
                batch_buffer.append(
                    {
                        "table": "gifts",
                        "data": format_gift(
                            gift,
                            debug=(
                                is_first_record and i == 0 and len(batch_buffer) == 0
                            ),
                        ),
                    }
                )

            total_synced += len(gifts)
            is_first_record = False

            # Check if this page indicates end of data
            if len(gifts) < PAGE_SIZE:
                log.info(
                    f"Partial page at skip={page_skip} ({len(gifts)} records), reached end"
                )
                reached_end = True
                break

        # Update skip position to after all fetched pages
        if not reached_end:
            skip += PARALLEL_REQUESTS * PAGE_SIZE
        else:
            # Find the actual final skip position based on last full page
            for page_skip, gifts in reversed(results):
                if gifts and len(gifts) == PAGE_SIZE:
                    skip = page_skip + PAGE_SIZE
                    break

        log.info(
            f"Parallel fetch complete. Total: {total_synced}, buffer: {len(batch_buffer)}"
        )

        # Check if we should yield batch and checkpoint
        if len(batch_buffer) >= BATCH_SIZE:
            log.info(
                f"Yielding batch of {len(batch_buffer)} gifts and checkpointing at skip={skip}"
            )

            # Yield all buffered upserts
            for item in batch_buffer:
                yield op.upsert(table=item["table"], data=item["data"])

            # Clear buffer
            batch_buffer = []

            # Checkpoint with current position
            state["gifts_skip"] = skip
            state["gifts_total_synced"] = total_synced
            yield op.checkpoint(state=state)

    # Yield any remaining buffered rows
    if batch_buffer:
        log.info(f"Yielding final batch of {len(batch_buffer)} gifts")
        for item in batch_buffer:
            yield op.upsert(table=item["table"], data=item["data"])

    log.info(f"Gifts sync complete. Total synced: {total_synced}")

    # Mark gifts as complete and clear skip position
    state["gifts_complete"] = True
    state.pop("gifts_skip", None)
    state.pop("gifts_total_synced", None)

    return state


def sync_contacts(
    configuration: dict,
    state: dict,
    modified_since: Optional[str] = None,
    modified_until: Optional[str] = None,
) -> Generator[Any, None, dict]:
    """Sync all contacts and related entities with parallel fetching and batch checkpointing.

    Fetches PARALLEL_REQUESTS pages concurrently, accumulates rows in batches
    of BATCH_SIZE (20k), yields all upserts, then checkpoints with the current
    skip position for resume capability.

    Yields upsert operations for:
    - contacts: Main contact record
    - individuals: People within the contact (from contactIndividuals)
    - addresses: Contact addresses (from address field)
    - contact_methods: Phone, email, etc. (from individual.contactMethods)

    Args:
        configuration: Connector configuration
        state: Current sync state (may contain contacts_skip for resume)
        modified_since: ISO datetime string for incremental sync start (None for full sync)
        modified_until: ISO datetime string for incremental sync end (debug mode)

    Yields:
        Upsert operations and checkpoint operations

    Returns:
        Updated state dict
    """
    # Resume from state if available
    skip = state.get("contacts_skip", 0)
    total_contacts = state.get("contacts_total_synced", 0)
    total_individuals = 0
    total_addresses = 0
    total_methods = 0

    if skip > 0:
        log.info(
            f"Resuming contacts sync from skip={skip}, total_contacts={total_contacts}"
        )

    # Buffer for accumulating rows before batch yield
    batch_buffer: List[dict] = []
    is_first_record = total_contacts == 0
    reached_end = False

    while not reached_end:
        # Submit parallel requests
        with ThreadPoolExecutor(max_workers=PARALLEL_REQUESTS) as executor:
            futures = {}
            for i in range(PARALLEL_REQUESTS):
                page_skip = skip + (i * PAGE_SIZE)
                future = executor.submit(
                    _fetch_contacts_page,
                    configuration,
                    page_skip,
                    modified_since,
                    modified_until,
                )
                futures[future] = page_skip

            # Collect results
            results: List[Tuple[int, list]] = []
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    log.severe(f"Error fetching contacts page: {e}")
                    raise

        # Sort results by skip position to maintain order
        results.sort(key=lambda x: x[0])

        log.info(
            f"Fetched {len(results)} contact pages in parallel starting at skip={skip}"
        )

        # Process results in order
        for page_skip, contacts in results:
            if not contacts:
                log.info(f"Empty page at skip={page_skip}, reached end")
                reached_end = True
                break

            for i, contact in enumerate(contacts):
                contact_id = str(contact.get("id", ""))
                debug = is_first_record and i == 0 and len(batch_buffer) == 0

                # 1. Add main contact record to buffer
                batch_buffer.append(
                    {
                        "table": "contacts",
                        "data": format_contact(contact, debug=debug),
                    }
                )
                total_contacts += 1

                # 2. Add address(es) - API returns single "address" object
                address = contact.get("address")
                if address and address.get("id"):
                    batch_buffer.append(
                        {
                            "table": "addresses",
                            "data": format_address(address, contact_id),
                        }
                    )
                    total_addresses += 1

                # 3. Add individuals and their contact methods
                individuals = contact.get("contactIndividuals", [])
                for individual in individuals:
                    individual_id = str(individual.get("id", ""))

                    batch_buffer.append(
                        {
                            "table": "individuals",
                            "data": format_individual(individual, contact_id),
                        }
                    )
                    total_individuals += 1

                    # 4. Add contact methods for this individual
                    methods = individual.get("contactMethods", [])
                    for method in methods:
                        batch_buffer.append(
                            {
                                "table": "contact_methods",
                                "data": format_contact_method(
                                    method, contact_id, individual_id
                                ),
                            }
                        )
                        total_methods += 1

            is_first_record = False

            # Check if this page indicates end of data
            if len(contacts) < PAGE_SIZE:
                log.info(
                    f"Partial page at skip={page_skip} ({len(contacts)} records), reached end"
                )
                reached_end = True
                break

        # Update skip position to after all fetched pages
        if not reached_end:
            skip += PARALLEL_REQUESTS * PAGE_SIZE
        else:
            # Find the actual final skip position based on last full page
            for page_skip, contacts in reversed(results):
                if contacts and len(contacts) == PAGE_SIZE:
                    skip = page_skip + PAGE_SIZE
                    break

        log.info(
            f"Parallel fetch complete. Contacts: {total_contacts}, "
            f"individuals: {total_individuals}, buffer: {len(batch_buffer)}"
        )

        # Check if we should yield batch and checkpoint
        if len(batch_buffer) >= BATCH_SIZE:
            log.info(
                f"Yielding batch of {len(batch_buffer)} rows and checkpointing at skip={skip}"
            )

            # Yield all buffered upserts
            for item in batch_buffer:
                yield op.upsert(table=item["table"], data=item["data"])

            # Clear buffer
            batch_buffer = []

            # Checkpoint with current position
            state["contacts_skip"] = skip
            state["contacts_total_synced"] = total_contacts
            yield op.checkpoint(state=state)

    # Yield any remaining buffered rows
    if batch_buffer:
        log.info(f"Yielding final batch of {len(batch_buffer)} rows")
        for item in batch_buffer:
            yield op.upsert(table=item["table"], data=item["data"])

    log.info(
        f"Contacts sync complete. Totals: {total_contacts} contacts, "
        f"{total_individuals} individuals, {total_addresses} addresses, "
        f"{total_methods} contact methods"
    )

    # Mark contacts as complete and clear skip position
    state["contacts_complete"] = True
    state.pop("contacts_skip", None)
    state.pop("contacts_total_synced", None)

    return state
