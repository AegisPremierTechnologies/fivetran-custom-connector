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
PAGE_SIZE = 1000  # Max allowed by Virtuous API
BATCH_SIZE = (
    8000  # Number of rows to accumulate before yielding upserts and checkpointing
)
PARALLEL_REQUESTS = 8  # Number of concurrent API requests


def _extract_gift_date(gift: dict) -> Optional[str]:
    """Extract giftDate from a gift record and return as YYYY-MM-DD string."""
    gift_date = gift.get("giftDate")
    if not gift_date:
        return None
    try:
        # Parse the datetime and return just the date portion
        from dateutil import parser as date_parser

        dt = date_parser.parse(gift_date)
        return dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _fetch_gifts_page(
    configuration: dict,
    skip: int,
    modified_since: Optional[str],
    modified_until: Optional[str],
    gift_date_since: Optional[str],
) -> Tuple[int, list]:
    """Fetch a single page of gifts. Returns (skip, gifts_list)."""
    response = query_gifts(
        configuration,
        skip=skip,
        take=PAGE_SIZE,
        modified_since=modified_since,
        modified_until=modified_until,
        gift_date_since=gift_date_since,
    )
    # Handle response structure - may be {"list": [...]} or just [...]
    gifts = response.get("list", response) if isinstance(response, dict) else response
    return (skip, gifts or [])


def sync_gifts(
    configuration: dict,
    state: dict,
    modified_since: Optional[str] = None,
    modified_until: Optional[str] = None,
) -> Generator[Any, None, dict]:
    """Sync all gifts with parallel fetching and date-based cursor.

    Uses Gift Date as the primary cursor to avoid large SKIP values that can
    cause server-side 500 errors. Fetches PARALLEL_REQUESTS pages concurrently
    within the date-filtered query.

    State variables:
    - gifts_date_cursor: Current date being synced (YYYY-MM-DD)
    - gifts_day_skip: Skip offset within the current date
    - gifts_total_synced: Running total of synced gifts

    Args:
        configuration: Connector configuration
        state: Current sync state (may contain old gifts_skip for migration)
        modified_since: ISO datetime string for incremental sync start (None for full sync)
        modified_until: ISO datetime string for incremental sync end (debug mode)

    Yields:
        Upsert operations and checkpoint operations

    Returns:
        Updated state dict
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Initialize state variables
    date_cursor = state.get("gifts_date_cursor")
    day_skip = state.get("gifts_day_skip", 0)
    total_synced = state.get("gifts_total_synced", 0)

    if date_cursor:
        log.info(
            f"Resuming gifts sync from date={date_cursor}, day_skip={day_skip}, total_synced={total_synced}"
        )
    else:
        log.info(f"Starting fresh gifts sync (no date cursor)")

    # Buffer for accumulating rows before batch yield
    batch_buffer: List[dict] = []
    is_first_record = total_synced == 0
    reached_end = False
    last_gift_date_in_batch: Optional[str] = None

    while not reached_end:
        # Submit parallel requests
        log.info(f"Parallel fetch: date_cursor={date_cursor}, starting skip={day_skip}")

        with ThreadPoolExecutor(max_workers=PARALLEL_REQUESTS) as executor:
            futures = {}
            for i in range(PARALLEL_REQUESTS):
                page_skip = day_skip + (i * PAGE_SIZE)
                future = executor.submit(
                    _fetch_gifts_page,
                    configuration,
                    page_skip,
                    modified_since,
                    modified_until,
                    date_cursor,
                )
                futures[future] = page_skip

            # Collect results
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

        log.info(f"Fetched {len(results)} pages in parallel")

        # Process results in order
        for page_skip, gifts in results:
            if not gifts:
                log.info(f"Empty page at skip={page_skip}, reached end")
                reached_end = True
                break

            for i, gift in enumerate(gifts):
                # Extract this gift's date
                gift_date = _extract_gift_date(gift)
                if gift_date:
                    last_gift_date_in_batch = gift_date

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

        # Update skip position for next parallel batch
        if not reached_end:
            day_skip += PARALLEL_REQUESTS * PAGE_SIZE

        log.info(f"Progress: total={total_synced}, buffer={len(batch_buffer)}")

        # Check if we should yield batch and checkpoint
        if len(batch_buffer) >= BATCH_SIZE:
            log.info(f"Yielding batch of {len(batch_buffer)} gifts and checkpointing")

            # Yield all buffered upserts
            for item in batch_buffer:
                yield op.upsert(table=item["table"], data=item["data"])

            # Clear buffer
            batch_buffer = []

            # Update date cursor based on last gift in batch
            if last_gift_date_in_batch:
                if date_cursor is None:
                    log.info(
                        f"Setting initial date cursor to {last_gift_date_in_batch}"
                    )
                    date_cursor = last_gift_date_in_batch
                    day_skip = 0  # Reset skip since we now have a date filter
                elif last_gift_date_in_batch > date_cursor:
                    log.info(
                        f"Date cursor advanced from {date_cursor} to {last_gift_date_in_batch}"
                    )
                    date_cursor = last_gift_date_in_batch
                    day_skip = 0  # Reset skip for new date

            # Checkpoint with current position
            state["gifts_date_cursor"] = date_cursor
            state["gifts_day_skip"] = day_skip
            state["gifts_total_synced"] = total_synced
            yield op.checkpoint(state=state)

    # Yield any remaining buffered rows
    if batch_buffer:
        log.info(f"Yielding final batch of {len(batch_buffer)} gifts")
        for item in batch_buffer:
            yield op.upsert(table=item["table"], data=item["data"])

    log.info(f"Gifts sync complete. Total synced: {total_synced}")

    # Mark gifts as complete and clear cursor fields
    state["gifts_complete"] = True
    state.pop("gifts_date_cursor", None)
    state.pop("gifts_day_skip", None)
    state.pop("gifts_total_synced", None)

    return state


def _fetch_contacts_page(
    configuration: dict,
    skip: int,
    modified_since: Optional[str],
    modified_until: Optional[str],
    id_cursor: Optional[int],
) -> Tuple[int, list]:
    """Fetch a single page of contacts. Returns (skip, contacts_list)."""
    response = query_contacts(
        configuration,
        skip=skip,
        take=PAGE_SIZE,
        modified_since=modified_since,
        modified_until=modified_until,
        id_cursor=id_cursor,
    )
    # Handle response structure - may be {"list": [...]} or just [...]
    contacts = (
        response.get("list", response) if isinstance(response, dict) else response
    )
    return (skip, contacts or [])


def sync_contacts(
    configuration: dict,
    state: dict,
    modified_since: Optional[str] = None,
    modified_until: Optional[str] = None,
) -> Generator[Any, None, dict]:
    """Sync all contacts and related entities with parallel fetching and ID-based cursor.

    Uses Contact ID as the primary cursor to avoid large SKIP values that can
    cause server-side 500 errors. Fetches PARALLEL_REQUESTS pages concurrently
    within the ID-filtered query.

    State variables:
    - contacts_id_cursor: Last synced contact ID
    - contacts_total_synced: Running total of synced contacts

    Yields upsert operations for:
    - contacts: Main contact record
    - individuals: People within the contact (from contactIndividuals)
    - addresses: Contact addresses (from address field)
    - contact_methods: Phone, email, etc. (from individual.contactMethods)

    Args:
        configuration: Connector configuration
        state: Current sync state
        modified_since: ISO datetime string for incremental sync start (None for full sync)
        modified_until: ISO datetime string for incremental sync end (debug mode)

    Yields:
        Upsert operations and checkpoint operations

    Returns:
        Updated state dict
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Initialize state variables - ID cursor is an int (or None for fresh start)
    id_cursor = state.get("contacts_id_cursor")
    total_contacts = state.get("contacts_total_synced", 0)
    total_individuals = 0
    total_addresses = 0
    total_methods = 0

    if id_cursor is not None:
        log.info(
            f"Resuming contacts sync from id_cursor={id_cursor}, total_contacts={total_contacts}"
        )
    else:
        log.info(f"Starting fresh contacts sync (no ID cursor)")

    # Buffer for accumulating rows before batch yield
    batch_buffer: List[dict] = []
    is_first_record = total_contacts == 0
    reached_end = False
    last_contact_id_in_batch: Optional[int] = None

    while not reached_end:
        # Submit parallel requests - always start skip at 0 since ID cursor handles position
        log.info(f"Parallel fetch contacts: id_cursor={id_cursor}, skip=0")

        with ThreadPoolExecutor(max_workers=PARALLEL_REQUESTS) as executor:
            futures = {}
            for i in range(PARALLEL_REQUESTS):
                page_skip = i * PAGE_SIZE
                future = executor.submit(
                    _fetch_contacts_page,
                    configuration,
                    page_skip,
                    modified_since,
                    modified_until,
                    id_cursor,
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

        log.info(f"Fetched {len(results)} contact pages in parallel")

        # Process results in order
        for page_skip, contacts in results:
            if not contacts:
                log.info(f"Empty page at skip={page_skip}, reached end")
                reached_end = True
                break

            for i, contact in enumerate(contacts):
                contact_id = contact.get("id")
                contact_id_str = str(contact_id) if contact_id else ""
                debug = is_first_record and i == 0 and len(batch_buffer) == 0

                # Track highest ID in batch for cursor
                if contact_id is not None:
                    if (
                        last_contact_id_in_batch is None
                        or contact_id > last_contact_id_in_batch
                    ):
                        last_contact_id_in_batch = contact_id

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
                            "data": format_address(address, contact_id_str),
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
                            "data": format_individual(individual, contact_id_str),
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
                                    method, contact_id_str, individual_id
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

        # Update ID cursor for next batch (filter by ID > last_id)
        if last_contact_id_in_batch is not None:
            id_cursor = last_contact_id_in_batch

        log.info(
            f"Progress: contacts={total_contacts}, individuals={total_individuals}, buffer={len(batch_buffer)}, last_id={last_contact_id_in_batch}"
        )

        # Check if we should yield batch and checkpoint
        if len(batch_buffer) >= BATCH_SIZE:
            log.info(
                f"Yielding batch of {len(batch_buffer)} rows and checkpointing at id_cursor={id_cursor}"
            )

            # Yield all buffered upserts
            for item in batch_buffer:
                yield op.upsert(table=item["table"], data=item["data"])

            # Clear buffer
            batch_buffer = []

            # Checkpoint with current ID position
            state["contacts_id_cursor"] = id_cursor
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

    # Mark contacts as complete and clear cursor fields
    state["contacts_complete"] = True
    state.pop("contacts_id_cursor", None)
    state.pop("contacts_total_synced", None)

    return state
