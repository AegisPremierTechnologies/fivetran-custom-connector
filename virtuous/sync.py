"""Sync logic for Virtuous connector with parallel fetching and batch checkpointing.

Uses shared fetch primitives for ID-based cursor pagination, parallel requests,
and adaptive query sizing on errors.
"""

from typing import Any, Callable, Generator, List, Optional

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api import query_gifts, query_contacts
from fetch import IDCursor, ErrorRecord, fetch_batch_parallel, BATCH_SIZE
from models import (
    format_gift,
    format_contact,
    format_individual,
    format_address,
    format_contact_method,
)


def _extract_gift_id(gift: dict) -> Optional[int]:
    """Extract gift ID from a gift record."""
    gift_id = gift.get("id")
    try:
        return int(gift_id) if gift_id is not None else None
    except (ValueError, TypeError):
        return None


def _extract_contact_id(contact: dict) -> Optional[int]:
    """Extract contact ID from a contact record."""
    contact_id = contact.get("id")
    try:
        return int(contact_id) if contact_id is not None else None
    except (ValueError, TypeError):
        return None


def _transform_gifts(gifts: list, is_first_batch: bool = False) -> List[dict]:
    """Transform gifts to upsert rows."""
    return [
        {"table": "gifts", "data": format_gift(g, debug=(is_first_batch and i == 0))}
        for i, g in enumerate(gifts)
    ]


def _transform_contacts(contacts: list, is_first_batch: bool = False) -> List[dict]:
    """Transform contacts to upsert rows for all related tables."""
    rows = []

    for i, contact in enumerate(contacts):
        contact_id_str = str(contact.get("id", ""))
        debug = is_first_batch and i == 0

        # Main contact
        rows.append({"table": "contacts", "data": format_contact(contact, debug=debug)})

        # Address
        address = contact.get("address")
        if address and address.get("id"):
            rows.append(
                {"table": "addresses", "data": format_address(address, contact_id_str)}
            )

        # Individuals and contact methods
        for individual in contact.get("contactIndividuals", []):
            individual_id = str(individual.get("id", ""))
            rows.append(
                {
                    "table": "individuals",
                    "data": format_individual(individual, contact_id_str),
                }
            )

            for method in individual.get("contactMethods", []):
                rows.append(
                    {
                        "table": "contact_methods",
                        "data": format_contact_method(
                            method, contact_id_str, individual_id
                        ),
                    }
                )

    return rows


def sync_entity(
    entity_type: str,
    configuration: dict,
    state: dict,
    query_fn: Callable[..., dict],
    extract_id: Callable[[Any], Optional[int]],
    transform_fn: Callable[[list, bool], List[dict]],
    modified_since: Optional[str] = None,
    modified_until: Optional[str] = None,
) -> Generator[Any, None, dict]:
    """Generic sync loop for any entity type."""
    cursor = IDCursor.from_state(state, entity_type)

    if cursor.last_id is not None:
        log.info(
            f"Resuming {entity_type} sync: id_cursor={cursor.last_id}, total={cursor.total_synced}"
        )
    else:
        log.info(f"Starting fresh {entity_type} sync")

    batch_buffer: List[dict] = []
    is_first_batch = cursor.total_synced == 0
    reached_end = False

    while not reached_end:
        records, max_id, reached_end = fetch_batch_parallel(
            query_fn=query_fn,
            configuration=configuration,
            id_cursor=cursor.last_id,
            extract_id=extract_id,
            modified_since=modified_since,
            modified_until=modified_until,
        )

        if not records:
            break

        # Separate error records from valid records
        valid_records = [r for r in records if not isinstance(r, ErrorRecord)]
        error_records = [r for r in records if isinstance(r, ErrorRecord)]

        # Log error records to the errors table
        for err in error_records:
            log.info(f"Logging failed query to errors table: {err.url}")
            yield op.upsert(table="errors", data=err.to_dict())

        # Transform and buffer valid records
        rows = transform_fn(valid_records, is_first_batch) if valid_records else []
        batch_buffer.extend(rows)
        is_first_batch = False

        if max_id is not None:
            cursor.advance(max_id, len(valid_records))

        log.info(
            f"{entity_type}: total={cursor.total_synced}, buffer={len(batch_buffer)}, errors={len(error_records)}"
        )

        if len(batch_buffer) >= BATCH_SIZE:
            log.info(f"Checkpointing {entity_type} at id_cursor={cursor.last_id}")
            for item in batch_buffer:
                yield op.upsert(table=item["table"], data=item["data"])
            batch_buffer = []
            state.update(cursor.to_state())
            yield op.checkpoint(state=state)

    # Final batch
    if batch_buffer:
        log.info(f"Final batch of {len(batch_buffer)} {entity_type} rows")
        for item in batch_buffer:
            yield op.upsert(table=item["table"], data=item["data"])

    log.info(f"{entity_type.title()} sync complete: {cursor.total_synced} records")
    state[f"{entity_type}_complete"] = True
    cursor.clear_from_state(state)
    return state


def sync_gifts(
    configuration: dict,
    state: dict,
    modified_since: Optional[str] = None,
    modified_until: Optional[str] = None,
) -> Generator[Any, None, dict]:
    """Sync all gifts with parallel fetching and ID-based cursor."""
    return sync_entity(
        entity_type="gifts",
        configuration=configuration,
        state=state,
        query_fn=query_gifts,
        extract_id=_extract_gift_id,
        transform_fn=_transform_gifts,
        modified_since=modified_since,
        modified_until=modified_until,
    )


def sync_contacts(
    configuration: dict,
    state: dict,
    modified_since: Optional[str] = None,
    modified_until: Optional[str] = None,
) -> Generator[Any, None, dict]:
    """Sync all contacts and related entities with parallel fetching and ID-based cursor."""
    return sync_entity(
        entity_type="contacts",
        configuration=configuration,
        state=state,
        query_fn=query_contacts,
        extract_id=_extract_contact_id,
        transform_fn=_transform_contacts,
        modified_since=modified_since,
        modified_until=modified_until,
    )
