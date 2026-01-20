"""Shared fetching primitives for Virtuous connector.

Provides ID-based cursor tracking, adaptive page fetching (smaller take on
any error), and parallel batch fetching.
"""

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple, Union

from requests.exceptions import RequestException
from fivetran_connector_sdk import Logging as log


# Configuration
PAGE_SIZE = 1000
BATCH_SIZE = 8000
PARALLEL_REQUESTS = 8
TAKE_SIZES = [1000, 500, 250, 50, 10, 1]


@dataclass
class ErrorRecord:
    """Represents a failed query that should be logged to the errors table."""

    url: str
    payload: str
    error_message: str
    skip: int
    take: int

    def to_dict(self) -> dict:
        return {
            "url": self.url,
            "payload": self.payload,
            "error_message": self.error_message,
            "skip": self.skip,
            "take": self.take,
        }


@dataclass
class IDCursor:
    """Tracks cursor position for ID-based pagination."""

    entity_type: str
    last_id: Optional[int]
    total_synced: int

    def to_state(self) -> dict:
        return {
            f"{self.entity_type}_id_cursor": self.last_id,
            f"{self.entity_type}_total_synced": self.total_synced,
        }

    @staticmethod
    def from_state(state: dict, entity_type: str) -> "IDCursor":
        return IDCursor(
            entity_type=entity_type,
            last_id=state.get(f"{entity_type}_id_cursor"),
            total_synced=state.get(f"{entity_type}_total_synced", 0),
        )

    def advance(self, new_max_id: int, count: int) -> None:
        if self.last_id is None or new_max_id > self.last_id:
            self.last_id = new_max_id
        self.total_synced += count

    def clear_from_state(self, state: dict) -> None:
        state.pop(f"{self.entity_type}_id_cursor", None)
        state.pop(f"{self.entity_type}_total_synced", None)


def _extract_list(response: dict) -> list:
    """Extract record list from API response."""
    if isinstance(response, dict):
        return response.get("list", response) or []
    return response or []


def _build_error_record(
    query_fn: Callable[..., dict],
    skip: int,
    take: int,
    error: Exception,
    **query_kwargs: Any,
) -> ErrorRecord:
    """Build an ErrorRecord from query context for logging to errors table."""
    # Infer URL from function name
    fn_name = query_fn.__name__
    if "gift" in fn_name.lower():
        url = f"https://api.virtuoussoftware.com/api/Gift/Query/FullGift?skip={skip}&take={take}"
    elif "contact" in fn_name.lower():
        url = f"https://api.virtuoussoftware.com/api/Contact/Query/FullContact?skip={skip}&take={take}"
    else:
        url = f"Unknown endpoint?skip={skip}&take={take}"

    # Reconstruct payload from kwargs
    id_cursor = query_kwargs.get("id_cursor")
    modified_since = query_kwargs.get("modified_since")
    modified_until = query_kwargs.get("modified_until")

    # Determine sort field based on entity type
    if "gift" in fn_name.lower():
        sort_by = "Gift Id"
        id_param = "Gift Id"
    else:
        sort_by = "Contact Id"
        id_param = "Contact Id"

    payload = {"sortBy": sort_by, "descending": False}
    conditions = []
    if id_cursor is not None:
        conditions.append(
            {
                "parameter": id_param,
                "operator": "GreaterThan",
                "value": str(id_cursor),
            }
        )
    if modified_since:
        conditions.append(
            {
                "parameter": "Last Modified Date",
                "operator": "OnOrAfter",
                "value": modified_since,
            }
        )
    if modified_until:
        conditions.append(
            {
                "parameter": "Last Modified Date",
                "operator": "OnOrBefore",
                "value": modified_until,
            }
        )
    if conditions:
        payload["groups"] = [{"conditions": conditions}]

    return ErrorRecord(
        url=url,
        payload=json.dumps(payload),
        error_message=str(error)[:500],  # Truncate to avoid huge error messages
        skip=skip,
        take=take,
    )


def fetch_page_adaptive(
    query_fn: Callable[..., dict],
    skip: int,
    take: int,
    **query_kwargs: Any,
) -> List[Union[dict, ErrorRecord]]:
    """Fetch a page with adaptive resizing on ANY error.

    On failure, recursively splits into smaller sub-queries.
    When take=1 fails, returns an ErrorRecord instead of raising.
    """
    try:
        response = query_fn(skip=skip, take=take, **query_kwargs)
        return _extract_list(response)

    except RequestException as e:
        # Find next smaller take size
        try:
            current_idx = TAKE_SIZES.index(take)
            next_idx = current_idx + 1
        except ValueError:
            next_idx = len(TAKE_SIZES)  # Force exhaustion

        if next_idx >= len(TAKE_SIZES):
            # Base case: take=1 still failed - log error instead of crashing
            log.warning(
                f"Query failed at take=1, skip={skip}. Logging to errors table. Error: {e}"
            )
            # Build URL and payload for error logging
            error_record = _build_error_record(
                query_fn=query_fn,
                skip=skip,
                take=take,
                error=e,
                **query_kwargs,
            )
            return [error_record]

        smaller_take = TAKE_SIZES[next_idx]
        num_sub_calls = take // smaller_take

        log.warning(
            f"Error at skip={skip}, take={take}. "
            f"Splitting into {num_sub_calls} calls of take={smaller_take}. Error: {e}"
        )

        # Recursively fetch sub-pages
        all_records: List[Any] = []
        for i in range(num_sub_calls):
            sub_skip = skip + (i * smaller_take)
            sub_records = fetch_page_adaptive(
                query_fn=query_fn,
                skip=sub_skip,
                take=smaller_take,
                **query_kwargs,
            )
            all_records.extend(sub_records)
            # Check if we got an error record back (don't break, might have more valid records)
            if sub_records and not isinstance(sub_records[-1], ErrorRecord):
                if len(sub_records) < smaller_take:
                    break

        return all_records


def fetch_batch_parallel(
    query_fn: Callable[..., dict],
    configuration: dict,
    id_cursor: Optional[int],
    extract_id: Callable[[Any], Optional[int]],
    modified_since: Optional[str] = None,
    modified_until: Optional[str] = None,
) -> Tuple[List[Any], Optional[int], bool]:
    """Fetch multiple pages concurrently with adaptive sizing per slot.

    Returns: (all_records, max_id_seen, reached_end)
    """
    log.info(f"Parallel fetch: id_cursor={id_cursor}")

    with ThreadPoolExecutor(max_workers=PARALLEL_REQUESTS) as executor:
        futures = {}
        for i in range(PARALLEL_REQUESTS):
            page_skip = i * PAGE_SIZE
            future = executor.submit(
                fetch_page_adaptive,
                query_fn=query_fn,
                skip=page_skip,
                take=PAGE_SIZE,
                configuration=configuration,
                id_cursor=id_cursor,
                modified_since=modified_since,
                modified_until=modified_until,
            )
            futures[future] = page_skip

        results: List[Tuple[int, list]] = []
        for future in as_completed(futures):
            page_skip = futures[future]
            records = future.result()  # Let exceptions propagate
            results.append((page_skip, records))

    results.sort(key=lambda x: x[0])

    all_records: List[Any] = []
    max_id: Optional[int] = None
    reached_end = False

    for page_skip, records in results:
        if not records:
            reached_end = True
            break

        all_records.extend(records)

        for record in records:
            # Skip error records when tracking max ID
            if isinstance(record, ErrorRecord):
                continue
            record_id = extract_id(record)
            if record_id is not None and (max_id is None or record_id > max_id):
                max_id = record_id

        if len(records) < PAGE_SIZE:
            reached_end = True
            break

    log.info(f"Fetched {len(all_records)} records, max_id={max_id}")
    return all_records, max_id, reached_end
