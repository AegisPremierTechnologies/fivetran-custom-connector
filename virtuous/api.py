"""Virtuous API client for Fivetran connector.

Simple API layer - all error handling/retries are done at the fetch layer
via adaptive query sizing.
"""

from typing import Optional

import requests as rq
from fivetran_connector_sdk import Logging as log


BASE_URL = "https://api.virtuoussoftware.com"
REQUEST_TIMEOUT = 600  # 10 minute timeout


def get_headers(configuration: dict) -> dict:
    """Build request headers with Bearer token authentication."""
    return {
        "Authorization": f"Bearer {configuration['bearer_token']}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def query_gifts(
    configuration: dict,
    skip: int = 0,
    take: int = 1000,
    modified_since: Optional[str] = None,
    modified_until: Optional[str] = None,
    id_cursor: Optional[int] = None,
) -> dict:
    """Query gifts from Virtuous API.

    Args:
        configuration: Connector configuration with bearer_token
        skip: Number of records to skip (pagination within ID range)
        take: Number of records to return (max 1000)
        modified_since: Date string (YYYY-MM-DD) for incremental sync
        modified_until: Date string (YYYY-MM-DD) for sync end
        id_cursor: Gift ID to filter gifts with id > value
    """
    headers = get_headers(configuration)
    params = {"skip": skip, "take": take}

    # Sort by Gift Id for stable cursor pagination
    payload = {"sortBy": "Gift Id", "descending": False}

    # Build filter conditions
    conditions = []
    if id_cursor is not None:
        conditions.append(
            {
                "parameter": "Gift Id",
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

    log.info(f"Querying gifts: skip={skip}, take={take}, id_cursor={id_cursor}")

    response = rq.post(
        f"{BASE_URL}/api/Gift/Query/FullGift",
        headers=headers,
        params=params,
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def query_contacts(
    configuration: dict,
    skip: int = 0,
    take: int = 1000,
    modified_since: Optional[str] = None,
    modified_until: Optional[str] = None,
    id_cursor: Optional[int] = None,
) -> dict:
    """Query contacts from Virtuous API.

    Args:
        configuration: Connector configuration with bearer_token
        skip: Number of records to skip (pagination within ID range)
        take: Number of records to return (max 1000)
        modified_since: Date string (YYYY-MM-DD) for incremental sync
        modified_until: Date string (YYYY-MM-DD) for sync end
        id_cursor: Contact ID to filter contacts with id > value
    """
    headers = get_headers(configuration)
    params = {"skip": skip, "take": take}

    # Sort by Contact Id for stable cursor pagination
    payload = {"sortBy": "Contact Id", "descending": False}

    # Build filter conditions
    conditions = []
    if id_cursor is not None:
        conditions.append(
            {
                "parameter": "Contact Id",
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

    log.info(f"Querying contacts: skip={skip}, take={take}, id_cursor={id_cursor}")

    response = rq.post(
        f"{BASE_URL}/api/Contact/Query/FullContact",
        headers=headers,
        params=params,
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()
