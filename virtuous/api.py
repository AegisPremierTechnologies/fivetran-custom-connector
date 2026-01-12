"""Virtuous API client for Fivetran connector."""

from typing import Optional

import requests as rq

from fivetran_connector_sdk import Logging as log


BASE_URL = "https://api.virtuoussoftware.com"


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
    take: int = 500,
    modified_since: Optional[str] = None,
    modified_until: Optional[str] = None,
) -> dict:
    """Query gifts from Virtuous API.

    Args:
        configuration: Connector configuration with bearer_token
        skip: Number of records to skip (pagination)
        take: Number of records to return (max 1000)
        modified_since: Date string (YYYY-MM-DD) for incremental sync start
        modified_until: Date string (YYYY-MM-DD) for incremental sync end (debug mode)

    Returns:
        API response with list of gifts
    """
    headers = get_headers(configuration)

    # Pagination via query params
    params = {
        "skip": skip,
        "take": take,
    }

    # Filter and sort criteria in body
    payload = {
        "sortBy": "Gift Date",
        "descending": False,
    }

    # Add date filter using proper API structure
    conditions = []
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

    log.info(
        f"Querying gifts: skip={skip}, take={take}, modifiedSince={modified_since}, modifiedUntil={modified_until}"
    )

    response = rq.post(
        f"{BASE_URL}/api/Gift/Query/FullGift",
        headers=headers,
        params=params,
        json=payload,
    )
    response.raise_for_status()

    return response.json()


def query_contacts(
    configuration: dict,
    skip: int = 0,
    take: int = 1000,
    modified_since: Optional[str] = None,
    modified_until: Optional[str] = None,
) -> dict:
    """Query full contacts from Virtuous API.

    Args:
        configuration: Connector configuration with bearer_token
        skip: Number of records to skip (pagination)
        take: Number of records to return (max 1000)
        modified_since: Date string (YYYY-MM-DD) for incremental sync start
        modified_until: Date string (YYYY-MM-DD) for incremental sync end (debug mode)

    Returns:
        API response with list of contacts
    """
    headers = get_headers(configuration)

    # Pagination via query params
    params = {
        "skip": skip,
        "take": take,
    }

    # Filter and sort criteria in body
    payload = {
        "sortBy": "Create DateTime UTC",
        "descending": False,
    }

    # Add date filter using proper API structure
    conditions = []
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

    log.info(
        f"Querying contacts: skip={skip}, take={take}, modifiedSince={modified_since}, modifiedUntil={modified_until}"
    )

    response = rq.post(
        f"{BASE_URL}/api/Contact/Query/FullContact",
        headers=headers,
        params=params,
        json=payload,
    )
    response.raise_for_status()

    return response.json()
