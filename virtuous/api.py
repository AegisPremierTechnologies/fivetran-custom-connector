"""Virtuous API client for Fivetran connector with retry logic."""

import random
import time
from typing import Optional

import requests as rq
from requests.exceptions import HTTPError, RequestException

from fivetran_connector_sdk import Logging as log


BASE_URL = "https://api.virtuoussoftware.com"

# Retry configuration
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 2
MAX_BACKOFF_SECONDS = 60
BACKOFF_MULTIPLIER = 2
JITTER_RANGE = 0.5  # Add random jitter up to 50% of backoff


def get_headers(configuration: dict) -> dict:
    """Build request headers with Bearer token authentication."""
    return {
        "Authorization": f"Bearer {configuration['bearer_token']}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _is_retryable_error(status_code: int) -> bool:
    """Check if the HTTP status code is retryable.

    Retryable errors:
    - 429: Too Many Requests (rate limit)
    - 500: Internal Server Error
    - 502: Bad Gateway
    - 503: Service Unavailable
    - 504: Gateway Timeout
    """
    return status_code in (429, 500, 502, 503, 504)


def _calculate_backoff(attempt: int) -> float:
    """Calculate backoff time with exponential increase and jitter.

    Args:
        attempt: The current retry attempt (0-indexed)

    Returns:
        Backoff time in seconds
    """
    # Exponential backoff: 2, 4, 8, 16, 32, ...
    backoff = INITIAL_BACKOFF_SECONDS * (BACKOFF_MULTIPLIER**attempt)

    # Cap at max backoff
    backoff = min(backoff, MAX_BACKOFF_SECONDS)

    # Add jitter to prevent thundering herd
    jitter = random.uniform(0, JITTER_RANGE * backoff)

    return backoff + jitter


def request_with_retry(
    method: str,
    url: str,
    headers: dict,
    params: Optional[dict] = None,
    json: Optional[dict] = None,
    context: str = "",
) -> rq.Response:
    """Make an HTTP request with exponential backoff retry logic.

    Args:
        method: HTTP method ('GET', 'POST', etc.)
        url: Request URL
        headers: Request headers
        params: Query parameters
        json: JSON body
        context: Context string for logging (e.g., "gifts skip=1000")

    Returns:
        Response object

    Raises:
        HTTPError: If all retries are exhausted
        RequestException: For non-retryable errors
    """
    last_exception = None

    for attempt in range(MAX_RETRIES + 1):  # +1 for initial attempt
        try:
            response = rq.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json,
                timeout=120,  # 2 minute timeout
            )

            # Check if we need to retry
            if _is_retryable_error(response.status_code):
                backoff = _calculate_backoff(attempt)

                if attempt < MAX_RETRIES:
                    log.warning(
                        f"Retryable error {response.status_code} for {context}. "
                        f"Attempt {attempt + 1}/{MAX_RETRIES + 1}. "
                        f"Retrying in {backoff:.1f}s..."
                    )
                    time.sleep(backoff)
                    continue
                else:
                    log.severe(
                        f"Max retries ({MAX_RETRIES}) exhausted for {context}. "
                        f"Last status: {response.status_code}"
                    )
                    response.raise_for_status()

            # Success or non-retryable error
            response.raise_for_status()
            return response

        except RequestException as e:
            last_exception = e

            # Check if this is a retryable HTTP error
            if isinstance(e, HTTPError) and e.response is not None:
                if _is_retryable_error(e.response.status_code):
                    if attempt < MAX_RETRIES:
                        backoff = _calculate_backoff(attempt)
                        log.warning(
                            f"Retryable HTTPError for {context}. "
                            f"Attempt {attempt + 1}/{MAX_RETRIES + 1}. "
                            f"Retrying in {backoff:.1f}s... Error: {e}"
                        )
                        time.sleep(backoff)
                        continue

            # Connection errors, timeouts, etc. - also retry
            if not isinstance(e, HTTPError):
                if attempt < MAX_RETRIES:
                    backoff = _calculate_backoff(attempt)
                    log.warning(
                        f"Connection error for {context}. "
                        f"Attempt {attempt + 1}/{MAX_RETRIES + 1}. "
                        f"Retrying in {backoff:.1f}s... Error: {e}"
                    )
                    time.sleep(backoff)
                    continue

            # Non-retryable error or max retries exhausted
            raise

    # Should not reach here, but just in case
    if last_exception:
        raise last_exception
    raise RuntimeError(f"Unexpected retry loop exit for {context}")


def query_gifts(
    configuration: dict,
    skip: int = 0,
    take: int = 500,
    modified_since: Optional[str] = None,
    modified_until: Optional[str] = None,
) -> dict:
    """Query gifts from Virtuous API with retry logic.

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

    response = request_with_retry(
        method="POST",
        url=f"{BASE_URL}/api/Gift/Query/FullGift",
        headers=headers,
        params=params,
        json=payload,
        context=f"gifts skip={skip}",
    )

    return response.json()


def query_contacts(
    configuration: dict,
    skip: int = 0,
    take: int = 1000,
    modified_since: Optional[str] = None,
    modified_until: Optional[str] = None,
) -> dict:
    """Query full contacts from Virtuous API with retry logic.

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

    response = request_with_retry(
        method="POST",
        url=f"{BASE_URL}/api/Contact/Query/FullContact",
        headers=headers,
        params=params,
        json=payload,
        context=f"contacts skip={skip}",
    )

    return response.json()
