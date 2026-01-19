"""Virtuous API client for Fivetran connector with retry and rate limit handling."""

import random
import time
from typing import Optional, Tuple

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

# Rate limit configuration
RATE_LIMIT_THRESHOLD = 10  # Proactively pause when remaining requests drop below this


def get_headers(configuration: dict) -> dict:
    """Build request headers with Bearer token authentication."""
    return {
        "Authorization": f"Bearer {configuration['bearer_token']}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _parse_rate_limit_headers(
    response: rq.Response,
) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """Parse rate limit headers from response.

    Args:
        response: HTTP response object

    Returns:
        Tuple of (limit, remaining, reset_timestamp)
        Any value may be None if header is missing
    """
    headers = response.headers

    limit = headers.get("X-RateLimit-Limit")
    remaining = headers.get("X-RateLimit-Remaining")
    reset_ts = headers.get("X-RateLimit-Reset")

    return (
        int(limit) if limit else None,
        int(remaining) if remaining else None,
        int(reset_ts) if reset_ts else None,
    )


def _get_rate_limit_backoff(reset_timestamp: Optional[int]) -> float:
    """Calculate how long to wait until rate limit resets.

    Args:
        reset_timestamp: Unix timestamp when rate limit resets

    Returns:
        Seconds to wait (minimum 1 second, maximum MAX_BACKOFF_SECONDS)
    """
    if reset_timestamp is None:
        return INITIAL_BACKOFF_SECONDS

    now = time.time()
    wait_time = reset_timestamp - now

    # Add a small buffer
    wait_time += 1

    # Clamp to reasonable bounds
    return max(1, min(wait_time, MAX_BACKOFF_SECONDS))


def _check_rate_limit(response: rq.Response, context: str = "") -> None:
    """Check rate limit headers, log status, and proactively sleep if running low.

    Args:
        response: HTTP response to check
        context: Context string for logging
    """
    limit, remaining, reset_ts = _parse_rate_limit_headers(response)

    # Always log rate limit status for visibility
    if remaining is not None and limit is not None:
        log.info(f"Rate limit status: {remaining}/{limit} remaining ({context})")

    if remaining is not None and remaining < RATE_LIMIT_THRESHOLD:
        wait_time = _get_rate_limit_backoff(reset_ts)
        log.warning(
            f"Rate limit running low for {context}. "
            f"Remaining: {remaining}/{limit}. "
            f"Proactively sleeping for {wait_time:.1f}s until reset..."
        )
        time.sleep(wait_time)


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


def _calculate_backoff(attempt: int, reset_timestamp: Optional[int] = None) -> float:
    """Calculate backoff time with exponential increase and jitter.

    If a rate limit reset timestamp is provided, uses that instead of
    exponential backoff for more precise timing.

    Args:
        attempt: The current retry attempt (0-indexed)
        reset_timestamp: Optional Unix timestamp when rate limit resets

    Returns:
        Backoff time in seconds
    """
    # If we have a reset timestamp (from rate limit), use it
    if reset_timestamp is not None:
        return _get_rate_limit_backoff(reset_timestamp)

    # Otherwise use exponential backoff: 2, 4, 8, 16, 32, ...
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
    """Make an HTTP request with exponential backoff retry logic and rate limit handling.

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
                timeout=600,  # 10 minute timeout
            )

            # Check if we need to retry
            if _is_retryable_error(response.status_code):
                # Get rate limit info for logging
                limit, remaining, reset_ts = _parse_rate_limit_headers(response)
                rate_info = (
                    f"Rate limit: {remaining}/{limit}"
                    if remaining is not None
                    else "Rate limit: unknown"
                )

                backoff = _calculate_backoff(
                    attempt, reset_ts if response.status_code == 429 else None
                )

                if attempt < MAX_RETRIES:
                    log.warning(
                        f"Retryable error {response.status_code} for {context}. "
                        f"{rate_info}. "
                        f"Attempt {attempt + 1}/{MAX_RETRIES + 1}. "
                        f"Retrying in {backoff:.1f}s..."
                    )
                    time.sleep(backoff)
                    continue
                else:
                    log.severe(
                        f"Max retries ({MAX_RETRIES}) exhausted for {context}. "
                        f"Last status: {response.status_code}. {rate_info}"
                    )
                    response.raise_for_status()

            # Success - check rate limit headers and proactively pause if running low
            _check_rate_limit(response, context)

            response.raise_for_status()
            return response

        except RequestException as e:
            last_exception = e

            # Check if this is a retryable HTTP error
            if isinstance(e, HTTPError) and e.response is not None:
                if _is_retryable_error(e.response.status_code):
                    if attempt < MAX_RETRIES:
                        # Get rate limit info for logging
                        limit, remaining, reset_ts = _parse_rate_limit_headers(
                            e.response
                        )
                        rate_info = (
                            f"Rate limit: {remaining}/{limit}"
                            if remaining is not None
                            else "Rate limit: unknown"
                        )

                        backoff = _calculate_backoff(
                            attempt, reset_ts if e.response.status_code == 429 else None
                        )
                        log.warning(
                            f"Retryable HTTPError for {context}. "
                            f"{rate_info}. "
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
                        f"Rate limit: {remaining}/{limit}"
                        if remaining is not None
                        else "Rate limit: unknown"
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
    gift_date_since: Optional[str] = None,
) -> dict:
    """Query gifts from Virtuous API with retry logic.

    Args:
        configuration: Connector configuration with bearer_token
        skip: Number of records to skip (pagination)
        take: Number of records to return (max 1000)
        modified_since: Date string (YYYY-MM-DD) for incremental sync start
        modified_until: Date string (YYYY-MM-DD) for incremental sync end (debug mode)
        gift_date_since: Date string (YYYY-MM-DD) to filter gifts with giftDate >= value
                         Used for date-based cursor pagination to avoid large skip values.

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

    # Gift Date filter for cursor-based pagination
    if gift_date_since:
        conditions.append(
            {
                "parameter": "Gift Date",
                "operator": "OnOrAfter",
                "value": gift_date_since,
            }
        )

    # Last Modified Date filters for incremental sync
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
    created_date_since: Optional[str] = None,
) -> dict:
    """Query full contacts from Virtuous API with retry logic.

    Args:
        configuration: Connector configuration with bearer_token
        skip: Number of records to skip (pagination)
        take: Number of records to return (max 1000)
        modified_since: Date string (YYYY-MM-DD) for incremental sync start
        modified_until: Date string (YYYY-MM-DD) for incremental sync end (debug mode)
        created_date_since: Date string (YYYY-MM-DD) to filter contacts with createDateTimeUtc >= value
                            Used for date-based cursor pagination to avoid large skip values.

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

    # Create DateTime filter for cursor-based pagination
    if created_date_since:
        conditions.append(
            {
                "parameter": "Create DateTime UTC",
                "operator": "OnOrAfter",
                "value": created_date_since,
            }
        )

    # Last Modified Date filters for incremental sync
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
        f"Querying contacts: skip={skip}, take={take}, createdDateSince={created_date_since}, modifiedSince={modified_since}"
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
