"""iDonate API client for Fivetran connector.

Simple API layer - returns raw JSON responses. Error handling and retries
are handled at the sync layer.
"""

from typing import Optional

import requests as rq
from fivetran_connector_sdk import Logging as log


def get_headers(configuration: dict) -> dict:
    """Build request headers with API key authentication."""
    return {
        "apiKey": configuration["api_key"],
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def get_base_url(configuration: dict) -> str:
    """Get the base URL from configuration, default to standard endpoint."""
    return configuration.get(
        "base_url", "https://api.idonate.com/data/20220413"
    ).rstrip("/")


def query_transactions(
    configuration: dict,
    organization_id: str,
    start_date: str,
    end_date: str,
    page: int = 1,
    per_page: int = 100,
    include_children: bool = False,
) -> dict:
    """Query transactions from iDonate API.

    Args:
        configuration: Connector configuration with api_key and base_url
        organization_id: Organization ID to query
        start_date: ISO8601 start date (required)
        end_date: ISO8601 end date (required)
        page: Page number (1-indexed)
        per_page: Results per page (1-100)
        include_children: If true, include child organizations

    Returns:
        Raw API response dict with structure:
        {
            "result": {
                "items": [...],
                "count": int,
                "total_count": int,
                "page": int,
                "more": bool
            },
            "status": int
        }
    """
    headers = get_headers(configuration)
    base_url = get_base_url(configuration)
    url = f"{base_url}/organization/{organization_id}/transactions"

    params = {
        "page": page,
        "per_page": min(per_page, 100),  # Cap at 100
        "start_date": start_date,
        "end_date": end_date,
    }

    if include_children:
        params["include_children"] = "true"

    log.info(
        f"Querying transactions for org {organization_id}, "
        f"page {page}, date range {start_date} to {end_date}"
    )

    response = rq.get(url, headers=headers, params=params, timeout=60)

    # Log status for debugging
    if response.status_code != 200:
        log.warning(f"Response status: {response.status_code}")

    response.raise_for_status()
    return response.json()
