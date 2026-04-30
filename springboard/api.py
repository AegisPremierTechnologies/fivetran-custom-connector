"""Heartland Retail (Springboard) API client.

Thin HTTP layer returning raw JSON. No Fivetran operations, no retry logic.
Error handling and retries belong in sync.py.
"""

import requests as rq
from fivetran_connector_sdk import Logging as log


def get_headers(configuration: dict) -> dict:
    """Build request headers with Bearer token authentication."""
    return {
        "Authorization": f"Bearer {configuration['api_token']}",
        "Content-Type": "application/json",
    }


def get_base_url(configuration: dict) -> str:
    """Build the account-specific API base URL from subdomain."""
    subdomain = configuration["subdomain"]
    return f"https://{subdomain}.retail.heartland.us/api"


def query_tickets(
    configuration: dict,
    page: int = 1,
    per_page: int = 20,
) -> dict:
    """Fetch a page of sales tickets.

    Args:
        configuration: Connector configuration with subdomain and api_token.
        page: Page number (1-indexed).
        per_page: Results per page.

    Returns:
        Raw API response: { "total": int, "pages": int, "results": [...] }
    """
    headers = get_headers(configuration)
    base_url = get_base_url(configuration)
    url = f"{base_url}/sales/tickets"

    params = {
        "page": page,
        "per_page": per_page,
    }

    log.info(f"Querying sales tickets: page {page}, per_page {per_page}")

    response = rq.get(url, headers=headers, params=params, timeout=60)

    if response.status_code != 200:
        log.warning(f"Response status: {response.status_code}")
        log.warning(f"Response body: {response.text[:500]}")

    response.raise_for_status()
    return response.json()
