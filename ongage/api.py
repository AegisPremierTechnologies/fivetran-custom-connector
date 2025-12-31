"""OnGage API client for Fivetran connector."""

import time
from datetime import datetime
from typing import Optional

import requests as rq

from fivetran_connector_sdk import Logging as log


def get_headers(configuration: dict) -> dict:
    """Build request headers with username/password/account_code authentication."""
    return {
        "x_username": configuration["x_username"],
        "x_password": configuration["x_password"],
        "x_account_code": configuration["x_account_code"],
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def get_base_url(list_id: str = "") -> str:
    """Build the base API URL with optional list_id."""
    if list_id:
        return f"https://api.ongage.net/{list_id}/api"
    return "https://api.ongage.net/api"


def get_all_lists(configuration: dict) -> list[dict]:
    """Fetch all available lists from the OnGage API."""
    headers = get_headers(configuration)
    base_url = get_base_url()

    response = rq.get(f"{base_url}/lists", headers=headers)
    response.raise_for_status()

    data = response.json()
    lists = data.get("payload", [])
    log.info(f"Found {len(lists)} lists")
    return lists


def create_contact_search(
    configuration: dict,
    list_id: str,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
) -> str:
    """Create a contact search with optional date range and return the search ID."""
    base_url = get_base_url(list_id)
    headers = get_headers(configuration)

    # Base criteria: email is not empty (to get all contacts)
    criteria = [
        {
            "type": "email",
            "field_name": "email",
            "operator": "notempty",
            "operand": [""],
            "case_sensitive": 0,
            "condition": "and",
        }
    ]

    # Add date filters
    if start_time is not None:
        criteria.append(
            {
                "type": "date_absolute",
                "field_name": "ocx_created_date",
                "operator": ">=",
                "operand": [start_time],
                "case_sensitive": 0,
                "condition": "and",
            }
        )
    if end_time is not None:
        criteria.append(
            {
                "type": "date_absolute",
                "field_name": "ocx_created_date",
                "operator": "<",
                "operand": [end_time],
                "case_sensitive": 0,
                "condition": "and",
            }
        )

    payload = {
        "title": f"Fivetran Sync {datetime.utcnow().isoformat()}",
        "include_behavior": False,
        "filters": {
            "type": "Active",
            "criteria": criteria,
            "user_type": "all",
        },
        "combined_as_and": True,
    }

    log.info(f"Creating contact search for list {list_id}")
    response = rq.post(f"{base_url}/contact_search", headers=headers, json=payload)
    response.raise_for_status()

    data = response.json()
    search_id = data["payload"]["id"]
    log.info(f"Created contact search with ID: {search_id}")
    return search_id


class SearchResult:
    """Result of waiting for a contact search."""

    SUCCESS = "success"
    TIMEOUT = "timeout"
    FAILED = "failed"


def wait_for_search_completion(
    configuration: dict, list_id: str, search_id: str, max_wait: int = 300
) -> str:
    """Poll the contact search status until completed or timeout.

    Returns: SearchResult.SUCCESS, SearchResult.TIMEOUT, or SearchResult.FAILED
    """
    base_url = get_base_url(list_id)
    headers = get_headers(configuration)

    start_time = time.time()
    while time.time() - start_time < max_wait:
        response = rq.get(f"{base_url}/contact_search/{search_id}", headers=headers)
        response.raise_for_status()

        data = response.json()
        status = data["payload"].get("status")
        desc = data["payload"].get("desc", "")
        log.fine(f"Contact search status: {status} ({desc})")

        # Status codes: 1 = Pending, 2 = Completed, 3 = Failed
        if status == 2 or str(status).lower() == "completed":
            return SearchResult.SUCCESS
        elif status == 3 or str(status).lower() == "failed":
            log.warning(f"Contact search failed: {data}")
            return SearchResult.FAILED

        time.sleep(5)  # Poll every 5 seconds

    log.warning("Contact search timed out")
    return SearchResult.TIMEOUT


def export_contacts_csv(configuration: dict, list_id: str, search_id: str) -> str:
    """Download the contact search export CSV content."""
    base_url = get_base_url(list_id)
    headers = get_headers(configuration)

    response = rq.get(f"{base_url}/contact_search/{search_id}/export", headers=headers)
    response.raise_for_status()

    return response.text
