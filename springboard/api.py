"""Springboard (Jackson River) API client.

Thin HTTP layer returning raw JSON. No Fivetran operations, no retry logic.
Error handling and retries belong in sync.py.

API docs: https://springboardapiv2.docs.apiary.io/
Auth: api_key query parameter on every request.
"""

import requests as rq
from fivetran_connector_sdk import Logging as log


def get_base_url(configuration: dict) -> str:
    """Get the Springboard instance base URL from configuration."""
    return configuration["base_url"].rstrip("/")


def get_api_key(configuration: dict) -> str:
    """Get the API key from configuration."""
    return configuration["api_key"]


def list_forms(
    configuration: dict,
    node_type: str = "donation_form",
) -> list[dict]:
    """List all webforms of a given content type.

    Args:
        configuration: Connector config with base_url and api_key.
        node_type: Springboard node type to filter by.
                   Options: donation_form, webform, petition, etc.

    Returns:
        List of form summary dicts, each with: nid, type, title, internal_name.
    """
    base_url = get_base_url(configuration)
    url = f"{base_url}/springboard-api/springboard-forms"

    params = {
        "api_key": get_api_key(configuration),
        "node_type": node_type,
    }

    log.info(f"Listing forms: node_type={node_type}")

    response = rq.get(url, params=params, timeout=60)

    if response.status_code != 200:
        log.warning(f"Response status: {response.status_code}")
        log.warning(f"Response body: {response.text[:500]}")

    response.raise_for_status()
    return response.json()


def get_form_detail(
    configuration: dict,
    form_id: str,
) -> dict:
    """Retrieve detailed information about a single form.

    Args:
        configuration: Connector config with base_url and api_key.
        form_id: Numeric node ID of the form.

    Returns:
        Full form detail dict with fields, body, token, etc.
    """
    base_url = get_base_url(configuration)
    url = f"{base_url}/springboard-api/springboard-forms/{form_id}"

    params = {
        "api_key": get_api_key(configuration),
    }

    log.info(f"Fetching form detail: form_id={form_id}")

    response = rq.get(url, params=params, timeout=60)

    if response.status_code != 200:
        log.warning(f"Response status: {response.status_code}")
        log.warning(f"Response body: {response.text[:500]}")

    response.raise_for_status()
    return response.json()
