import time
from typing import Dict, Generator, List, Optional, Tuple

import requests


DEFAULT_BASE_URL = "https://api.fundraiseup.com"


class FundraiseUpApiError(Exception):
    pass


def get_session(configuration: Dict) -> requests.Session:
    """
    Build an HTTP session with Bearer auth for Fundraise Up API.
    """
    api_key = configuration.get("FUNDRAISEUP_API_KEY")
    if not api_key:
        raise FundraiseUpApiError("Missing FUNDRAISEUP_API_KEY in configuration")

    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            # Explicitly opt into v1 paths present in the OpenAPI document
        }
    )
    return session


def _backoff_sleep(attempt: int) -> None:
    # Exponential backoff with jitter, capped to 10s
    base = min(2**attempt, 10)
    time.sleep(base + (0.1 * attempt))


def api_get(
    session: requests.Session,
    path: str,
    params: Optional[Dict] = None,
    base_url: str = DEFAULT_BASE_URL,
    max_retries: int = 5,
) -> Dict:
    """
    Perform a GET request with basic retry handling for 429/5xx.
    """
    url = f"{base_url}{path}"
    attempt = 0
    while True:
        resp = session.get(url, params=params or {}, timeout=60)
        if resp.status_code == 200:
            return resp.json()

        # Handle retryable statuses
        if resp.status_code in (429, 500, 502, 503, 504):
            attempt += 1
            if attempt > max_retries:
                raise FundraiseUpApiError(
                    f"GET {url} failed after retries: {resp.status_code} {resp.text}"
                )
            _backoff_sleep(attempt)
            continue

        # Hard failures
        raise FundraiseUpApiError(f"GET {url} failed: {resp.status_code} {resp.text}")


def list_entities(
    session: requests.Session,
    path: str,
    limit: int = 100,
    starting_after: Optional[str] = None,
    ending_before: Optional[str] = None,
    extra_params: Optional[Dict] = None,
) -> Tuple[List[Dict], bool]:
    """
    One-shot list call for list endpoints returning { data: [], has_more: bool }.
    """
    params = {"limit": limit}
    if starting_after:
        params["starting_after"] = starting_after
    if ending_before:
        params["ending_before"] = ending_before
    if extra_params:
        params.update(extra_params)
    payload = api_get(session, path, params=params)
    data = payload.get("data", [])
    has_more = bool(payload.get("has_more", False))
    return data, has_more


def get_supporters(
    session: requests.Session,
    limit: int = 100,
    starting_after: Optional[str] = None,
    ending_before: Optional[str] = None,
) -> Tuple[List[Dict], bool]:
    return list_entities(
        session,
        "/v1/supporters",
        limit=limit,
        starting_after=starting_after,
        ending_before=ending_before,
    )


def get_donations(
    session: requests.Session,
    limit: int = 100,
    starting_after: Optional[str] = None,
    ending_before: Optional[str] = None,
) -> Tuple[List[Dict], bool]:
    return list_entities(
        session,
        "/v1/donations",
        limit=limit,
        starting_after=starting_after,
        ending_before=ending_before,
    )


def get_events(
    session: requests.Session,
    limit: int = 100,
    starting_after: Optional[str] = None,
    ending_before: Optional[str] = None,
    event_types: Optional[List[str]] = None,
) -> Tuple[List[Dict], bool]:
    # The OpenAPI spec marks "types" as required, but the description suggests it can be omitted.
    # Provide a compact default set to satisfy validation and remain broadly useful.
    default_types = [
        "donation.created",
        "donation.success",
        "donation.failed",
        "donation.updated",
        "donation.refunded",
        "supporter.created",
        "supporter.updated",
        "recurring_plan.activated",
        "recurring_plan.failed",
        "recurring_plan.canceled",
    ]
    extra_params = {}
    if event_types is None:
        event_types = default_types
    # API expects "types" as array in query; many servers accept repeated key format
    # e.g., types=donation.created&types=donation.success
    # requests supports list values; it will serialize as repeated keys.
    extra_params["types"] = event_types

    return list_entities(
        session,
        "/v1/events",
        limit=limit,
        starting_after=starting_after,
        ending_before=ending_before,
        extra_params=extra_params,
    )
