"""Sync orchestration for Springboard (Jackson River) connector.

Handles fetching forms, transforming rows, and yielding Fivetran upsert ops.
Retry/backoff logic will be added here as complexity grows.
"""

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api import get_form_detail, list_forms
from models import format_donation_form_detail


def sync_donation_forms(configuration: dict, state: dict, limit: int = 0):
    """Fetch donation forms and yield upsert ops with full detail.

    Lists all donation_form nodes, then fetches detail for each.
    An optional limit keeps hello-world runs small.

    Args:
        configuration: Connector config with base_url + api_key.
        state: Connector state dict (reserved for incremental sync).
        limit: Max forms to sync. 0 means all.

    Yields:
        op.upsert for each donation form row.
    """
    forms = list_forms(configuration, node_type="donation_form")
    log.info(f"Found {len(forms)} donation forms")

    if limit > 0:
        forms = forms[:limit]
        log.info(f"Limiting to first {limit} forms for hello world")

    for i, form_summary in enumerate(forms, 1):
        nid = form_summary.get("nid")
        detail = get_form_detail(configuration, form_id=nid)

        if isinstance(detail, list) and len(detail) > 0:
            detail = detail[0]

        row = format_donation_form_detail(detail)
        yield op.upsert(table="donation_forms", data=row)
        log.info(f"Synced form {i}/{len(forms)}: nid={nid} title={form_summary.get('title')}")

    log.info(f"Finished syncing {len(forms)} donation forms")
