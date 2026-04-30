"""Sync orchestration for Springboard (Heartland Retail) connector.

Handles pagination, state management, and yielding Fivetran upsert operations.
Retry/backoff logic will be added here as complexity grows.
"""

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api import query_tickets
from models import format_ticket

PAGE_SIZE = 20


def sync_tickets(configuration: dict, state: dict):
    """Fetch all sales tickets via paginated API calls and yield upsert ops.

    Iterates through pages until all results are consumed.
    Each ticket is transformed via format_ticket before upserting.

    Args:
        configuration: Connector config with subdomain + api_token.
        state: Connector state dict (unused in hello world, reserved for
               incremental sync cursors in future).

    Yields:
        op.upsert for each ticket row.
    """
    page = 1
    total_synced = 0

    while True:
        response = query_tickets(
            configuration=configuration,
            page=page,
            per_page=PAGE_SIZE,
        )

        total = response.get("total", 0)
        total_pages = response.get("pages", 1)
        results = response.get("results", [])

        if not results:
            log.info(f"No results on page {page}, stopping.")
            break

        for raw_ticket in results:
            row = format_ticket(raw_ticket)
            yield op.upsert(table="tickets", data=row)
            total_synced += 1

        log.info(
            f"Page {page}/{total_pages}: synced {len(results)} tickets "
            f"({total_synced} total, {total} in API)"
        )

        if page >= total_pages:
            break

        page += 1

    log.info(f"Finished syncing tickets: {total_synced} rows")
