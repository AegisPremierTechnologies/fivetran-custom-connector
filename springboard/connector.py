"""Springboard (Heartland Retail) Fivetran Custom Connector.

Syncs sales ticket data from the Heartland Retail POS API.
"""

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from sync import sync_tickets


def schema(_configuration: dict):
    """Define the Fivetran warehouse schema for Springboard data."""
    return [
        {
            "table": "tickets",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "type": "STRING",
                "status": "STRING",
                "total": "FLOAT",
                "customer_id": "STRING",
                "source_location_id": "STRING",
                "station_id": "STRING",
                "parent_transaction_id": "STRING",
                "created_by_user_id": "STRING",
                "updated_by_user_id": "STRING",
                "coupon_id": "STRING",
                "completed_at": "UTC_DATETIME",
                "local_created_at": "UTC_DATETIME",
                "local_updated_at": "UTC_DATETIME",
            },
        },
    ]


def update(configuration: dict, state: dict):
    """Main sync driver. Delegates to sync layer, then checkpoints."""
    log.info("Springboard Connector: starting sync")

    yield from sync_tickets(configuration, state)

    yield op.checkpoint(state=state)
    log.info("Springboard Connector: sync complete")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
