"""Springboard (Jackson River) Fivetran Custom Connector.

Syncs donation form data from the Springboard nonprofit fundraising platform.
API docs: https://springboardapiv2.docs.apiary.io/
"""

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from sync import sync_donation_forms

HELLO_WORLD_LIMIT = 5


def schema(_configuration: dict):
    """Define the Fivetran warehouse schema for Springboard data."""
    return [
        {
            "table": "donation_forms",
            "primary_key": ["nid"],
            "columns": {
                "nid": "STRING",
                "type": "STRING",
                "title": "STRING",
                "internal_name": "STRING",
                "body": "STRING",
                "fields": "STRING",
                "token": "STRING",
            },
        },
    ]


def update(configuration: dict, state: dict):
    """Main sync driver. Delegates to sync layer, then checkpoints."""
    log.info("Springboard Connector: starting sync")

    limit = int(configuration.get("limit", HELLO_WORLD_LIMIT))
    yield from sync_donation_forms(configuration, state, limit=limit)

    yield op.checkpoint(state=state)
    log.info("Springboard Connector: sync complete")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
