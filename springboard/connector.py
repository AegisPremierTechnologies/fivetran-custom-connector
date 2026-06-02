"""Springboard (Jackson River) Fivetran Custom Connector.

First deploy: connect to MongoDB, list available databases, log and exit.
This discovers the database name we need for subsequent iterations.
"""

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from db import get_client, list_databases

DISCOVERY_TABLE = {
    "table": "discovery_log",
    "primary_key": ["name"],
    "columns": {
        "name": "STRING",
        "size_on_disk": "FLOAT",
        "empty": "BOOLEAN",
    },
}


def schema(_configuration: dict):
    """Static schema for database discovery output."""
    return [DISCOVERY_TABLE]


def update(configuration: dict, state: dict):
    """Connect to MongoDB, list databases, log results, and exit."""
    log.info("Springboard Connector: discovering databases")

    client = get_client(configuration)
    try:
        databases = list_databases(client)

        for db_info in databases:
            yield op.upsert(table="discovery_log", data={
                "name": db_info.get("name"),
                "size_on_disk": db_info.get("sizeOnDisk"),
                "empty": db_info.get("empty"),
            })

        yield op.checkpoint(state=state)
        log.info("Springboard Connector: database discovery complete")

    finally:
        client.close()


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
