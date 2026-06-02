"""Springboard (Jackson River) Fivetran Custom Connector.

Connects to the Springboard MongoDB data warehouse and syncs
sb_* collections. First pass is exploration: sample a few documents
per collection with raw JSON output to discover document shapes.
"""

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from db import get_client, get_database, list_collection_names
from sync import explore_collections

DEFAULT_SAMPLE_SIZE = 5

EXPLORATION_SCHEMA = {
    "_id": "STRING",
    "raw_document": "STRING",
}


def schema(configuration: dict):
    """Discover sb_* collections and declare a table for each.

    Each table gets the same two-column exploration schema:
    _id (primary key) and raw_document (full JSON). Typed columns
    will replace raw_document once document shapes are known.
    """
    client = get_client(configuration)
    try:
        database = get_database(configuration, client)
        collections = list_collection_names(database)

        tables = []
        for name in collections:
            tables.append({
                "table": name,
                "primary_key": ["_id"],
                "columns": dict(EXPLORATION_SCHEMA),
            })

        log.info(f"Schema: declared {len(tables)} tables")
        return tables

    finally:
        client.close()


def update(configuration: dict, state: dict):
    """Main sync driver. Samples each collection, then checkpoints."""
    log.info("Springboard Connector: starting exploration sync")

    sample_size = int(configuration.get("sample_size", DEFAULT_SAMPLE_SIZE))
    yield from explore_collections(configuration, state, sample_size=sample_size)

    yield op.checkpoint(state=state)
    log.info("Springboard Connector: exploration sync complete")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
