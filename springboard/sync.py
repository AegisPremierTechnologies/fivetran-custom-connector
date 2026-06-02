"""Sync orchestration for Springboard MongoDB data warehouse connector.

Iterates over sb_* collections, samples documents, logs document shapes,
and yields Fivetran upsert operations. Designed for iterative exploration
before building full typed schemas.
"""

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from db import get_client, get_database, list_collection_names, sample_collection
from models import extract_field_names, flatten_document


def explore_collections(configuration: dict, state: dict, sample_size: int = 5):
    """Sample every sb_* collection, log shapes, and yield upserts.

    Args:
        configuration: Connector config with connection_string + database.
        state: Connector state dict (reserved for incremental sync).
        sample_size: Max documents to pull per collection.

    Yields:
        op.upsert for each sampled document.
    """
    client = get_client(configuration)
    try:
        database = get_database(configuration, client)
        collections = list_collection_names(database)

        if not collections:
            log.warning("No sb_* collections found in database")
            return

        for collection_name in collections:
            log.info(f"--- Exploring {collection_name} ---")

            docs = sample_collection(database, collection_name, limit=sample_size)

            if not docs:
                log.info(f"  {collection_name}: empty collection, skipping")
                continue

            field_names = extract_field_names(docs)
            log.info(f"  {collection_name}: {len(docs)} docs, fields: {field_names}")

            for doc in docs:
                row = flatten_document(doc)
                yield op.upsert(table=collection_name, data=row)

            log.info(f"  {collection_name}: upserted {len(docs)} sample rows")

        log.info(f"Exploration complete: sampled {len(collections)} collections")

    finally:
        client.close()
        log.info("MongoDB connection closed")
