"""MongoDB access layer for the Springboard data warehouse.

Thin wrapper around pymongo. Returns raw documents as dicts.
No Fivetran operations, no retry logic -- those belong in sync.py.
"""

from pymongo import MongoClient
from fivetran_connector_sdk import Logging as log


def get_client(configuration: dict) -> MongoClient:
    """Create a MongoClient from the connection string in configuration."""
    connection_string = configuration["connection_string"]
    log.info("Connecting to MongoDB...")
    return MongoClient(connection_string, serverSelectionTimeoutMS=10_000)


def get_database(configuration: dict, client: MongoClient):
    """Return the database handle for the configured database name."""
    db_name = configuration["database"]
    return client[db_name]


def list_collection_names(database, prefix: str = "sb_") -> list[str]:
    """Return sorted collection names matching the given prefix."""
    all_names = database.list_collection_names()
    matched = sorted(name for name in all_names if name.startswith(prefix))
    log.info(f"Discovered {len(matched)} collections with prefix '{prefix}': {matched}")
    return matched


def sample_collection(database, collection_name: str, limit: int = 5) -> list[dict]:
    """Fetch a small sample of documents from a collection.

    Args:
        database: pymongo Database handle.
        collection_name: Name of the collection to sample.
        limit: Maximum number of documents to return.

    Returns:
        List of document dicts.
    """
    cursor = database[collection_name].find().limit(limit)
    docs = list(cursor)
    log.info(f"Sampled {len(docs)} documents from {collection_name}")
    return docs
