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


def list_databases(client: MongoClient) -> list[dict]:
    """List all databases the authenticated user can see.

    Returns:
        List of database info dicts with name, sizeOnDisk, empty.
    """
    db_list = list(client.list_databases())
    for db_info in db_list:
        log.info(f"  Database: {db_info.get('name')}  size: {db_info.get('sizeOnDisk')}  empty: {db_info.get('empty')}")
    log.info(f"Total databases found: {len(db_list)}")
    return db_list
