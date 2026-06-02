"""MongoDB access layer for the Springboard data warehouse."""

from pymongo import MongoClient
from fivetran_connector_sdk import Logging as log

DATABASE = "kqed-staging"


def get_client(configuration: dict) -> MongoClient:
    connection_string = configuration["connection_string"]
    log.info("Connecting to MongoDB...")
    return MongoClient(connection_string, serverSelectionTimeoutMS=10_000)


def list_sb_collections(client: MongoClient) -> list[str]:
    db = client[DATABASE]
    names = [n for n in db.list_collection_names() if n.startswith("sb_")]
    names.sort()
    log.info(f"Found {len(names)} sb_* collections: {names}")
    return names


def sample_documents(client: MongoClient, collection_name: str, limit: int = 3) -> list[dict]:
    db = client[DATABASE]
    return list(db[collection_name].find().limit(limit))
