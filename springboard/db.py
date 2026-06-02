"""MongoDB access layer for the Springboard data warehouse."""

from bson import ObjectId
from pymongo import ASCENDING, MongoClient
from fivetran_connector_sdk import Logging as log

DATABASE = "kqed-staging"


def get_client(configuration: dict) -> MongoClient:
    connection_string = configuration["connection_string"]
    log.info("Connecting to MongoDB...")
    return MongoClient(
        connection_string,
        serverSelectionTimeoutMS=10_000,
        maxPoolSize=1,
        maxIdleTimeMS=30_000,
    )


def list_sb_collections(client: MongoClient) -> list[str]:
    db = client[DATABASE]
    names = [n for n in db.list_collection_names() if n.startswith("sb_")]
    names.sort()
    log.info(f"Found {len(names)} sb_* collections: {names}")
    return names


def query_historical(client: MongoClient, collection_name: str, after_id: str | None, batch_size: int):
    db = client[DATABASE]
    if after_id is None:
        return db[collection_name].find().sort("_id", ASCENDING).limit(batch_size)
    return db[collection_name].find({"_id": {"$gt": ObjectId(after_id)}}).sort("_id", ASCENDING).limit(batch_size)


def query_incremental(client: MongoClient, collection_name: str, since_timestamp: str, batch_size: int):
    db = client[DATABASE]
    return db[collection_name].find(
        {"dw_updated_at.date": {"$gt": since_timestamp}}
    ).sort("dw_updated_at.date", ASCENDING).limit(batch_size)


def query_all(client: MongoClient, collection_name: str):
    db = client[DATABASE]
    return db[collection_name].find().sort("_id", ASCENDING)
