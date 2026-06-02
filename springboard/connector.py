"""Springboard MongoDB Connector -- exploration pass.

Connects to kqed-staging, samples a few docs from each sb_* collection,
and logs them raw. No data lands in Fivetran yet -- this is just recon.
"""

import json
from datetime import datetime

from bson import ObjectId
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from db import get_client, list_sb_collections, sample_documents

PLACEHOLDER_TABLE = {
    "table": "placeholder",
    "primary_key": ["id"],
    "columns": {"id": "STRING"},
}


def _json_default(o):
    if isinstance(o, ObjectId):
        return str(o)
    if isinstance(o, datetime):
        return o.isoformat()
    if isinstance(o, bytes):
        return o.hex()
    return str(o)


def schema(_configuration: dict):
    return [PLACEHOLDER_TABLE]


def update(configuration: dict, state: dict):
    log.info("=== Springboard collection sampler ===")

    client = get_client(configuration)
    try:
        collections = list_sb_collections(client)

        for name in collections:
            docs = sample_documents(client, name, limit=1)
            log.info(f"--- {name} ({len(docs)} docs) ---")
            if docs:
                log.info(json.dumps(docs[0], default=_json_default, indent=2))

        yield op.upsert(table="placeholder", data={"id": "done"})
        yield op.checkpoint(state=state)
        log.info("=== Sampling complete ===")

    finally:
        client.close()
        log.info("MongoDB connection closed")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
