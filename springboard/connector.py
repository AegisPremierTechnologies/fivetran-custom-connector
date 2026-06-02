"""Springboard MongoDB Fivetran Custom Connector.

Syncs all sb_* collections from the Springboard data warehouse.
Supports checkpointed historical sync and incremental sync via dw_updated_at.
"""

from datetime import datetime, timezone

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from db import get_client, list_sb_collections
from schemas import get_schemas
from sync import (
    sync_collection_full_replace,
    sync_collection_historical,
    sync_collection_incremental,
)

FULL_REPLACE_COLLECTIONS = {"sb_sync_failure_counts"}


def schema(_configuration: dict):
    return get_schemas()


def update(configuration: dict, state: dict):
    last_sync_time = state.get("last_sync_time")
    is_historical = last_sync_time is None

    if is_historical:
        log.info("Starting historical sync")
    else:
        log.info(f"Starting incremental sync (since {last_sync_time})")

    client = get_client(configuration)
    try:
        collections = list_sb_collections(client)

        for name in collections:
            if name in FULL_REPLACE_COLLECTIONS:
                yield from sync_collection_full_replace(client, name, state)
                continue

            if is_historical:
                yield from sync_collection_historical(client, name, state)
            else:
                yield from sync_collection_incremental(
                    client, name, last_sync_time, state,
                )

        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        final_state = {"last_sync_time": now}
        yield op.checkpoint(state=final_state)
        log.info(f"Sync complete. Next sync from {now}")

    finally:
        client.close()
        log.info("MongoDB connection closed")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
