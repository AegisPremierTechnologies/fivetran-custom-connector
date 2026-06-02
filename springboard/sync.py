"""Sync orchestration for Springboard MongoDB data warehouse connector.

Provides three sync strategies:
- Historical: walks a collection by _id, checkpoints every batch
- Incremental: queries by dw_updated_at.date > cursor, checkpoints every batch
- Full replace: queries all docs (for collections without dw_updated_at)
"""

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from db import query_historical, query_incremental, query_all
from models import format_document

BATCH_SIZE = 1000


def sync_collection_historical(client, collection_name, state, batch_size=BATCH_SIZE):
    cursor_key = f"{collection_name}_cursor"
    complete_key = f"{collection_name}_complete"
    after_id = state.get(cursor_key)

    if state.get(complete_key):
        log.info(f"{collection_name}: already complete, skipping")
        return

    log.info(f"{collection_name}: historical sync (cursor={after_id})")
    total = 0

    while True:
        docs = query_historical(client, collection_name, after_id, batch_size)
        batch_count = 0
        last_id = None

        for doc in docs:
            row = format_document(collection_name, doc)
            yield op.upsert(table=collection_name, data=row)
            last_id = str(doc["_id"])
            batch_count += 1

        if batch_count == 0:
            break

        total += batch_count
        after_id = last_id
        state[cursor_key] = after_id
        yield op.checkpoint(state=state)
        log.info(f"{collection_name}: checkpointed {total} docs (cursor={after_id})")

        if batch_count < batch_size:
            break

    state[complete_key] = True
    state.pop(cursor_key, None)
    yield op.checkpoint(state=state)
    log.info(f"{collection_name}: historical sync complete ({total} docs)")


def sync_collection_incremental(client, collection_name, since, state, batch_size=BATCH_SIZE):
    log.info(f"{collection_name}: incremental sync (since={since})")
    total = 0

    while True:
        docs = query_incremental(client, collection_name, since, batch_size)
        batch_count = 0
        last_timestamp = None

        for doc in docs:
            row = format_document(collection_name, doc)
            yield op.upsert(table=collection_name, data=row)
            dw_updated = doc.get("dw_updated_at", {})
            if isinstance(dw_updated, dict):
                last_timestamp = dw_updated.get("date", since)
            batch_count += 1

        if batch_count == 0:
            break

        total += batch_count
        if last_timestamp:
            since = last_timestamp
        yield op.checkpoint(state=state)
        log.info(f"{collection_name}: checkpointed {total} incremental docs")

        if batch_count < batch_size:
            break

    log.info(f"{collection_name}: incremental sync complete ({total} docs)")


def sync_collection_full_replace(client, collection_name):
    log.info(f"{collection_name}: full replace sync")
    docs = query_all(client, collection_name)
    total = 0

    for doc in docs:
        row = format_document(collection_name, doc)
        yield op.upsert(table=collection_name, data=row)
        total += 1

    log.info(f"{collection_name}: full replace complete ({total} docs)")
