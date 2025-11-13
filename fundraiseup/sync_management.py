from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


class SyncConfig:
    """Centralized configuration for sync behavior."""

    def __init__(self, configuration: dict):
        self.checkpoint_every_records = int(
            configuration.get("CHECKPOINT_EVERY_RECORDS", 10000)
        )
        self.page_limit = int(configuration.get("PAGE_LIMIT", 0) or 0)

    @property
    def is_debug_mode(self) -> bool:
        return self.page_limit > 0


class SyncState:
    """Tracks cursors and checkpoint counters."""

    def __init__(self, state: dict):
        self._state = state
        self.records_since_checkpoint = int(
            state.get("records_since_checkpoint", 0) or 0
        )

    def get_cursor(self, entity: str):
        return self._state.get(f"{entity}_last_id")

    def set_cursor(self, entity: str, record_id: str):
        self._state[f"{entity}_last_id"] = record_id

    def to_checkpoint(self):
        return {
            "supporters_last_id": self._state.get("supporters_last_id"),
            "fundraisers_last_id": self._state.get("fundraisers_last_id"),
            "recurring_plans_last_id": self._state.get("recurring_plans_last_id"),
            "events_last_id": self._state.get("events_last_id"),
            "donations_last_id": self._state.get("donations_last_id"),
            "records_since_checkpoint": 0,
        }


class CheckpointManager:
    """Responsible for checkpoint emission logic."""

    def __init__(self, config: SyncConfig, state: SyncState):
        self.config = config
        self.state = state
        self.records_since_last_checkpoint = state.records_since_checkpoint

    def record_upserted(self, entity: str, record_id: str):
        self.state.set_cursor(entity, record_id)
        self.records_since_last_checkpoint += 1

    def should_checkpoint(self) -> bool:
        if self.config.checkpoint_every_records <= 0:
            return False
        return (
            self.records_since_last_checkpoint >= self.config.checkpoint_every_records
        )

    def emit_checkpoint(self):
        log.info(f"Emitting checkpoint at {self.records_since_last_checkpoint} records")
        self.records_since_last_checkpoint = 0
        return op.checkpoint(self.state.to_checkpoint())

    def emit_final_checkpoint(self):
        log.info("Emitting final checkpoint")
        return op.checkpoint(self.state.to_checkpoint())


def sync_basic(
    name,
    fetch_page_fn,
    transform_fn,
    checkpoint_mgr,
    page_limit,
    pre_hook=None,
):
    cursor = checkpoint_mgr.state.get_cursor(name)
    pages_fetched = 0
    total = 0

    log.info(f"[{name}] Starting sync from cursor {cursor}")

    while True:
        if page_limit > 0 and pages_fetched >= page_limit:
            log.info(f"[{name}] Page limit reached")
            break

        items, has_more = fetch_page_fn(starting_after=cursor, limit=100)
        if not items:
            break

        for item in items:
            record_id = item["id"]

            if pre_hook:
                for op_item in pre_hook(item):
                    yield op_item

            yield op.upsert(table=name, data=transform_fn(item))
            checkpoint_mgr.record_upserted(name, record_id)

            if checkpoint_mgr.should_checkpoint():
                yield checkpoint_mgr.emit_checkpoint()

            total += 1

        cursor = items[-1]["id"]
        pages_fetched += 1

        if not has_more:
            break

    log.info(f"[{name}] Sync complete: {total} records")
