from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from fundraiseup_api import get_session, get_supporters, get_donations, get_events
from schema import schema as fru_schema


# ============================================================================
# CONFIGURATION - Single source of truth
# ============================================================================


class SyncConfig:
    """Centralized configuration for sync behavior"""

    def __init__(self, configuration: dict):
        # Checkpointing
        self.checkpoint_every_records = int(
            configuration.get("CHECKPOINT_EVERY_RECORDS", 1000)
        )

        # Debug/testing
        self.page_limit = int(configuration.get("PAGE_LIMIT", 0) or 0)

        # API config
        self.event_types = configuration.get("EVENT_TYPES", None)

    @property
    def is_debug_mode(self) -> bool:
        return self.page_limit > 0


class SyncState:
    """Clean interface to state management"""

    def __init__(self, state: dict):
        self._state = state
        self.records_since_checkpoint = int(
            state.get("records_since_checkpoint", 0) or 0
        )

    def get_cursor(self, entity: str) -> str:
        """Get the last synced ID for an entity (supporters, donations, events)"""
        return self._state.get(f"{entity}_last_id")

    def set_cursor(self, entity: str, record_id: str):
        """Update cursor for an entity"""
        self._state[f"{entity}_last_id"] = record_id

    def to_checkpoint(self) -> dict:
        """Convert current state to checkpoint dict"""
        return {
            "supporters_last_id": self._state.get("supporters_last_id"),
            "donations_last_id": self._state.get("donations_last_id"),
            "events_last_id": self._state.get("events_last_id"),
            "records_since_checkpoint": 0,  # Reset after checkpoint
        }


# ============================================================================
# DATA EXTRACTION - Transform API responses to table rows
# ============================================================================


def _extract_supporter_row(s: dict) -> dict:
    account = s.get("account") or {}
    return {
        "id": s.get("id"),
        "created_at": s.get("created_at"),
        "livemode": s.get("livemode"),
        "email": s.get("email"),
        "first_name": s.get("first_name"),
        "last_name": s.get("last_name"),
        "phone": s.get("phone"),
        "title": s.get("title"),
        "language": s.get("language"),
        "account_id": account.get("id"),
        "account_code": account.get("code"),
        "account_name": account.get("name"),
        "address": (s.get("address") and str(s.get("address"))) or None,
        "employer": (s.get("employer") and str(s.get("employer"))) or None,
    }


def _extract_donation_row(d: dict) -> dict:
    account = d.get("account") or {}
    campaign = d.get("campaign") or {}
    designation = d.get("designation") or {}
    supporter = d.get("supporter") or {}
    recurring_plan = d.get("recurring_plan") or {}
    return {
        "id": d.get("id"),
        "created_at": d.get("created_at"),
        "livemode": d.get("livemode"),
        "status": d.get("status"),
        "amount": d.get("amount"),
        "amount_in_default_currency": d.get("amount_in_default_currency"),
        "currency": d.get("currency"),
        "installment": d.get("installment"),
        "succeeded_at": d.get("succeeded_at"),
        "failed_at": d.get("failed_at"),
        "refunded_at": d.get("refunded_at"),
        "source": d.get("source"),
        "supporter_id": supporter.get("id"),
        "campaign_id": campaign.get("id"),
        "designation_id": designation.get("id"),
        "recurring_plan_id": recurring_plan.get("id"),
        "account_id": account.get("id"),
        "supporter": (supporter and str(supporter)) or None,
        "campaign": (campaign and str(campaign)) or None,
        "designation": (designation and str(designation)) or None,
        "element": (d.get("element") and str(d.get("element"))) or None,
        "platform_fee": (d.get("platform_fee") and str(d.get("platform_fee"))) or None,
        "processing_fee": (d.get("processing_fee") and str(d.get("processing_fee")))
        or None,
        "payout": (d.get("payout") and str(d.get("payout"))) or None,
        "payment": (d.get("payment") and str(d.get("payment"))) or None,
        "device": (d.get("device") and str(d.get("device"))) or None,
        "utm": (d.get("utm") and str(d.get("utm"))) or None,
        "fundraiser": (d.get("fundraiser") and str(d.get("fundraiser"))) or None,
        "tribute": (d.get("tribute") and str(d.get("tribute"))) or None,
        "custom_fields": (d.get("custom_fields") and str(d.get("custom_fields")))
        or None,
        "questions": (d.get("questions") and str(d.get("questions"))) or None,
        "consent": (d.get("consent") and str(d.get("consent"))) or None,
        "url": d.get("url"),
        "on_behalf_of": d.get("on_behalf_of"),
        "receipt_id": d.get("receipt_id"),
        "anonymous": d.get("anonymous"),
    }


def _extract_event_row(e: dict) -> dict:
    account = e.get("account") or {}
    return {
        "id": e.get("id"),
        "created_at": e.get("created_at"),
        "type": e.get("type"),
        "livemode": e.get("livemode"),
        "donation": e.get("donation"),
        "recurring_plan": e.get("recurring_plan"),
        "supporter": e.get("supporter"),
        "account_id": account.get("id"),
        "account_code": account.get("code"),
        "account_name": account.get("name"),
    }


# ============================================================================
# CHECKPOINTING - Emit checkpoints at configurable intervals
# ============================================================================


class CheckpointManager:
    """Manages checkpoint emission logic"""

    def __init__(self, config: SyncConfig, state: SyncState):
        self.config = config
        self.state = state
        self.records_since_last_checkpoint = state.records_since_checkpoint

    def record_upserted(self, entity: str, record_id: str):
        """Track that a record was upserted"""
        self.state.set_cursor(entity, record_id)
        self.records_since_last_checkpoint += 1

    def should_checkpoint(self) -> bool:
        """Check if we should emit a checkpoint"""
        if self.config.checkpoint_every_records <= 0:
            return False
        return (
            self.records_since_last_checkpoint >= self.config.checkpoint_every_records
        )

    def emit_checkpoint(self):
        """Yield a checkpoint operation and reset counter"""
        log.info(f"Emitting checkpoint at {self.records_since_last_checkpoint} records")
        self.records_since_last_checkpoint = 0
        return op.checkpoint(self.state.to_checkpoint())

    def emit_final_checkpoint(self):
        """Emit final checkpoint with latest cursors"""
        log.info("Emitting final checkpoint")
        return op.checkpoint(self.state.to_checkpoint())


# ============================================================================
# SYNC LOGIC - Fetch and yield records for each entity
# ============================================================================


def sync_entity(
    entity_name: str,
    fetch_page_fn,
    transform_fn,
    checkpoint_mgr: CheckpointManager,
    page_limit: int = 0,
):
    """
    Sync a single entity (supporters, donations, or events).

    Starting from cursor (last synced ID), fetch pages until exhausted.
    Emit checkpoints every N records as configured.
    """
    cursor = checkpoint_mgr.state.get_cursor(entity_name)
    pages_fetched = 0
    total_records = 0

    log.info(f"[{entity_name}] Starting sync from cursor: {cursor}")

    while True:
        # Check page limit (debug mode)
        if page_limit > 0 and pages_fetched >= page_limit:
            log.info(f"[{entity_name}] Reached page limit: {page_limit}")
            break

        # Fetch next page
        items, has_more = fetch_page_fn(starting_after=cursor, limit=100)

        if not items:
            log.info(f"[{entity_name}] No more records")
            break

        # Process each record
        for item in items:
            record_id = item.get("id")

            # Upsert record
            yield op.upsert(table=entity_name, data=transform_fn(item))

            # Track for checkpointing
            checkpoint_mgr.record_upserted(entity_name, record_id)

            # Emit checkpoint if needed
            if checkpoint_mgr.should_checkpoint():
                yield checkpoint_mgr.emit_checkpoint()

        # Update cursor and counters
        cursor = items[-1].get("id")
        pages_fetched += 1
        total_records += len(items)

        log.info(
            f"[{entity_name}] Page {pages_fetched}: {len(items)} records (total: {total_records})"
        )

        # Check if more pages available
        if not has_more:
            log.info(f"[{entity_name}] API reports no more pages")
            break

    log.info(
        f"[{entity_name}] Sync complete: {total_records} records in {pages_fetched} pages"
    )


# ============================================================================
# MAIN UPDATE FUNCTION - Entry point for Fivetran connector
# ============================================================================


def update(configuration: dict, state: dict):
    """
    Fivetran update function - syncs all entities.

    Flow:
    1. Get secrets from configuration
    2. Get cursors from state
    3. Sync each entity (supporters, donations, events) until exhausted
    4. Emit checkpoints every N records (configurable)
    5. Yield final checkpoint with latest cursors for next sync
    """
    # 1. Get secrets from config
    session = get_session(configuration)
    config = SyncConfig(configuration)

    # 2. Get cursors from state
    sync_state = SyncState(state)

    # 3. Create checkpoint manager
    checkpoint_mgr = CheckpointManager(config, sync_state)

    # Debug mode info
    if config.is_debug_mode:
        log.info(f"Running in DEBUG mode: max {config.page_limit} pages per entity")

    # 4. Sync each entity until we run out of data

    # Supporters
    def fetch_supporters(starting_after=None, limit=100):
        return get_supporters(session, limit=limit, starting_after=starting_after)

    yield from sync_entity(
        entity_name="supporters",
        fetch_page_fn=fetch_supporters,
        transform_fn=_extract_supporter_row,
        checkpoint_mgr=checkpoint_mgr,
        page_limit=config.page_limit,
    )

    # Donations
    def fetch_donations(starting_after=None, limit=100):
        return get_donations(session, limit=limit, starting_after=starting_after)

    yield from sync_entity(
        entity_name="donations",
        fetch_page_fn=fetch_donations,
        transform_fn=_extract_donation_row,
        checkpoint_mgr=checkpoint_mgr,
        page_limit=config.page_limit,
    )

    # Events
    def fetch_events(starting_after=None, limit=100):
        return get_events(
            session,
            limit=limit,
            starting_after=starting_after,
            event_types=config.event_types,
        )

    yield from sync_entity(
        entity_name="events",
        fetch_page_fn=fetch_events,
        transform_fn=_extract_event_row,
        checkpoint_mgr=checkpoint_mgr,
        page_limit=config.page_limit,
    )

    # 5. Emit final checkpoint with latest cursors
    yield checkpoint_mgr.emit_final_checkpoint()


# ============================================================================
# SCHEMA FUNCTION
# ============================================================================


def schema(configuration: dict):
    return fru_schema(configuration)


# ============================================================================
# CONNECTOR INITIALIZATION
# ============================================================================

connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
