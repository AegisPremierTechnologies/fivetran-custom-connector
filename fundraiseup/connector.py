from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


from fundraiseup_api import get_session, get_supporters, get_donations
from schema import schema as fru_schema

from sync_management import SyncConfig, SyncState, CheckpointManager, sync_basic
from extractors import _extract_supporter_row, _extract_donation_row


def sync_supporters(session, checkpoint_mgr, page_limit):
    def fetch(starting_after=None, limit=100):
        return get_supporters(session, limit=limit, starting_after=starting_after)

    yield from sync_basic(
        name="supporters",
        fetch_page_fn=fetch,
        transform_fn=_extract_supporter_row,
        checkpoint_mgr=checkpoint_mgr,
        page_limit=page_limit,
    )


def sync_donations(session, checkpoint_mgr, page_limit):
    def fetch(starting_after=None, limit=100):
        return get_donations(session, limit=limit, starting_after=starting_after)

    def pre_supporter(donation):
        supporter = donation.get("supporter")
        if supporter:
            supporter_row = _extract_supporter_row(supporter)
            if supporter_row:
                yield op.upsert(
                    table="supporters",
                    data=supporter_row,
                )

    yield from sync_basic(
        name="donations",
        fetch_page_fn=fetch,
        transform_fn=_extract_donation_row,
        checkpoint_mgr=checkpoint_mgr,
        page_limit=page_limit,
        pre_hook=pre_supporter,
    )


def update(configuration: dict, state: dict):
    """Narrative-style orchestration of all sync steps."""

    session = get_session(configuration)
    config = SyncConfig(configuration)
    sync_state = SyncState(state)
    checkpoint_mgr = CheckpointManager(config, sync_state)

    if config.is_debug_mode:
        log.info(f"DEBUG MODE: page_limit = {config.page_limit}")

    # ---- 1. Sync supporters -----------------------------------------
    yield from sync_supporters(session, checkpoint_mgr, config.page_limit)

    # ---- 2. Sync donations (with embedded supporter upserts) --------
    yield from sync_donations(session, checkpoint_mgr, config.page_limit)

    # ---- 3. Final checkpoint ----------------------------------------
    yield checkpoint_mgr.emit_final_checkpoint()


# =====================================================================
# SCHEMA + CONNECTOR EXPORT
# =====================================================================


def schema(configuration: dict):
    return fru_schema(configuration)


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
