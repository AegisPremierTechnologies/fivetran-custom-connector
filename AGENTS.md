# Fivetran Custom Connector Repository Guide

## Architecture Overview

This repository contains multiple Fivetran custom connectors built using the [Fivetran Connector SDK](https://fivetran.com/docs/connectors/connector-sdk/setup-guide). Each connector syncs data from external APIs (Virtuous CRM, OnGage, EveryAction, SA360) into a data warehouse.

**Repository structure**: Each connector lives in its own directory (`virtuous/`, `ongage/`, `everyaction/`, etc.) and follows this pattern:
- `connector.py` — Main entry point defining schema (`def schema(configuration)`) and sync logic (`def update(configuration, state)`)
- `api.py` — HTTP client: authentication, base URLs, error handling, pagination helpers (returns raw JSON; **no Fivetran formatting**)
- `sync.py` — Sync orchestration: state management, cursor tracking, batching strategies, adaptive retries
- `models.py` — Data transformations: date parsing, type casting, flattening nested structures (pure input/output)
- `requirements.txt` — Python dependencies
- `configuration.json` — Local secrets (gitignored for development)

## Key Patterns & Workflows

### 1. **Cursor-Based Pagination (Virtuous Model)**
Virtuous connector uses ID-based cursor tracking (`virtuous/fetch.py`) for stable, resumable pagination:

- `IDCursor` dataclass tracks `last_id` and `total_synced` in connector state
- API queries filter `id > cursor_id` for deterministic ordering
- On errors, query size shrinks adaptively (`TAKE_SIZES = [1000, 500, 250, 50, 10, 1]`)
- Parallel requests via `ThreadPoolExecutor` with `PARALLEL_REQUESTS = 8`
- Failed queries logged to an `errors` table with full context

**When to use**: When API supports ID-based filtering and you need resumable syncs that survive crashes.

### 2. **Adaptive Batch Retry (OnGage Model)**
OnGage connector handles time-based batching with adaptive sizing:

- Large lists (`> 200000` contacts) use batched sync with date ranges
- Initial batch size is `INITIAL_BATCH_MONTHS = 3`
- On timeout, batch shrinks to minimum `MIN_BATCH_SECONDS = 14 * 24 * 60 * 60` (2 weeks)
- Up to `MAX_RETRIES_PER_BATCH = 3` before giving up
- Auto-discovers all lists on first sync

**When to use**: When API has expensive operations (async searches) but supports time-based filtering.

### 3. **Schema Definition**
Every `connector.py` defines schema as a list of table dicts:
```python
def schema(configuration: dict):
    return [{
        "table": "table_name",
        "primary_key": ["id"],  # or composite keys like ["id", "list_id"]
        "columns": {
            "column_name": "STRING",  # or UTC_DATETIME, FLOAT, INT, BOOLEAN
        }
    }]
```
Use `UTC_DATETIME` for all date fields to ensure warehouse compatibility.

### 4. **Configuration & Authentication**
Each connector reads auth from Fivetran configuration dict:
- Bearer token: `configuration['bearer_token']` (Virtuous)
- Basic auth: `configuration['username']`, `configuration['password']` (EveryAction)
- Header-based: `configuration['x_username']`, `configuration['x_password']`, `configuration['x_account_code']` (OnGage)

Store credentials in `get_headers()` functions; never log them.

### 5. **State Management & Checkpointing**
Connector state is a dict yielded via `op.checkpoint(state=...)`:
- Always implement checkpointing using `yield op.checkpoint(state=state_dict)` after completing logical work units
- For time-based syncs: store `last_sync_time` (ISO string or Unix timestamp)
- For large initial syncs: use **batching** with intermediate checkpoints — if the connector crashes, it resumes from the last checkpoint
- Example: OnGage batches by month; each month that completes gets a checkpoint
- State is per-connector instance and persists across runs

### 6. **Upsert vs. Delete Operations**
Use `op.upsert()` for most syncs (idempotent):
```python
op.upsert({"table": "table_name", "data": row_dict})
```
Use `op.delete()` only when records are physically removed from the source and you want warehouse soft deletes.

## Development Workflow

### Setup
```bash
cd connector_name
python -m venv myenv
source myenv/bin/activate  # macOS/Linux
pip install -r requirements.txt
```

### Local Testing
```bash
# From connector directory with venv activated
python connector.py
# or
fivetran debug
```

### Creating configuration.json
For local testing, create `configuration.json` in the connector directory (add to `.gitignore`):
```json
{
  "bearer_token": "your-token-here",
  "username": "your-username",
  "password": "your-password"
}
```

### Deployment via GitHub Actions
Connectors deploy automatically on changes to `main`:

**Secret Naming Convention**: `SFL_<SERVICE>_<KEY>` (e.g., `SFL_ONGAGE_PASSWORD`, `SFL_VIRTUOUS_BEARER_TOKEN`)

**Deploy Command** (in workflow):
```bash
fivetran deploy --api-key $FIVETRAN_API_KEY \
                --destination $FIVETRAN_DESTINATION \
                --configuration ./configuration.json \
                --connection <connection_name> \
                --force
```

See `.github/workflows` for CI/CD YAML templates and examples.

## Coding Standards & Gotchas

### Separation of Concerns
- **api.py**: Returns raw JSON only. No Fivetran formatting, no retry logic here.
- **sync.py**: Implements adaptive retry, batching, state checkpointing, cursor management.
- **models.py**: Pure transformations (date parsing, type casting). No side effects.

### Common Gotchas
1. **Pagination ordering must be deterministic** — Use `sortBy` in queries so cursor pagination doesn't skip/duplicate records
2. **State is ephemeral in debug mode** — Local testing with `python connector.py` won't persist state; use Fivetran cloud for real state management
3. **API errors vs. retry logic** — Error handling & retries belong in `sync.py` or `fetch.py`; let `api.py` raise exceptions for connection issues
4. **Nested data in string columns** — Complex nested objects are serialized to JSON strings, not normalized to separate tables (unless expensive to do so)
5. **Column type mismatches** — All numeric IDs should be STRING; dates must use UTC_DATETIME format
6. **Credentials in logs** — Never log tokens, passwords, or auth headers; store in `get_headers()` functions only

## Testing & Debugging

- **Logging**: Use `fivetran_connector_sdk.Logging` (`log.info()`, `log.warning()`) — appears in Fivetran UI
- **Error Inspection**: Check `errors` table (if implemented) for failed batch details
- **Graceful Errors**: Use `op.upsert(..., on_error='continue')` to skip bad rows without halting sync
- **Batch Failures**: Virtuous stores failed queries in `ErrorRecord` with full context (URL, payload, error message)
- **Rate Limiting**: Implement exponential backoff (see EveryAction connector for example)

## Key Files to Understand

- [virtuous/connector.py](../virtuous/connector.py) — Full-featured cursor-based sync with parallel fetching
- [virtuous/fetch.py](../virtuous/fetch.py) — Reusable ID-cursor and adaptive retry primitives
- [ongage/sync.py](../ongage/sync.py) — Adaptive batch retry and state checkpointing for async APIs
- [README.md](../README.md) — Quick setup and deployment guide
