---
trigger: always_on
---

# AI Rules for Fivetran Custom Connectors

This document outlines the standards, patterns, and workflows for creating and maintaining Fivetran custom connectors in this repository. Future AI agents should follow these rules to ensure consistency and correctness.

## 1. Directory Structure

Each connector typically resides in its own top-level directory. The directory name should be the "slug" of the service (e.g., `ongage`, `everyaction`).

**Standard File Layout:**

```
root-connectors/
  <service_name>/
    connector.py        # Entry point (schema & update functions)
    configuration.json  # (Gitignored) Local secrets/config
    requirements.txt    # Python dependencies
    api.py              # API client (requests, auth, retries)
    models.py           # Data transformations & schema mapping
    sync.py             # Sync logic (batching, state management)
    __init__.py
```

## 2. Core File Responsibilities

### `connector.py`

- **Purpose**: Main entry point for the Fivetran SDK.
- **Responsibilities**:
  - JSON Schema definition (`def schema(configuration)`).
  - Main sync loop (`def update(configuration, state)`).
  - Initialization of `Connector` object.
- **Key Pattern**: Keep this file high-level. Delegate complex logic to `sync.py`.

### `api.py`

- **Purpose**: Handle all raw HTTP interactions with the 3rd party API.
- **Responsibilities**:
  - Authentication headers/tokens.
  - Base URL management.
  - Error handling (raise_for_status).
  - Pagination/Search helpers.
- **Rules**: return raw JSON or response objects. Do not format for Fivetran here.

### `models.py`

- **Purpose**: Transform raw API data into Fivetran-compliant dictionary rows.
- **Responsibilities**:
  - Date parsing (handle timezones, ISO formats).
  - Type casting (string, int, boolean).
  - Flattening nested structures if needed.
- **Rules**: Functions here should be pure input/output transformations.

### `sync.py`

- **Purpose**: Logic for HOW to fetch data.
- **Responsibilities**:
  - Handling `state` (cursors, checkpoints).
  - Implementation of incremental sync vs initial sync.
  - Batching strategies (e.g., time-based, id-based).
  - Adaptive retries for large datasets.

## 3. Deployment Workflow

We use GitHub Actions to deploy connectors to Fivetran's infrastructure.

**Workflow Location**: `.github/workflows/<service_name>_deploy.yml`

**Standard Steps:**

1.  **Checkout & Setup**: Checkout repo, install Python 3.12+.
2.  **Dependencies**: Install `fivetran-connector-sdk` and `requirements.txt`.
3.  **Config Generation**: Create `configuration.json` dynamically using GitHub Secrets.
    - Secret Naming Convention: `SFL_<SERVICE>_<KEY>` (e.g., `SFL_ONGAGE_PASSWORD`).
4.  **Deploy Command**:
    ```bash
    fivetran deploy --api-key $FIVETRAN_API_KEY \
                    --destination $FIVETRAN_DESTINATION \
                    --configuration ./configuration.json \
                    --connection <connection_name> \
                    --force
    ```

**Example YAML Template:**

```yaml
name: Deploy <Service> Connector
on:
  push:
    branches: [main]
    paths: ["<service_name>/**", ".github/workflows/<service_name>_deploy.yml"]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with: { python-version: "3.12" }
      - run: pip install fivetran-connector-sdk
      - name: Create configuration.json
        working-directory: "./<service_name>"
        env:
          API_KEY: ${{ secrets.SFL_<SERVICE>_API_KEY }}
        run: |
          echo "{\"api_key\": \"$API_KEY\"}" > configuration.json
      - name: Deploy
        working-directory: "./<service_name>"
        env:
          FIVETRAN_API_KEY: ${{ secrets.FIVETRAN_API_KEY }}
          FIVETRAN_DESTINATION: ${{ secrets.<SERVICE>_FIVETRAN_DESTINATION }}
        run: |
          fivetran deploy --api-key $FIVETRAN_API_KEY \
                          --destination $FIVETRAN_DESTINATION \
                          --configuration ./configuration.json \
                          --connection <connection_name> \
                          --force
```

## 4. Coding Standards

### State Management (Cursors)

- Always implement checkpointing using `yield op.checkpoint(state=...)`.
- For time-based syncs, store the `last_sync_time` (usually Unix timestamp or ISO string).
- For large initial syncs, use **Batching**:
  - Break requests into smaller chunks (e.g., by month).
  - Checkpoint after each successful chunk.
  - This creates "resumable" syncs if the connector crashes or times out.

### Error Handling

- Use `fivetran_connector_sdk.Logging` (`log`) for all logs.
- Fail fast on configuration errors (invalid auth).
- Implement adaptive backoff/retry for rate limits or timeout errors in `sync.py` or `api.py`.

### Schema Definition

- Define primary keys clearly.
- Use Fivetran standard types: `STRING`, `INT`, `BOOLEAN`, `UTC_DATETIME`, `FLOAT`.

## 5. Local Development

- **Configuration**: Create a local `configuration.json` with real credentials (ensure it is gitignored).
- **Debug Run**: `python connector.py` (ensure `if __name__ == "__main__": connector.debug()` is present).
