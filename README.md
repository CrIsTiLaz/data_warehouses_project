# Python Starter Project

Starter template for a clean Python workflow in Cursor, tailored for TS/JS users.

## Quick start (PowerShell)

1. Activate virtual environment:

```powershell
.\.venv\Scripts\Activate.ps1
```

2. Install dependencies:

```powershell
python -m pip install fastapi uvicorn pymongo python-dotenv requests pytest ruff httpx
```

3. Run tests:

```powershell
pytest
```

4. Run lint:

```powershell
ruff check .
```

## Ingestion service (Alpha Vantage + metals.dev → MongoDB)

From the project root, use a virtual environment and install runtime dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install requests python-dotenv pymongo
```

Create a `.env` file next to `ingestion_service.py` (do not commit it):

- `MONGO_URI` or `MONGODB_ATLAS_URI` — Atlas connection string
- `ALPHA_VANTAGE_API_KEY` — your key (free tier has strict daily limits)
- `METAL_API_KEY` — [metals.dev](https://metals.dev/) key (required only when ingesting `XAG` / `XPT`)
- `MONGO_DB_NAME` or `DB_NAME` — target database name (default `acme_financial_dw`)

Run full default ingest (TSLA + BTC):

```bash
python ingestion_service.py
```

Ingest one symbol to save API calls, or force-refresh daily series after schema changes:

```bash
python ingestion_service.py --symbol TSLA
python ingestion_service.py --symbol BTC --force
```

Use `-v` for DEBUG-level logs. Precious metals (`XAG`, `XPT`) use `dataSourceId` `metals_dev_v1` and backfill the latest 100 daily prices from metals.dev timeseries. The metals.dev API allows a maximum 30-day timeseries window per request, so ingestion automatically chunks the 100-day range into multiple calls. Verify in Atlas: collections `assets`, `time_series`, and canonical rows in `data_sources` for `alpha_vantage_api_v1` and `metals_dev_v1` when those pipelines run.

## Data quality and governance

Ingestion now writes a final audit document to `ingestion_runs` for each execution. Audit fields:

- `runId` — UUID for the ingestion execution
- `startedAt`, `finishedAt` — UTC ISO timestamps
- `status` — `success`, `failed`, or `partial`
- `symbolsRequested`, `symbolsSucceeded`, `symbolsFailed`
- `recordsInserted`
- `invalidRowsSkipped`
- `duplicateRowsSkipped`
- `validationErrors`
- `errorSummary`
- `dataSourcesTouched`

Before writing to `time_series`, rows are validated for non-empty `assetId`, non-empty `dataSourceId`, UTC ISO `timestamp`, and numeric price fields (`open`, `high`, `low`, `close`) when present. Invalid rows are skipped and recorded in the ingestion run diagnostics.

Setup and ingestion ensure the `ingestion_runs` collection exists and create an idempotent unique index on `time_series` for (`assetId`, `dataSourceId`, `timestamp`). Duplicate key errors are treated as skipped duplicate rows, not fatal ingestion failures.

## REST API (UC2)

The FastAPI backend is implemented in `src/app/main.py` and reads the same MongoDB
configuration as ingestion:

- `MONGO_URI` or `MONGODB_ATLAS_URI` — Atlas connection string
- `MONGO_DB_NAME` or `DB_NAME` — target database name (default `acme_financial_dw`)

Install API dependencies in the project virtual environment:

```bash
source .venv/bin/activate
python -m pip install fastapi uvicorn pymongo python-dotenv
```

Run the API from the project root:

```bash
uvicorn app.main:app --app-dir src --reload
```

Open the generated docs at `http://127.0.0.1:8000/docs`.

Endpoints:

- `GET /assets` → `{ "assetIds": ["TSLA", "BTC"] }`
- `GET /assets/{asset_id}` → latest active asset metadata
- `GET /data-sources` → `{ "dataSourceIds": ["alpha_vantage_api_v1"] }`
- `GET /data-sources/{source_id}` → full data source details
- `GET /time-series?assetId=TSLA&dataSourceId=alpha_vantage_api_v1` → matching rows sorted by `timestamp`
- `GET /quality/freshness` → latest timestamp and lag per (`assetId`, `dataSourceId`)
- `GET /quality/summary` → row count, duplicate risk, and latest ingestion run status
- `POST /assistant/query` → grounded assistant response using read-only DWH tools

Sample requests:

```bash
curl "http://127.0.0.1:8000/assets"
curl "http://127.0.0.1:8000/assets/TSLA"
curl "http://127.0.0.1:8000/data-sources"
curl "http://127.0.0.1:8000/data-sources/alpha_vantage_api_v1"
curl "http://127.0.0.1:8000/time-series?assetId=TSLA&dataSourceId=alpha_vantage_api_v1"
curl "http://127.0.0.1:8000/time-series?assetId=TSLA&dataSourceId=alpha_vantage_api_v1&startDate=2026-05-01&endDate=2026-05-10"
curl "http://127.0.0.1:8000/quality/freshness"
curl "http://127.0.0.1:8000/quality/freshness?thresholdHours=48"
curl "http://127.0.0.1:8000/quality/summary"
```

Freshness statuses:

- `fresh` — latest timestamp is within the threshold, default 24 hours
- `stale` — latest timestamp is older than the threshold
- `missing` — no valid UTC latest timestamp is available for that pair

## LLM assistant via MCP (UC4)

The assistant endpoint is implemented as a grounded, read-only orchestration layer over the DWH tools. It never writes to MongoDB and should not invent numeric values; numeric and temporal claims are formed from tool results and returned with grounding metadata.

Assistant environment variables:

- `ASSISTANT_MCP_ENABLED` — set to `true` to enable assistant tool execution
- `ASSISTANT_MCP_TIMEOUT_SECONDS` — timeout guard, default `10`
- `ASSISTANT_MODEL` — local MCP adapter label, default `grounded-dwh-assistant`
- `ASSISTANT_MCP_ENDPOINT` — optional endpoint/transport setting for future external MCP adapter use
- `LLM_API_KEY` — OpenRouter API key for LLM tool planning/final answer mode
- `LLM_MODEL` — OpenRouter model ID, for example `openai/gpt-4o-mini`
- `LLM_BASE_URL` — optional, default `https://openrouter.ai/api/v1`
- `LLM_TIMEOUT_SECONDS` — optional, default `15`

Example `.env` snippet:

```bash
ASSISTANT_MCP_ENABLED=true
LLM_API_KEY=your-openrouter-key
LLM_MODEL=openai/gpt-4o-mini
```

Behavior:

- If `ASSISTANT_MCP_ENABLED` is not `true`, `POST /assistant/query` returns HTTP 503 with a structured diagnostic.
- If MCP is enabled but `LLM_API_KEY` and `LLM_MODEL` are absent, the assistant uses deterministic fallback planning with the same grounded read-only tools.
- If only one of `LLM_API_KEY` or `LLM_MODEL` is set, the endpoint returns HTTP 503 with an actionable configuration message.
- If OpenRouter is configured but a timeout/network error occurs, the assistant falls back to deterministic planning instead of making unsupported claims.

Sample request:

```bash
curl -X POST "http://127.0.0.1:8000/assistant/query" \
  -H "Content-Type: application/json" \
  -d '{
    "question": "What is the latest close price for TSLA from alpha_vantage_api_v1?"
  }'
```

You can also pass explicit context to remove ambiguity:

```bash
curl -X POST "http://127.0.0.1:8000/assistant/query" \
  -H "Content-Type: application/json" \
  -d '{
    "question": "What is the latest close price?",
    "context": {
      "assetId": "TSLA",
      "dataSourceId": "alpha_vantage_api_v1"
    }
  }'
```

Response shape:

```json
{
  "answer": "Found 100 time-series rows for TSLA from alpha_vantage_api_v1. Latest timestamp is 2026-05-01T00:00:00Z. The latest close is 390.82.",
  "status": "grounded",
  "grounding": [
    {
      "toolName": "query_time_series",
      "arguments": {
        "asset_id": "TSLA",
        "data_source_id": "alpha_vantage_api_v1"
      },
      "provenance": {
        "endpoint": "GET /time-series",
        "collection": "time_series"
      },
      "resultSummary": {
        "assetId": "TSLA",
        "dataSourceId": "alpha_vantage_api_v1",
        "count": 100,
        "points": {
          "count": 100
        }
      }
    }
  ],
  "clarificationNeeded": null
}
```

Statuses:

- `grounded` — answer was formed from read-only tool results
- `insufficient_data` — the assistant needs an asset/source/date range or no matching data was found
- `error` — assistant execution failed or MCP is unavailable
