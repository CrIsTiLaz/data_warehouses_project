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
- `GET /analytics/summary?assetId=TSLA&dataSourceId=alpha_vantage_api_v1` → min/max/average metrics
- `GET /analytics/forecast?assetId=TSLA&dataSourceId=alpha_vantage_api_v1` → deterministic trend forecast
- `GET /analytics/spark-shape?assetId=TSLA&dataSourceId=alpha_vantage_api_v1` → flattened analytics-friendly rows
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
curl "http://127.0.0.1:8000/analytics/summary?assetId=TSLA&dataSourceId=alpha_vantage_api_v1"
curl "http://127.0.0.1:8000/analytics/forecast?assetId=TSLA&dataSourceId=alpha_vantage_api_v1"
curl "http://127.0.0.1:8000/analytics/spark-shape?assetId=TSLA&dataSourceId=alpha_vantage_api_v1&startDate=2026-05-01&endDate=2026-05-10"
curl "http://127.0.0.1:8000/quality/freshness"
curl "http://127.0.0.1:8000/quality/freshness?thresholdHours=48"
curl "http://127.0.0.1:8000/quality/summary"
```

Freshness statuses:

- `fresh` — latest timestamp is within the threshold, default 24 hours
- `stale` — latest timestamp is older than the threshold
- `missing` — no valid UTC latest timestamp is available for that pair

## Analytics & data mining (UC3)

UC3 adds deterministic, read-only analytics on `time_series` rows:

- Summary metrics endpoint for min/max/average (`close` required, plus `open`/`high`/`low`/`volume` when available)
- Basic trend forecast endpoint from recent `close` values
- Spark-friendly flattened row endpoint for direct DataFrame ingestion

All analytics endpoints support:

- `assetId` and `dataSourceId` (required)
- `startDate` and `endDate` in `YYYY-MM-DD` (optional)

Validation behavior:

- Missing `assetId` or `dataSourceId` → HTTP 400
- Invalid date format → HTTP 400
- `startDate > endDate` → HTTP 400
- No matching rows → HTTP 404

Summary example response:

```json
{
  "assetId": "TSLA",
  "dataSourceId": "alpha_vantage_api_v1",
  "count": 105,
  "dateRange": {
    "start": "2026-01-01T00:00:00Z",
    "end": "2026-05-08T00:00:00Z"
  },
  "close": {
    "min": 221.86,
    "max": 428.35,
    "average": 312.45
  }
}
```

Forecast example response:

```json
{
  "assetId": "TSLA",
  "dataSourceId": "alpha_vantage_api_v1",
  "basis": "last_10_close_values",
  "latestTimestamp": "2026-05-08T00:00:00Z",
  "latestClose": 428.35,
  "averageDailyChange": 2.15,
  "forecast": {
    "nextPeriodClose": 430.5,
    "direction": "up"
  },
  "note": "Simple deterministic trend estimate from historical close values only. Not financial advice."
}
```

Spark-shape example response:

```json
{
  "assetId": "TSLA",
  "dataSourceId": "alpha_vantage_api_v1",
  "count": 105,
  "rows": [
    {
      "assetId": "TSLA",
      "dataSourceId": "alpha_vantage_api_v1",
      "timestamp": "2026-05-08T00:00:00Z",
      "open": 420.1,
      "high": 431.2,
      "low": 418.9,
      "close": 428.35,
      "volume": 1234567
    }
  ]
}
```

## Spark Analytics and ML Workflow

This project now includes explicit **Apache Spark** / **PySpark** workflows in addition to the REST analytics endpoints.

- **Spark aggregation**: grouped metrics using Spark SQL DataFrames
- **Spark MLlib**: **LinearRegression**-based next-close prediction
- **ML workflow**: data load -> feature engineering -> model train -> metrics -> next prediction

Implementation files:

- `src/spark_analytics.py` (main CLI workflow)
- `src/spark_common.py` (small reusable helpers)
- `spark_analytics.py` (root wrapper)
- `data/time_series_export.sample.jsonl` (small demo dataset)

### Install dependencies

```bash
source .venv/bin/activate
python -m pip install -e .
```

If needed, install Spark package directly:

```bash
python -m pip install pyspark
```

Spark runtime prerequisite:

- Java Runtime (JRE/JDK) must be installed and available on `PATH` (or via `JAVA_HOME`) for local `SparkSession` startup.

### Export time-series rows for Spark

Export from MongoDB (read-only) to a flattened JSONL file:

```bash
.venv/bin/python -m src.spark_analytics export \
  --output data/time_series_export.jsonl
```

Optional filters:

```bash
.venv/bin/python -m src.spark_analytics export \
  --output data/time_series_export.tsla.jsonl \
  --asset-id TSLA \
  --data-source-id alpha_vantage_api_v1 \
  --start-date 2026-01-01 \
  --end-date 2026-05-08
```

### Run Spark aggregation

```bash
.venv/bin/python -m src.spark_analytics aggregate \
  --input data/time_series_export.sample.jsonl \
  --output data/spark_aggregations
```

The Spark aggregation output includes:

- `count`
- `startTimestamp`, `endTimestamp`
- `closeMin`, `closeMax`, `closeAvg`
- `openMin`, `openMax`, `openAvg`
- `highMin`, `highMax`, `highAvg`
- `lowMin`, `lowMax`, `lowAvg`
- `volumeMin`, `volumeMax`, `volumeAvg`

### Run Spark MLlib forecast

```bash
.venv/bin/python -m src.spark_analytics forecast \
  --input data/time_series_export.sample.jsonl \
  --asset-id TSLA \
  --data-source-id alpha_vantage_api_v1
```

Forecast output contains:

- model: `Spark MLlib LinearRegression`
- training rows
- coefficient/intercept
- `rmse`, `r2`
- latest close and predicted next close
- direction (`up`, `down`, `flat`)

Spark ML output is for demonstration and engineering validation only, not financial advice.

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
