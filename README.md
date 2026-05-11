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

Sample requests:

```bash
curl "http://127.0.0.1:8000/assets"
curl "http://127.0.0.1:8000/assets/TSLA"
curl "http://127.0.0.1:8000/data-sources"
curl "http://127.0.0.1:8000/data-sources/alpha_vantage_api_v1"
curl "http://127.0.0.1:8000/time-series?assetId=TSLA&dataSourceId=alpha_vantage_api_v1"
curl "http://127.0.0.1:8000/time-series?assetId=TSLA&dataSourceId=alpha_vantage_api_v1&startDate=2026-05-01&endDate=2026-05-10"
```
