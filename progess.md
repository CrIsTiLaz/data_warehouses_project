# Acme Financial DWH - Progress Tracker

## Overall Status
- [x] UC1: Data Ingest
- [x] UC2: REST API for Data Access (Q1-Q5)
- [x] UC3: Analytics & Data Mining
- [x] UC4: LLM-Powered Assistant (MCP)
- [ ] Deliverables (report, IIAGen statement, demo video)

## Validator Improvement Pass (2026-05-25)

- [x] Spark M6 aggregation evidence strengthened (commands + output examples + tests)
- [x] Spark M7 ML workflow evidence strengthened (VectorAssembler + LinearRegression + runnable CLI)
- [x] DAL boundary improved via `src/repositories.py` and API/tool refactor
- [x] Pagination added for `GET /assets` and `GET /data-sources`
- [x] Temporal lifecycle handling improved (`valid_to` alignment + `asset_lifecycle_events`)
- [x] Assistant guardrails hardened (unknown/malformed tool calls, deterministic overrides, fallback safety)
- [x] Scalability story expanded in `README.md`
- [x] Test/lint verification rerun after changes

### New/Updated Modules

- `src/repositories.py` (Asset/DataSource/TimeSeries/Quality/Analytics repositories)
- `src/app/main.py` (repository-backed API reads, pagination)
- `ingestion_service.py` (idempotent asset lifecycle event logging on version changes)
- `src/quality.py` (collection/index setup for `asset_lifecycle_events`)
- `src/assistant/service.py` (stronger LLM guardrail and fallback handling)
- `src/assistant/mcp_adapter.py` (unknown/invalid tool-call rejection as value errors)
- `src/assistant/tools.py` (repository-backed tool reads)

### Added Tests

- `tests/test_repositories.py`
- `tests/test_ingestion_temporal.py`
- `tests/test_spark_analytics_cli.py`
- Extended `tests/test_main.py` with pagination and validation coverage
- Extended `tests/test_assistant.py` with malformed/unknown tool-call guardrail tests, quality guardrail override, and final-answer failure fallback test

### Latest Verification

- `.venv/bin/python -m pytest` -> passed (`75 passed`)
- `.venv/bin/python -m ruff check .` -> passed
- Spark aggregation command (`src.spark_analytics aggregate`) -> passed locally with Java 17
- Spark forecast command (`src.spark_analytics forecast`) -> passed locally with Java 17

---

## UC1 - Data Ingest (Completed)

### What works
- Ingestion pipeline implemented in `ingestion_service.py`.
- Stocks ingestion (Alpha Vantage): `TSLA`, `AMZN`.
- Crypto ingestion (Alpha Vantage): `BTC`, `ETH`.
- Metals ingestion (metals.dev): `XAG`, `XPT`.
- Bond represented via seed/reference data: Romanian Government Bond (`ROS2CQA3C829`).
- Data provenance tracked via `dataSourceId`.
- Temporal asset versioning active (`version`, `valid_from`, `is_active`, `valid_to`).

### Current DB observations
- `time_series`: populated (latest observed around 598 rows).
- `assets`: multiple versions (historical + active), active rows observed: 7.
- `data_sources`: contains provider records including `alpha_vantage_api_v1` and `metals_dev_v1`.

### Notes
- Metals historical backfill uses metals.dev timeseries with chunking and retry/backoff resilience.
- Duplicate/inactive asset versions are expected due to temporal model.

---

## UC2 - REST API for Data Access (Completed)

### What works
- FastAPI backend implemented in `src/app/main.py`.
- MongoDB startup/shutdown lifecycle uses `MONGO_URI` / `MONGODB_ATLAS_URI` and `MONGO_DB_NAME` / `DB_NAME`.
- Mongo documents are serialized safely for JSON responses, including `_id`.
- Missing assets, data sources, and time-series queries return 404 with clear details.
- Invalid Q5 query parameters return 400 with clear details.
- API can be run locally with: `uvicorn app.main:app --app-dir src --reload`.

### Required endpoints
- [x] Q1: List all available assets (`assetId` list, active rows only)
- [x] Q2: Get full metadata for a specific `assetId` (latest active version)
- [x] Q3: List all registered data sources
- [x] Q4: Get full details for a specific `dataSourceId`
- [x] Q5: Get historical time-series filtered by `assetId` and `dataSourceId`
- [x] Optional Q5 date filters: `startDate` and `endDate`

### Verification
- API tests cover Q1-Q5 happy paths, not-found responses, Q5 date filtering, and validation errors.
- Latest run at this milestone: `.venv/bin/python -m pytest` passed.
- Latest run: `.venv/bin/python -m ruff check .` passed.
- Live API startup verified against MongoDB Atlas via `uvicorn`.
- Live endpoint checks returned:
  - `/assets`: `AMZN`, `BTC`, `ETH`, `ROS2CQA3C829`, `TSLA`, `XAG`, `XPT`
  - `/assets/TSLA`: latest active TSLA metadata
  - `/data-sources`: registered source IDs including `alpha_vantage_api_v1` and `metals_dev_v1`
  - `/data-sources/alpha_vantage_api_v1`: Alpha Vantage provider details
  - `/time-series?assetId=TSLA&dataSourceId=alpha_vantage_api_v1`: 100 rows sorted ascending by `timestamp`

---

## UC3 - Analytics (Completed)
- [x] Step 3: Data Quality & Governance prep
- [x] Min / Max / Average metrics on time-series
- [x] Basic trend forecasting
- [x] Ensure data shape is Spark-friendly
- [x] Apache Spark aggregation workflow
- [x] Spark MLlib prediction workflow

### What works
- Added reusable analytics helpers in `src/analytics.py`:
  - `summarize_time_series`
  - `forecast_next_close`
  - `flatten_time_series_rows`
  - `build_time_series_query`
- Added `GET /analytics/summary` with required `assetId` and `dataSourceId`, optional `startDate`/`endDate`, and deterministic min/max/average metrics from read-only `time_series` data.
- Added `GET /analytics/forecast` with deterministic trend estimate based on average daily close change over the latest valid close values (default basis window up to 10 values).
- Added `GET /analytics/spark-shape` returning flattened rows (`assetId`, `dataSourceId`, `timestamp`, `open`, `high`, `low`, `close`, `volume`) for Spark/DataFrame ingestion.
- Added validation parity with `/time-series` for missing identifiers, invalid dates, and reversed date ranges.
- Added assistant read-only analytics tools in `src/assistant/tools.py`:
  - `get_analytics_summary`
  - `get_analytics_forecast`
- Extended assistant planning/composition in `src/assistant/service.py` so analytics summary and forecast questions route to analytics tools with grounded responses.
- Added PySpark workflows in `src/spark_analytics.py` and `src/spark_common.py`.
- Added Spark CLI commands for:
  - `export` (read-only MongoDB `time_series` -> flattened JSONL)
  - `aggregate` (Spark DataFrame grouped metrics)
  - `forecast` (Spark MLlib `LinearRegression` next-close prediction)

### Verification
- Added endpoint tests for UC3 summary/forecast/spark-shape happy paths, date filtering, validation errors, not-found behavior, and insufficient-close forecast behavior.
- Added unit tests for analytics helper functions (`tests/test_analytics.py`).
- Added assistant tests proving analytics summary/forecast questions route to analytics tools.
- Added helper tests for Spark workflow input/date/direction logic (`tests/test_spark_common.py`).
- Latest run at this milestone: `.venv/bin/python -m pytest` passed.
- Latest run: `.venv/bin/python -m ruff check .` passed.

### Spark Workflows
- Spark aggregation implemented with PySpark DataFrames, `SparkSession`, and Spark SQL functions (`count`, `min`, `max`, `avg`) grouped by `assetId` and `dataSourceId`.
- Spark ML workflow implemented with Spark MLlib `LinearRegression` using `VectorAssembler` over `timeIndex` to predict `close`.
- Commands:
  - `.venv/bin/python -m src.spark_analytics aggregate --input data/time_series_export.sample.jsonl --output data/spark_aggregations`
  - `.venv/bin/python -m src.spark_analytics forecast --input data/time_series_export.sample.jsonl --asset-id TSLA --data-source-id alpha_vantage_api_v1`
- Verification status: PySpark and NumPy installation completed; command execution currently blocked in this environment by missing Java runtime (`SparkSession` startup requires JRE/JDK).

### Step 3 - Data Quality & Governance (Completed)
- Added reusable data quality helpers in `quality.py`.
- Added `ingestion_runs` audit collection support.
- Ingestion now records final run documents with run ID, timestamps, status, requested/succeeded/failed symbols, inserted records, skipped invalid rows, skipped duplicates, validation errors, error summary, and touched data sources.
- Time-series writes now validate `assetId`, `dataSourceId`, UTC ISO `timestamp`, and numeric price fields before insertion.
- Invalid time-series rows are skipped and counted without crashing the whole run.
- Duplicate time-series rows are guarded by a unique index on (`assetId`, `dataSourceId`, `timestamp`) and duplicate-key insert errors are counted as skipped duplicates.
- `db_setup.py` and ingestion setup ensure `ingestion_runs` and quality indexes are created idempotently without deleting existing data.
- Added `/quality/freshness` endpoint with `fresh`, `stale`, and `missing` status values.
- Added `/quality/summary` endpoint with total row count, duplicate risk, latest invalid-row count, and latest ingestion run status.
- Added tests for validation rejection, duplicate handling, ingestion audit success/failure documents, freshness status logic, and quality endpoint schemas.
- Latest run at this milestone: `.venv/bin/python -m pytest` passed.
- Latest run: `.venv/bin/python -m ruff check .` passed.

---

## UC4 - LLM Assistant with MCP (Completed)
- [x] Added assistant package under `src/assistant/`.
- [x] Added request/response models and grounding metadata schema.
- [x] Added read-only DWH tools for assets, data sources, time-series, freshness, and quality summary.
- [x] Added MCP adapter abstraction with env-configured enablement, timeout, model label, optional provider key, and optional endpoint.
- [x] Added OpenRouter LLM integration using `LLM_API_KEY`, `LLM_MODEL`, optional `LLM_BASE_URL`, and optional `LLM_TIMEOUT_SECONDS`.
- [x] LLM mode exposes tool definitions, lets the model choose read-only tools, executes selected tools through the adapter, and asks the model for a final answer using tool outputs.
- [x] Added deterministic fallback planning when OpenRouter is not configured or a runtime LLM/network error occurs.
- [x] Added deterministic service orchestration that selects tools, executes them through the adapter, and composes grounded answers when fallback mode is used.
- [x] Added `POST /assistant/query`.
- [x] Empty questions return 400; overly long questions return 422.
- [x] MCP disabled/unavailable returns structured 503 diagnostics.
- [x] Partial LLM configuration returns structured 503 diagnostics without exposing secrets.
- [x] Missing data and ambiguous time-series questions return `insufficient_data` with explicit clarification.
- [x] Numeric claims are derived from tool results and included with grounding metadata.
- [x] Added assistant tests for LLM tool calls, multi-tool calls, OpenRouter HTTP mocking, runtime fallback, missing data, MCP unavailable, invalid input, endpoint integration, and no-hallucination behavior.
- [x] Latest run at this milestone: `.venv/bin/python -m pytest` passed.
- [x] Latest run: `.venv/bin/python -m ruff check .` passed.

---

## Deliverables (Pending)
- [ ] Runnable implementation
- [ ] Project report (architecture + design + AI usage)
- [ ] IIAGen statement
- [ ] Demo video
