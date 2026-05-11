# Acme Financial DWH - Progress Tracker

## Overall Status
- [x] UC1: Data Ingest
- [x] UC2: REST API for Data Access (Q1-Q5)
- [ ] UC3: Analytics & Data Mining
- [ ] UC4: LLM-Powered Assistant (MCP)
- [ ] Deliverables (report, IIAGen statement, demo video)

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
- Latest run: `.venv/bin/python -m pytest` passed with 18 tests.
- Latest run: `.venv/bin/python -m ruff check .` passed.
- Live API startup verified against MongoDB Atlas via `uvicorn`.
- Live endpoint checks returned:
  - `/assets`: `AMZN`, `BTC`, `ETH`, `ROS2CQA3C829`, `TSLA`, `XAG`, `XPT`
  - `/assets/TSLA`: latest active TSLA metadata
  - `/data-sources`: registered source IDs including `alpha_vantage_api_v1` and `metals_dev_v1`
  - `/data-sources/alpha_vantage_api_v1`: Alpha Vantage provider details
  - `/time-series?assetId=TSLA&dataSourceId=alpha_vantage_api_v1`: 100 rows sorted ascending by `timestamp`

---

## UC3 - Analytics (Pending)
- [ ] Min / Max / Average metrics on time-series
- [ ] Basic trend forecasting
- [ ] Ensure data shape is Spark-friendly

---

## UC4 - LLM Assistant with MCP (Pending)
- [ ] Integrate MCP-based assistant
- [ ] Ground responses in live MongoDB queries

---

## Deliverables (Pending)
- [ ] Runnable implementation
- [ ] Project report (architecture + design + AI usage)
- [ ] IIAGen statement
- [ ] Demo video
