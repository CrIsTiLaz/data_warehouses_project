# Acme Financial DWH - Progress Tracker

## Overall Status
- [x] UC1: Data Ingest
- [ ] UC2: REST API for Data Access (Q1-Q5)
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

## Next Step - UC2 (In progress: not started yet)

### Required endpoints
- [ ] Q1: List all available assets (`assetId` list)
- [ ] Q2: Get full metadata for a specific `assetId` (latest active version)
- [ ] Q3: List all registered data sources
- [ ] Q4: Get full details for a specific `dataSourceId`
- [ ] Q5: Get historical time-series filtered by `assetId` and `dataSourceId`

### Suggested implementation
- Backend framework: FastAPI
- Add routes + Pydantic response models
- Add basic tests for each endpoint

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