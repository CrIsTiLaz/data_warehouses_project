To implement **UC 1 (Data Ingest)** correctly and support **UC 3 (Analytics)**, use the prompt below in Cursor Chat (`Cmd+L`) or Composer (`Cmd+I`).

---

### The Cursor Agent Prompt

> **Role**: Senior Data Engineer
>
> **Task**: Create a production-grade Python script `ingestion_service.py` to fulfill **UC 1 (Data Ingest)** and provide data for **UC 3 (Analytics)**.
>
> **Core Architecture Requirements**:
> 1. **API Integration**: Use the `requests` library to fetch data from the **Alpha Vantage API**.
>    - Fetch **TIME_SERIES_DAILY_ADJUSTED** for **TSLA** (stock).
>    - Fetch **DIGITAL_CURRENCY_DAILY** for **BTC** (crypto) in **USD**.
>    - Fetch at least **100 days** of historical data points for each.
> 2. **Temporal Versioning Logic (Strict Compliance)**:
>    - Before inserting into the `assets` collection, check if a document with that `assetId` already exists.
>    - If it exists, compare the metadata (for example: description, exchange). If different, **never update in place**.
>    - Instead:
>      a) Set the previous version's `is_active` field to `False`.  
>      b) Insert a **new document** with `version` incremented by 1, `valid_from` set to now, and `is_active` set to `True`.
> 3. **Heterogeneous Data Handling**:
>    - Map different vendor JSON structures into our common schema.
>    - Stocks should include `exchange` and `dividend_yield` in attributes.
>    - Crypto should include `market_cap` and `max_supply` in attributes.
> 4. **Provenance and Time Series**:
>    - Every record in `assets` and `time_series` must include `dataSourceId: "alpha_vantage_api_v1"`.
>    - Bulk insert the 100 historical price points into the `time_series` collection, ensuring each has an `assetId` reference.
>
> **Technical Specs**:
> - Use `python-dotenv` to load `MONGO_URI` and `ALPHA_VANTAGE_API_KEY`.
> - Implement error handling for API rate limits (Alpha Vantage free tier is 5 calls/minute).
> - Include a main execution block that runs ingestion for TSLA and BTC.

---

### Why this prompt helps

- **Temporal audit trail**: versioned inserts + deactivation of old versions aligns with the strict temporal requirement.
- **Data grounding for analytics**: 100 days gives enough history for moving averages and trend analysis.
- **Provenance compliance**: explicit `dataSourceId` on both entities and measurements.

### After the script is created

1. **API key**: Get one from [Alpha Vantage](https://www.alphavantage.co/support/#api-key) and add `ALPHA_VANTAGE_API_KEY=YOUR_KEY` to `.env`.
2. **Run**:
   ```bash
   python ingestion_service.py
   ```
3. **Verify**: In MongoDB Atlas Data Explorer, confirm `time_series` has at least **200** new rows (100 for TSLA + 100 for BTC).

Would you like a follow-up prompt for UC 3 (Q1-Q5 analytics queries) next?