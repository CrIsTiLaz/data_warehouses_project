# Project Requirements: Acme Ltd Financial Data Warehouse

This document outlines the official requirements for the development of the **Acme Ltd** platform, a specialized financial data warehouse designed for high-integrity data ingestion and analysis.

## 1. Core Architectural Paradigms (Non-Functional)

### 1.1 Temporal Database Paradigm
* **No Updates/Deletes**: The system must strictly follow a temporal logic where records are never overwritten or removed.
* **Versioning**: Any change to an asset's metadata must result in a new record insertion with an incremented `version` number.
* **Audit Fields**: Every document must contain:
    * `version`: Integer (starting at 1).
    * `valid_from`: UTC timestamp of insertion.
    * `is_active`: Boolean flag (True for the current version, False for historical/soft-deleted records).

### 1.2 Heterogeneous Data Support
* The database must handle diverse asset classes with varying attributes in the same or linked collections without a rigid schema.
* **Required Asset Classes**:
    * **Stocks**: (e.g., Tesla, Amazon) with attributes like exchange and dividend yield.
    * **Bonds**: (e.g., Romanian Government Bonds) with ISINs (e.g., ROS2CQA3C829) and maturity dates.
    * **Cryptocurrencies**: (e.g., Bitcoin, Ethereum) with circulating supply and market cap.
    * **Metals**: (e.g., Silver, Platinum).

### 1.3 Data Provenance
* Every data point (metadata or time-series) must be traceable.
* Required field: `dataSourceId` (e.g., `alpha_vantage_v1`, `nasdaq_lbma_feed`).

---

## 2. Functional Requirements (Use Cases)

### UC 1: Data Ingest
* Import data from at least one external provider via a **RESTful API** (e.g., Alpha Vantage, Nasdaq Data Link).
* Implement the mapping logic to convert vendor JSON into the internal DWH schema while applying the temporal paradigm.

### UC 2: RESTful API for Data Access
Implement a backend (FastAPI/Flask) providing five mandatory query endpoints:
* **Q1**: List all available assets (returns list of `assetId`).
* **Q2**: Retrieve full metadata for a specific `assetId` (latest active version).
* **Q3**: List all registered data sources.
* **Q4**: Retrieve full details for a specific `dataSourceId`.
* **Q5**: Retrieve historical time-series data filtered by `assetId` and `dataSourceId`.

### UC 3: Analytics & Data Mining
* Perform statistical analysis on the stored time-series data.
* Generate metrics: Minimum, Maximum, Average, and basic trend forecasting.
* Ensure the data is structured to be consumable by processing engines like Apache Spark.

### UC 4: LLM-Powered Assistant
* Integrate a Large Language Model (LLM) using the **Model Context Protocol (MCP)**.
* The assistant must be "grounded" in the DWH data, answering natural language questions (e.g., "What was the average price of Bitcoin last week?") using live database queries.

---

## 3. Technology Stack
* **Database**: NoSQL (MongoDB Atlas recommended).
* **Backend**: Python (FastAPI/Flask).
* **AI Integration**: Model Context Protocol (MCP).
* **Environment**: Local project with a `requirements.txt` and virtual environment.

---

## 4. Deliverables
1.  **Runnable Implementation**: The full source code.
2.  **Project Report**: Documentation explaining the architecture, design choices, and AI usage.
3.  **IIAGen Statement**: Disclosure of how Generative AI (e.g., Cursor) was used to assist in development.
4.  **Video Demonstration**: A short recording showcasing the data ingestion, API functionality, and the LLM assistant interaction.