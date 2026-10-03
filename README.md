# 🚕 NYC TaxiFlow — End-to-End Data Engineering Platform

A production-inspired data engineering pipeline that transforms raw NYC Taxi & Limousine Commission (TLC) trip records into analytics-ready datasets and a Power BI dashboard — built to demonstrate the complete lifecycle of a modern data platform, from ingestion to business intelligence.

> **This is not a notebook that analyzes a CSV.** It's a layered, tested, idempotent pipeline spanning ingestion, a medallion data lake, distributed processing, a relational warehouse, SQL-based transformation and testing, and orchestration — run end-to-end against **3.8 million real trip records**.

---

## 📐 Architecture

```
NYC TLC (public data) 
        │
        ▼
┌───────────────────┐
│  Python Ingestion  │   download, validate, idempotent (metadata log)
└─────────┬──────────┘
          ▼
┌───────────────────┐
│   BRONZE (Parquet) │   immutable raw layer, partitioned by year/month
└─────────┬──────────┘
          ▼
┌───────────────────┐
│      PySpark       │   cleaning, validation, derived fields
└─────────┬──────────┘
          ▼
┌─────────────────────────────┐
│  SILVER (valid)  │ QUARANTINE (invalid, with reason) │
└─────────┬────────────────────┘
          ▼
┌───────────────────┐
│  PostgreSQL stage  │   idempotent load (delete + COPY per batch)
└─────────┬──────────┘
          ▼
┌───────────────────┐
│        dbt         │   staging → intermediate → marts, with tests
└─────────┬──────────┘
          ▼
┌───────────────────┐
│  GOLD (star schema)│   fact_trips + 4 dimensions
└─────────┬──────────┘
          ▼
┌───────────────────┐
│      Power BI      │   executive, geographic, temporal, financial views
└───────────────────┘

        Orchestrated end-to-end by Apache Airflow
        Entire environment reproducible via Docker
```

## 🧰 Tech stack — and why each piece is there

Every technology here has one clear job. Nothing was added just to pad a CV line.

| Technology | Responsibility |
|---|---|
| **Python** | Ingestion (download, validate, track idempotency) |
| **PySpark** | Distributed-style cleaning and transformation (Bronze → Silver) |
| **Parquet** | Columnar, compressed, partition-friendly data lake format |
| **PostgreSQL** | Relational warehouse layer (staging + analytics schemas) |
| **dbt** | SQL transformation, testing, and documentation (staging → star schema) |
| **Apache Airflow** | Orchestration — coordinates tasks, handles failure, no business logic itself |
| **Docker / Docker Compose** | Reproducible environment for Airflow + its metadata DB |
| **Power BI** | Business-facing dashboard on top of the Gold layer |

## ✅ What's been built and verified against real data

Every number below comes from an actual run against **3,837,248 real Yellow Taxi trip records** — not test fixtures.

### 1. Ingestion — idempotent, resilient, logged
- Downloads directly from the official TLC CloudFront endpoint, streaming to disk.
- Append-only JSON metadata log tracks every attempt (success *and* failure) for a full audit trail.
- Re-running `ingest()` for an already-successful month is a safe no-op; a previously-failed month always retries cleanly from scratch.
- 22 automated pytest tests covering network failures, corrupt files, empty responses, and idempotency edge cases.

### 2. Data quality — nothing is silently discarded
The pipeline distinguishes **invalid** records (removed, sent to quarantine with a documented reason) from **suspicious** ones (kept, just flagged):

| Metric | Count |
|---|---|
| Input rows | 3,837,248 |
| Valid rows → Silver | 3,822,900 |
| Quarantined (invalid) | 14,348 |
| Flagged as suspicious (kept) | 10,603 |

A real, non-obvious data pattern was investigated and confirmed rather than assumed: ~1M rows have five columns (`passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, `airport_fee`) null *simultaneously* — verified to correlate 100% with Flex Fare trips (`payment_type == 0`), a source-system behavior rather than a data defect. These nulls are preserved as-is, never filled or miscategorized.

### 3. PySpark transformation
- Type casting, column normalization, and derived fields: `trip_duration_minutes`, `pickup_date`, `pickup_hour`, `pickup_weekday`, `pickup_month`.
- A real null-handling bug (SQL three-valued logic silently dropping Flex Fare rows from filters) was caught during testing and fixed — documented in code comments as a cautionary example.

### 4. PostgreSQL warehouse loading
- Idempotent per-batch loading via `DELETE` + bulk `COPY` (not row-by-row `INSERT`) for performance at scale.
- Re-running a load for the same month replaces that month's data rather than duplicating it.

### 5. dbt — tested, documented star schema
- **`fact_trips`** (grain: one row = one taxi trip) joined to **`dim_date`**, **`dim_zone`**, **`dim_payment`**, **`dim_vendor`**.
- The raw TLC data has **no natural trip identifier** — a surrogate key was engineered, tested at scale, and iteratively hardened: an initial version produced 11 real hash collisions out of 3.8M rows; root-caused by inspecting the actual colliding records (not guessed), then fixed by enriching the key — first down to 1 collision, then to zero.
- All dbt tests pass: `not_null`, `unique`, `accepted_values`, and `relationships` (referential integrity between the fact table and every dimension).

### 6. Orchestration & infrastructure
- Airflow DAG (`taxi_pipeline`) parameterized by year/month: `ingest → spark_transform → load_postgres → dbt_seed → dbt_run → dbt_test`.
- Runs in Docker (custom image with Java + PySpark + dbt), connecting to a native PostgreSQL instance — demonstrating that containerized orchestration and host-level services can interoperate cleanly.
- Every stage fails loudly and stops downstream execution on error — no silent corruption propagates through the pipeline.

## 📊 Power BI Dashboard

Built on top of the `analytics` (Gold) schema:
- **Executive overview** — total trips, revenue, average fare/distance/duration, trends over time
- **Geographic analysis** — busiest pickup/dropoff zones, origin-destination patterns
- **Temporal analysis** — demand by hour/weekday/month, a weekday × hour heatmap
- **Financial analysis** — revenue by zone and payment type, tip rate analysis

*(See `powerbi/dashboard_documentation.md` and screenshots in `powerbi/screenshots/`.)*

## 📁 Project structure

```
nyc-taxi-data-platform/
├── airflow/dags/taxi_pipeline.py    # Orchestration DAG
├── src/
│   ├── ingestion/                   # Download + idempotency tracking
│   ├── quality/                     # Schema validation
│   ├── processing/                  # PySpark Bronze → Silver
│   ├── warehouse/                   # Postgres connection + loading
│   └── utils/                       # Config, logging
├── dbt/taxi_analytics/              # staging → intermediate → marts
├── data/{bronze,silver,quarantine}/ # Medallion layers (gitignored)
├── tests/                           # 22+ pytest tests
├── docs/                            # Architecture, data dictionary, decisions
├── docker-compose.yml               # Airflow + its metadata DB
└── Dockerfile                       # Custom Airflow image (Java + pipeline deps)
```

## 🚀 Running it

```bash
# 1. Clone and configure
git clone <repo-url>
cd nyc-taxi-data-platform
cp .env.example .env   # fill in your Postgres credentials

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Start PostgreSQL (native install) and create the database
createdb nyc_taxi

# 4. Start Airflow (Docker)
docker compose up --build

# 5. Trigger the pipeline via the Airflow UI (localhost:8080)
#    Config: {"year": 2026, "month": 6}
```

Full step-by-step instructions, including the dbt seed/run/test setup, are in `docs/architecture.md`.

## 🎓 What this project demonstrates

- **Medallion architecture** (Bronze/Silver/Gold) with a clear, documented rationale for each layer
- **Idempotency** engineered explicitly at every stage — ingestion, Spark writes, warehouse loads — not just claimed
- **Data quality as a first-class concern**: invalid vs. suspicious records are a deliberate design decision, not an afterthought, and every anomaly in this README was *investigated with real queries*, not assumed
- **Debugging real, non-synthetic problems**: a Windows PySpark worker misconfiguration, a null-handling SQL bug, surrogate-key hash collisions — all found, diagnosed, and fixed against actual data, with the reasoning preserved in code comments
- **Dimensional modeling**: an explicitly documented fact grain, a star schema, and referential-integrity tests enforcing it
- **Infrastructure as code**: the entire orchestration environment is reproducible via `docker compose up`

## 🔭 Possible next steps

- Retry-with-backoff on network failures (a transient DNS failure was encountered and documented during development)
- CI/CD via GitHub Actions (tests + dbt tests + lint on push)
- Incremental backfill support for multiple months
- Lightweight run-level monitoring (rows processed/rejected/duration per execution)

---

*Built as a learning-focused portfolio project to demonstrate production-style data engineering practices at a scale appropriate for a Data Engineer / Data Analyst internship.*
