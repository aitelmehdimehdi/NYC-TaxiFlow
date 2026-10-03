# Airflow DAG Guide: NYC Taxi Pipeline

## Overview

The `taxi_pipeline` DAG orchestrates the complete data engineering lifecycle:

```
ingest (Phase 2)
    ↓
validate_raw (Phase 3)
    ↓
spark_transform (Phase 4)
    ↓
load_postgres (Phase 5)
    ↓
dbt_run (Phase 6a)
    ↓
dbt_test (Phase 6b)
```

## How It Works

### Parameters

The DAG is **parameterized** by year and month, so you can process any month independently.

Parameters are passed as a JSON config when triggering the DAG:

```json
{
  "year": 2024,
  "month": 1
}
```

### Task Descriptions

| Task | Phase | Purpose | Key Function |
|------|-------|---------|--------------|
| `ingest` | 2 | Download raw TLC data → Bronze | `ingestion.ingest()` |
| `validate_raw` | 3 | Validate schema of Bronze data | `quality.validate_schema()` |
| `spark_transform` | 4 | Clean & transform Bronze → Silver | `processing.clean_trips()` |
| `load_postgres` | 5 | Load Silver → PostgreSQL staging | `warehouse.load_month_to_staging()` |
| `dbt_run` | 6a | Build star schema (staging → gold) | `dbt run` |
| `dbt_test` | 6b | Quality tests on Gold layer | `dbt test` |

## Running the DAG

### 1. Start Docker & Airflow

```bash
cd /path/to/project
docker compose up -d
```

Check that Airflow is running:
```bash
http://localhost:8080
```

Log in: **admin / admin**

### 2. Trigger the DAG for a Specific Month

#### Using the Airflow CLI

```bash
# Process January 2024
docker compose exec airflow-webserver airflow dags test taxi_pipeline 2024-01-15 \
  --conf '{"year": 2024, "month": 1}'
```

```bash
# Process February 2024
docker compose exec airflow-webserver airflow dags test taxi_pipeline 2024-02-15 \
  --conf '{"year": 2024, "month": 2}'
```

#### Using the Airflow Web UI

1. Navigate to **DAGs** → **taxi_pipeline**
2. Click the **Trigger DAG** button (blue play icon)
3. In the **Configuration** JSON field, paste:
   ```json
   {
     "year": 2024,
     "month": 1
   }
   ```
4. Click **Trigger**

The DAG will start running immediately.

### 3. Monitor Execution

**Via Web UI:**
- Go to **DAGs** → **taxi_pipeline**
- Click on the run to see the **Graph View**
- Each task shows success (green), failure (red), or running (blue)
- Click a task to see its logs

**Via CLI:**
```bash
# List all runs
docker compose exec airflow-webserver airflow dags list-runs --dag-id taxi_pipeline

# Get logs for a specific task
docker compose exec airflow-webserver airflow tasks logs taxi_pipeline spark_transform <run_id>
```

---

## Important Notes

### XCom (Cross-communication)

Tasks use **XCom** to pass data between them:

- `ingest` → stores bronze file path via `xcom_push()`
- `validate_raw` → retrieves it via `xcom_pull()`

This allows each task to know the exact location of data from upstream tasks.

### Environment Variables

The DAG expects these to be set in your Docker environment (already in your `docker-compose.yml`):

```yaml
POSTGRES_HOST: host.docker.internal
POSTGRES_PORT: 5432
POSTGRES_DB: nyc_taxi
POSTGRES_USER: postgres
POSTGRES_PASSWORD: <your_password>
```

dbt automatically uses these to connect to your PostgreSQL warehouse.

### Failure Handling

If any task fails:
1. The pipeline **stops** (no downstream tasks execute)
2. The failed task shows **red** in the web UI
3. You can **retry** the DAG run from the Web UI
4. Check logs to diagnose the issue

Example: If `validate_raw` fails, `spark_transform` will not run.

---

## Typical Workflow Example

### Process January 2024 end-to-end

```bash
# 1. Start the environment
docker compose up -d

# 2. Wait for Airflow to initialize (~30 seconds)
sleep 30

# 3. Trigger the pipeline for January 2024
docker compose exec airflow-webserver airflow dags test taxi_pipeline 2024-01-15 \
  --conf '{"year": 2024, "month": 1}'

# 4. Watch logs in real-time
docker compose logs -f airflow-scheduler
```

Expected output (if all succeeds):
```
[2024-01-15 10:00:00,000] INFO - Running task ingest
[2024-01-15 10:05:00,000] INFO - Ingestion complete
[2024-01-15 10:05:10,000] INFO - Running task validate_raw
[2024-01-15 10:05:30,000] INFO - Schema validation passed
[2024-01-15 10:05:40,000] INFO - Running task spark_transform
[2024-01-15 10:20:00,000] INFO - Spark transformation complete
[2024-01-15 10:20:10,000] INFO - Running task load_postgres
[2024-01-15 10:25:00,000] INFO - Warehouse load complete
[2024-01-15 10:25:10,000] INFO - Running task dbt_run
[2024-01-15 10:30:00,000] INFO - dbt run successful
[2024-01-15 10:30:10,000] INFO - Running task dbt_test
[2024-01-15 10:35:00,000] INFO - dbt test successful
[2024-01-15 10:35:10,000] INFO - DAG run completed successfully
```

### Processing Multiple Months

Once January works, process other months:

```bash
# February
docker compose exec airflow-webserver airflow dags test taxi_pipeline 2024-02-15 \
  --conf '{"year": 2024, "month": 2}'

# March
docker compose exec airflow-webserver airflow dags test taxi_pipeline 2024-03-15 \
  --conf '{"year": 2024, "month": 3}'
```

Each month is independent and idempotent (running twice with the same month overwrites safely).

---

## Troubleshooting

### Issue: "No module named 'ingestion'"

**Cause:** Python sys.path not set correctly.

**Solution:** The DAG has:
```python
sys.path.insert(0, '/opt/airflow/src')
```

This adds your `./src` folder to the Python path. Make sure:
- Your `./src` folder is mounted in `docker-compose.yml`
- The files exist: `src/ingestion/ingest.py`, etc.

### Issue: "POSTGRES_PASSWORD not set"

**Cause:** Environment variable missing in docker-compose.yml.

**Solution:** Ensure your `docker-compose.yml` has:
```yaml
environment: &airflow-common-env
  ...
  POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
```

And set it in your shell before `docker compose up`:
```bash
export POSTGRES_PASSWORD=your_password
docker compose up -d
```

### Issue: "dbt: command not found"

**Cause:** dbt not installed in the Airflow container.

**Solution:** Check your `Dockerfile` has:
```dockerfile
COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt
```

And `requirements-airflow.txt` includes:
```
dbt-postgres==1.7.0
```

### Issue: Schema validation fails

**Cause:** Bronze file has unexpected columns or types.

**Solution:**
1. Check the TLC dataset schema has not changed
2. Update `quality/schema.py` if TLC changed column definitions
3. Look at logs to see exactly which columns/types are missing

### Issue: Spark transformation is slow

**Cause:** PySpark is running on your local machine with limited resources.

**Solution (Phase 10 improvement):**
- Batch process (multiple months in one Spark job)
- Add Spark memory/core configuration
- Later: migrate to cloud Spark (EMR, Databricks, Dataproc)

---

## Next Steps

### Phase 7 Complete ✓

You now have:
- ✓ Orchestrated pipeline (Airflow DAG)
- ✓ Parameterized by year/month
- ✓ Proper error handling
- ✓ XCom for task communication
- ✓ dbt integration

### Phase 8: Docker

If you haven't already, make sure your `docker-compose.yml` is complete.

### Phase 9: Power BI

Once the Gold layer is built (after successful dbt_run), connect Power BI to:
```
Server: host.docker.internal
Database: nyc_taxi
Tables:
  analytics.fact_trips
  analytics.dim_date
  analytics.dim_zone
  analytics.dim_payment
  analytics.dim_vendor
```

### Phase 10 (Optional): Production Improvements

Consider adding:
- **Incremental processing:** Only reprocess changed months
- **Monitoring:** Log rows in/out/rejected per run
- **Backfill:** Process Jan–Dec 2024 in one Airflow run
- **CI/CD:** GitHub Actions to test DAG on push
- **Scheduling:** `schedule_interval="@monthly"` for automatic monthly runs

---

## Reference: DAG Structure

```python
dag = DAG(
    dag_id="taxi_pipeline",
    schedule_interval=None,  # Manual trigger with parameters
    ...
)

with dag:
    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=task_ingest,
        op_kwargs={
            "year": "{{ dag_run.conf.year | int }}",
            "month": "{{ dag_run.conf.month | int }}",
        },
    )
    
    validate_task = PythonOperator(task_id="validate_raw", ...)
    transform_task = PythonOperator(task_id="spark_transform", ...)
    load_task = PythonOperator(task_id="load_postgres", ...)
    dbt_run_task = PythonOperator(task_id="dbt_run", ...)
    dbt_test_task = PythonOperator(task_id="dbt_test", ...)
    
    ingest_task >> validate_task >> transform_task >> load_task >> dbt_run_task >> dbt_test_task
```

---

## Questions?

If the DAG fails, check:
1. **Logs** in the Airflow Web UI (most helpful)
2. **Docker logs:** `docker compose logs airflow-scheduler`
3. **Environment variables:** `docker compose exec airflow-webserver env | grep POSTGRES`
4. **File paths:** Ensure `./src`, `./dbt`, `./data` are mounted in docker-compose.yml
