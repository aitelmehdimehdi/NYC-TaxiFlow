# Airflow DAG Deployment Checklist

## Step 1: Place the DAG File

**Copy** `taxi_pipeline.py` to your Airflow DAGs directory:

```bash
cp taxi_pipeline.py /path/to/project/airflow/dags/
```

Expected location:
```
your-project/
├── airflow/
│   └── dags/
│       └── taxi_pipeline.py  ← place it here
├── src/
├── dbt/
├── docker-compose.yml
└── ...
```

The DAG will be auto-discovered by Airflow (thanks to the volume mount in `docker-compose.yml`).

---

## Step 2: Verify Your Project Structure

Before starting Airflow, make sure these folders exist and are **mounted in docker-compose.yml**:

```bash
# Check that these paths exist locally
ls -la src/
ls -la dbt/
ls -la data/
ls -la airflow/dags/
```

Your `docker-compose.yml` should have volumes like:

```yaml
volumes: &airflow-common-volumes
  - ./airflow/dags:/opt/airflow/dags
  - ./src:/opt/airflow/src
  - ./dbt:/opt/airflow/dbt
  - ./data:/opt/airflow/data
  - airflow-logs:/opt/airflow/logs
```

✓ If this is already in your docker-compose.yml, you're good.

---

## Step 3: Verify requirements-airflow.txt

Your `requirements-airflow.txt` must include **dbt** and **PySpark** support.

Check that it has (at minimum):

```
dbt-postgres>=1.7.0
pyspark>=3.5.0
psycopg2-binary>=2.9.0
requests>=2.28.0
pyarrow>=13.0.0
```

If missing, add them and rebuild the Docker image:

```bash
docker compose build
```

---

## Step 4: Set POSTGRES_PASSWORD Environment Variable

Before running `docker compose up`, set the password:

```bash
export POSTGRES_PASSWORD=your_secure_password_here
```

This password is used by:
- Airflow's internal metadata database (postgres-airflow)
- Your NYC Taxi warehouse (nyc_taxi on Windows)

Both must be accessible.

---

## Step 5: Start Docker & Airflow

```bash
docker compose up -d
```

Wait ~30 seconds for services to initialize.

Check logs:
```bash
docker compose logs -f airflow-scheduler
```

---

## Step 6: Verify Airflow UI

Open your browser:
```
http://localhost:8080
```

Login: **admin / admin**

You should see **taxi_pipeline** in the DAGs list.

If you don't see it:
1. Check `/opt/airflow/dags/taxi_pipeline.py` exists inside the container:
   ```bash
   docker compose exec airflow-webserver ls -la /opt/airflow/dags/
   ```
2. Check for Python syntax errors:
   ```bash
   docker compose exec airflow-webserver python -m py_compile /opt/airflow/dags/taxi_pipeline.py
   ```

---

## Step 7: Verify Environment Variables Inside Container

Run:
```bash
docker compose exec airflow-webserver env | grep POSTGRES
```

Should output:
```
POSTGRES_HOST=host.docker.internal
POSTGRES_PORT=5432
POSTGRES_DB=nyc_taxi
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
```

If any are missing, update `docker-compose.yml` and restart:
```bash
docker compose restart
```

---

## Step 8: Test dbt Connection (Optional But Recommended)

```bash
docker compose exec airflow-webserver dbt debug \
  --project-dir /opt/airflow/dbt/taxi_analytics
```

Should show:
```
Connection test: [ok connection ok]
```

If it fails, check:
1. PostgreSQL is running on Windows
2. `host.docker.internal:5432` is reachable
3. Credentials in `docker-compose.yml` match your Windows PostgreSQL

---

## Step 9: Trigger the DAG

### Option A: Via CLI (Quickest)

```bash
docker compose exec airflow-webserver airflow dags test taxi_pipeline 2024-01-15 \
  --conf '{"year": 2024, "month": 1}'
```

### Option B: Via Web UI

1. Go to **DAGs** → **taxi_pipeline**
2. Click **Trigger DAG** (blue play icon)
3. In **Configuration**, paste:
   ```json
   {
     "year": 2024,
     "month": 1
   }
   ```
4. Click **Trigger**

---

## Step 10: Monitor Execution

### Via Web UI (Best for visual debugging)
- Go to **DAGs** → **taxi_pipeline**
- Click on the run
- See **Graph View** with task status
- Click any task to see logs

### Via Logs
```bash
docker compose logs -f airflow-scheduler
```

### Expected Timeline (for 1 month of data)

| Task | Typical Duration |
|------|------------------|
| ingest | 30–60 sec (download + network) |
| validate_raw | 5–10 sec (quick schema check) |
| spark_transform | 60–120 sec (PySpark processing) |
| load_postgres | 30–45 sec (SQL insert) |
| dbt_run | 60–90 sec (build models) |
| dbt_test | 30–60 sec (run tests) |
| **Total** | **~4–6 minutes** |

---

## Troubleshooting

### DAG shows in UI but won't trigger

**Check:** Airflow scheduler is running
```bash
docker compose ps | grep scheduler
```

Should show `airflow-scheduler` with status `Up`.

If not running:
```bash
docker compose restart airflow-scheduler
```

---

### Task fails immediately with Python import error

**Check:** sys.path and PYTHONPATH

The DAG has:
```python
sys.path.insert(0, '/opt/airflow/src')
```

If this doesn't work, check:
1. Does `/opt/airflow/src` exist in container?
   ```bash
   docker compose exec airflow-webserver ls /opt/airflow/src/
   ```
2. Does `src/ingestion/ingest.py` have `__init__.py`?
   ```bash
   ls -la src/ingestion/__init__.py
   ```

---

### Task fails: "ValidationError in dbt profiles"

**Check:** dbt credentials

dbt uses:
```yaml
host: "{{ env_var('POSTGRES_HOST', 'localhost') }}"
port: "{{ env_var('POSTGRES_PORT', '5432') }}"
```

Verify env vars are set:
```bash
docker compose exec airflow-webserver env | grep POSTGRES
```

---

### PostgreSQL connection refused

**Check:** Windows PostgreSQL is running and accessible from Docker

Test connection from inside container:
```bash
docker compose exec airflow-webserver psql \
  -h host.docker.internal \
  -U postgres \
  -d nyc_taxi \
  -c "SELECT 1;"
```

If this fails:
1. Check Windows PostgreSQL is running
2. Check `POSTGRES_PASSWORD` is correct
3. Check Windows Firewall allows port 5432

---

### dbt models are created but tests fail

**Check:** Data quality

dbt tests validate:
- `not_null` checks
- `unique` checks on trip_id
- Referential integrity (fact → dimensions)

If tests fail:
1. Look at dbt test output in logs
2. Check the rejected records in `data/quarantine/`
3. Update validation rules if necessary

---

## Success Indicators

After a successful run:

### 1. Airflow Web UI
- All tasks are **green** (success)
- DAG shows "Runs" with status "success"

### 2. File System
```bash
# Bronze (raw data)
ls data/bronze/taxi/year=2024/month=01/

# Silver (cleaned data)
ls data/silver/taxi/year=2024/month=01/

# Quarantine (rejected records, if any)
ls data/quarantine/taxi/year=2024/month=01/
```

### 3. PostgreSQL
```bash
psql -h localhost -U postgres -d nyc_taxi -c "
  SELECT COUNT(*) FROM staging.stg_taxi_trips;
  SELECT COUNT(*) FROM analytics.fact_trips;
"
```

Both should have records.

### 4. Power BI (Phase 9)
Once successful, Power BI can connect and build dashboards from `analytics.*` schema.

---

## Next: Process Additional Months

Once January 2024 works, process other months:

```bash
# February
docker compose exec airflow-webserver airflow dags test taxi_pipeline 2024-02-15 \
  --conf '{"year": 2024, "month": 2}'

# March
docker compose exec airflow-webserver airflow dags test taxi_pipeline 2024-03-15 \
  --conf '{"year": 2024, "month": 3}'

# And so on...
```

Each month is **idempotent**:
- Running the same month twice overwrites safely (no duplicates)
- Months are independent (processing Feb doesn't affect Jan)

---

## Optional: Schedule Monthly Runs (Phase 10)

To run automatically every month instead of manually triggering:

Change in `taxi_pipeline.py`:

```python
dag = DAG(
    dag_id="taxi_pipeline",
    schedule_interval="0 1 1 * *",  # 1 AM on the 1st of every month
    ...
)
```

And remove the `--conf` parameter (use `execution_date` instead).

But keep it manual for now while testing.

---

## Support

If anything fails:

1. **Check logs first:**
   ```bash
   docker compose logs -f airflow-scheduler
   docker compose logs -f airflow-webserver
   ```

2. **Ask yourself:**
   - Are all files mounted?
   - Is POSTGRES_PASSWORD set?
   - Is PostgreSQL running on Windows?
   - Are src/ and dbt/ structured correctly?

3. **Rebuild if needed:**
   ```bash
   docker compose down -v
   docker compose build
   docker compose up -d
   ```

---

## Summary

Your Airflow DAG is now:

✓ **Phase 7 Complete**
- Orchestrates all 6 phases
- Parameterized by year/month
- Proper error handling
- XCom for task communication
- dbt integration

✓ **Phase 8 Done** (Docker)
- Everything runs in containers

✓ **Ready for Phase 9** (Power BI)
- Gold layer (analytics schema) ready for BI consumption

🎯 **Next:** Power BI dashboards!
