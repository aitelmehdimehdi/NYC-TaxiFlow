# Data Dictionary — NYC Yellow Taxi Trip Records

**Source file explored:** `yellow_tripdata_2026-07.parquet` (downloaded from the official TLC trip data page)

**⚠️ Naming discrepancy found:** the file was downloaded expecting July 2026 data, but the actual `tpep_pickup_datetime` values fall almost entirely within **June 2026** (2026-06-01 to 2026-06-30, with a small tail of dropoffs into 2026-07-01). **This needs to be confirmed** — re-check the exact download link/filename before this data is used as the pipeline's official "2026-06" or "2026-07" partition. Until confirmed, this dictionary treats the file's _content_ as June 2026 data regardless of its filename.

**Row count:** 3,837,248
**Column count:** 21

---

## 1. Schema

| Column                  | dtype          | Nullable?         | Null count       | Notes                                                                                                                                                                                                        |
| ----------------------- | -------------- | ----------------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `VendorID`              | int32          | No                | 0                | See §2 for code mapping                                                                                                                                                                                      |
| `tpep_pickup_datetime`  | datetime64[us] | No                | 0                | Min value (2008-12-31) is a clear data-entry error — see §4                                                                                                                                                  |
| `tpep_dropoff_datetime` | datetime64[us] | No                | 0                |                                                                                                                                                                                                              |
| `passenger_count`       | float64        | **Conditionally** | 1,013,500        | Null **iff** `payment_type == 0` (Flex Fare) — confirmed, see §3                                                                                                                                             |
| `trip_distance`         | float64        | No                | 0                | Max value (311,565 mi) is an outlier — see §4                                                                                                                                                                |
| `RatecodeID`            | float64        | **Conditionally** | 1,013,500        | Same null pattern as `passenger_count`                                                                                                                                                                       |
| `store_and_fwd_flag`    | object (Y/N)   | **Conditionally** | 1,013,500        | Same null pattern                                                                                                                                                                                            |
| `PULocationID`          | int32          | No                | 0                | Joins to `taxi_zone_lookup.csv` for `dim_zone`                                                                                                                                                               |
| `DOLocationID`          | int32          | No                | 0                | Joins to `taxi_zone_lookup.csv` for `dim_zone`                                                                                                                                                               |
| `payment_type`          | int64          | No                | 0                | See §2 for code mapping                                                                                                                                                                                      |
| `fare_amount`           | float64        | No                | 0                | Negative values exist — see §4                                                                                                                                                                               |
| `extra`                 | float64        | No                | 0                |                                                                                                                                                                                                              |
| `mta_tax`               | float64        | No                | 0                |                                                                                                                                                                                                              |
| `tip_amount`            | float64        | No                | 0                |                                                                                                                                                                                                              |
| `tolls_amount`          | float64        | No                | 0                |                                                                                                                                                                                                              |
| `improvement_surcharge` | float64        | No                | 0                |                                                                                                                                                                                                              |
| `total_amount`          | float64        | No                | 0                | Negative values exist — see §4                                                                                                                                                                               |
| `congestion_surcharge`  | float64        | **Conditionally** | 1,013,500        | Same null pattern                                                                                                                                                                                            |
| `Airport_fee`           | float64        | **Conditionally** | 1,013,500        | Note inconsistent casing vs. rest of schema (capital "A") — normalize in Silver                                                                                                                              |
| `cbd_congestion_fee`    | float64        | No                | 0                | New as of 2025 — MTA Congestion Relief Zone charge                                                                                                                                                           |
| `request_source`        | object         | Yes (mostly)      | 2,824,068 (~74%) | **Undocumented column** — not present in official TLC data dictionary as of this writing. Values, when present, not yet characterized. Carry through Bronze/Silver as-is; do not build business logic on it. |

Note: the original project spec assumed ~19 columns based on older TLC documentation. The real file has 21 — schema drift confirmed in practice, exactly why this step matters.

---

## 2. Categorical code mappings (verified against official TLC data dictionary, March 2025)

### `VendorID`

| Code | Meaning                           | Rows in this file |
| ---- | --------------------------------- | ----------------- |
| 1    | Creative Mobile Technologies, LLC | 613,145           |
| 2    | Curb Mobility, LLC                | 3,165,903         |
| 6    | Myle Technologies Inc             | 8,676             |
| 7    | Helix                             | 49,524            |

### `payment_type`

| Code | Meaning        | Rows in this file           |
| ---- | -------------- | --------------------------- |
| 0    | Flex Fare trip | 1,013,500                   |
| 1    | Credit card    | 2,432,871                   |
| 2    | Cash           | 357,804                     |
| 3    | No charge      | 11,205                      |
| 4    | Dispute        | 21,866                      |
| 5    | Unknown        | 2                           |
| 6    | Voided trip    | 0 (not observed this month) |

### `RatecodeID`

| Code   | Meaning                        | Rows in this file |
| ------ | ------------------------------ | ----------------- |
| 1      | Standard rate                  | 2,672,774         |
| 2      | JFK                            | 92,624            |
| 3      | Newark                         | 12,193            |
| 4      | Nassau or Westchester          | 8,814             |
| 5      | Negotiated fare                | 36,252            |
| 6      | Group ride                     | 1                 |
| 99     | Null/unknown                   | 1,090             |
| (null) | Not reported — Flex Fare trips | 1,013,500         |

---

## 3. Confirmed data patterns

**The correlated-null cluster is fully explained by `payment_type == 0` (Flex Fare).**

Verified: filtering to rows where `passenger_count IS NULL` yields exactly 1,013,500 rows, and 100% of them have `payment_type == 0`. The reverse also holds — every `payment_type == 0` row has these fields null. This is a **source-system behavior, not a data quality defect**: Flex Fare trips apparently don't populate `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, or `Airport_fee`.

This is not vendor-specific — Vendors 1, 2, and 6 all appear within the null cluster. One incidental observation: Vendor 7 (Helix) has zero `payment_type == 0` rows in this file (all 49,524 of its rows fall outside the null cluster). Single-month data, not a rule to build logic on yet — worth re-checking once a second month is available.

**Implication for Silver layer:** these five columns should **not** be flagged as data-quality failures when null. The validation logic should treat "null AND payment_type == 0" as expected/valid, and only flag nulls in these columns as suspicious when `payment_type != 0`.

---

## 4. Business rule violations found (invalid vs. suspicious)

Per the project's data quality philosophy (invalid → quarantine, suspicious → flag but keep):

### Invalid (recommend quarantine)

| Rule                                                                               | Rows affected                                                                              |
| ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `fare_amount < 0`                                                                  | 13,648                                                                                     |
| `total_amount < 0`                                                                 | 14,312                                                                                     |
| `tpep_dropoff_datetime < tpep_pickup_datetime`                                     | 1                                                                                          |
| `tpep_pickup_datetime` far outside the file's actual month (e.g. min = 2008-12-31) | at least 1 (the same record as the min value; needs a full count with a date-range filter) |

### Suspicious (flag, do not auto-delete)

| Rule                              | Observation                                                                                                                                                                                       |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `trip_distance` outliers          | max = 311,565 miles — physically impossible, likely GPS/meter error                                                                                                                               |
| `fare_amount` outliers            | max = $7,045                                                                                                                                                                                      |
| `passenger_count` == 0 (not null) | 10,416 rows — distinct from the Flex Fare null cluster; zero passengers is unusual but not impossible (e.g. package delivery has been reported anecdotally in TLC data) — flag, don't auto-reject |

**Not yet checked / to do before finalizing quarantine rules:**

- Exact row count of "pickup datetime outside the plausible month range" (only the min was inspected — there may be more than one 2008-era row, or a symmetric issue at the max end)
- Whether `trip_distance == 0` combined with non-zero `fare_amount` is common (potential meter-only trips)
- Duplicate trip detection — not yet performed; the project spec explicitly warns not to assume duplicate-looking rows are invalid without a defined business rule first

---

## 5. Open items before this dictionary is considered final

1. **Resolve the June vs. July filename discrepancy** — confirm which month this data actually belongs to before it's used to name any Bronze partition.
2. **Characterize `request_source`** — sample the non-null values (~1M rows) to see if a pattern emerges, even though it's undocumented.
3. **Decide the exact quarantine boundary** for outliers (trip_distance, fare_amount) — a hard threshold (e.g. IQR-based or a fixed sanity cap) should be chosen and documented, not left implicit.
4. **Confirm this schema is stable** across at least one more month before hardcoding it into Spark schema definitions (Phase 4) — TLC has changed columns before and may again ("minor changes... to standardize the parquet schema" per their own site).
