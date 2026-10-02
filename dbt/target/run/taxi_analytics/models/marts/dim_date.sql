
  
    

  create  table "nyc_taxi"."public_analytics"."dim_date__dbt_tmp"
  
  
    as
  
  (
    -- dbt/taxi_analytics/models/marts/dim_date.sql
--
-- Generated dynamically from the actual min/max pickup_date found in
-- stg_trips (with a small buffer on each side), rather than a
-- hardcoded year. This means adding a new month of trip data later
-- automatically extends dim_date on the next `dbt run`, with no
-- manual edits needed here.
WITH
    date_bounds AS (
        SELECT
            min(pickup_date) - interval '3 days' AS start_date,
            max(pickup_date) + interval '3 days' AS end_date
        FROM
            "nyc_taxi"."public_staging"."stg_trips"
    ),
    date_spine AS (
        SELECT
            generate_series(
                (
                    SELECT
                        start_date
                    FROM
                        date_bounds
                ),
                (
                    SELECT
                        end_date
                    FROM
                        date_bounds
                ),
                interval '1 day'
            )::date AS full_date
    )
SELECT
    to_char (full_date, 'YYYYMMDD')::int AS date_key,
    full_date,
    extract (
        year
        FROM
            full_date
    )::int AS year,
    extract (
        quarter
        FROM
            full_date
    )::int AS quarter,
    extract (
        month
        FROM
            full_date
    )::int AS month,
    to_char (full_date, 'Month') AS month_name,
    extract (
        week
        FROM
            full_date
    )::int AS week,
    extract (
        day
        FROM
            full_date
    )::int AS day,
    to_char (full_date, 'Day') AS day_name,
    extract (
        isodow
        FROM
            full_date
    ) IN (6, 7) AS is_weekend
FROM
    date_spine
  );
  