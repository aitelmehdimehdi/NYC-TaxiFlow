-- dbt/taxi_analytics/models/marts/fact_trips.sql
--
-- FACT_TRIPS GRAIN: one row represents one taxi trip.
--
-- Foreign keys point to dim_date (via pickup_date_key), dim_zone
-- (via pickup_zone_id / dropoff_zone_id), dim_payment (via
-- payment_type_id), and dim_vendor (via vendor_id). See schema.yml
-- in this folder for the relationship tests enforcing these links.
WITH
    enriched AS (
        SELECT
            *
        FROM
            "nyc_taxi"."public_intermediate"."int_trip_enriched"
    ),
    dates AS (
        SELECT
            date_key,
            full_date
        FROM
            "nyc_taxi"."public_analytics"."dim_date"
    )
SELECT
    e.trip_id,
    e.vendor_id,
    e.pickup_datetime,
    e.dropoff_datetime,
    d.date_key AS pickup_date_key,
    e.pickup_location_id AS pickup_zone_id,
    e.dropoff_location_id AS dropoff_zone_id,
    e.payment_type AS payment_type_id,
    e.passenger_count,
    e.trip_distance,
    e.trip_duration_minutes,
    e.fare_amount,
    e.extra,
    e.mta_tax,
    e.tip_amount,
    e.tolls_amount,
    e.improvement_surcharge,
    e.congestion_surcharge,
    e.airport_fee,
    e.cbd_congestion_fee,
    e.total_amount,
    e.is_suspicious,
    e.batch_year,
    e.batch_month
FROM
    enriched e
    LEFT JOIN dates d ON e.pickup_date = d.full_date