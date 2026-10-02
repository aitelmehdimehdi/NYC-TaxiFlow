
  create view "nyc_taxi"."public_staging"."stg_trips__dbt_tmp"
    
    
  as (
    -- dbt/taxi_analytics/models/staging/stg_trips.sql
--
-- Staging layer: light renaming/casting only, no business logic.
-- Source: staging.stg_taxi_trips (loaded by src/warehouse/load.py).
--
-- IMPORTANT DESIGN NOTE: the raw TLC data has NO natural trip
-- identifier -- there's no trip_id column in the source at all. Since
-- the project's documented grain is "one row = one taxi trip", we
-- need *some* stable identifier for tests (unique, not_null) and for
-- joins downstream. We synthesize one as a hash of fields that,
-- together, should uniquely identify a real trip.
--
-- CONFIRMED IN PRACTICE: an earlier version of this key (vendor +
-- pickup/dropoff timestamps + both locations only) produced 11 hash
-- collisions out of 3.8M rows. Inspecting those collisions showed
-- genuinely DIFFERENT trips -- same vendor, same pickup/dropoff
-- second, same pickup/dropoff zone, but different fare_amount and
-- trip_distance (e.g. two cars from the same vendor completing
-- different trips that happen to start/end at the same intersection
-- in the same second). This is not a data quality defect in the
-- source; it's a limitation of the original key's granularity. Adding
-- fare_amount and trip_distance -- which were confirmed to differ in
-- every observed collision -- resolves it.
--
-- One remaining collision after that fix involved a trip with
-- trip_duration_minutes = 0 (pickup and dropoff at the identical
-- second) where only payment_type and tip_amount differed (0 tip on
-- a Cash row vs a tip on a Credit card row for otherwise identical
-- fare/distance) -- consistent with TLC's own documented caveat that
-- tips are only captured for credit card payments. This looks like the
-- same physical trip re-submitted under two payment records, a known
-- category of TLC data artifact, rather than a random hash collision.
-- Adding payment_type resolves it while keeping both rows (no silent
-- deletion, consistent with this project's data quality philosophy).
-- Zero-duration trips like this one are a good candidate for a future
-- "suspicious" flag, tracked as a follow-up rather than solved here.
--
-- This remains a probabilistic guarantee, not a mathematical one: two
-- trips identical across ALL of these fields would still collide, but
-- at that point they'd be indistinguishable by any field this dataset
-- provides, which is a reasonable place to stop.
WITH
    source AS (
        SELECT
            *
        FROM
            "nyc_taxi"."staging"."stg_taxi_trips"
    )
SELECT
    md5(
        concat_ws(
            '|',
            "VendorID"::text,
            tpep_pickup_datetime::text,
            tpep_dropoff_datetime::text,
            "PULocationID"::text,
            "DOLocationID"::text,
            fare_amount::text,
            trip_distance::text,
            payment_type::text
        )
    ) AS trip_id,
    "VendorID" AS vendor_id,
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    "PULocationID" AS pickup_location_id,
    "DOLocationID" AS dropoff_location_id,
    "RatecodeID" AS ratecode_id,
    payment_type,
    passenger_count,
    trip_distance,
    trip_duration_minutes,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    total_amount,
    pickup_date,
    pickup_hour,
    pickup_weekday,
    pickup_month,
    is_suspicious,
    batch_year,
    batch_month
FROM
    source
  );