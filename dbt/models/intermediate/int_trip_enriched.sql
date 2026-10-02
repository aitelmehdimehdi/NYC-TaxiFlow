-- dbt/taxi_analytics/models/intermediate/int_trip_enriched.sql
--
-- Joins stg_trips with stg_zones TWICE: once for the pickup location,
-- once for the dropoff location. This is where the "business logic"
-- of resolving raw LocationIDs into human-readable zone/borough names
-- happens, kept separate from the final fact table so fact_trips.sql
-- stays a simple, readable projection.
WITH
    trips AS (
        SELECT
            *
        FROM
            {{ref ('stg_trips')}}
    ),
    zones AS (
        SELECT
            *
        FROM
            {{ref ('stg_zones')}}
    ),
    enriched AS (
        SELECT
            t.*,
            pu_zone.borough AS pickup_borough,
            pu_zone.zone AS pickup_zone_name,
            pu_zone.service_zone AS pickup_service_zone,
            do_zone.borough AS dropoff_borough,
            do_zone.zone AS dropoff_zone_name,
            do_zone.service_zone AS dropoff_service_zone
        FROM
            trips t
            LEFT JOIN zones pu_zone ON t.pickup_location_id = pu_zone.location_id
            LEFT JOIN zones do_zone ON t.dropoff_location_id = do_zone.location_id
    )
SELECT
    *
FROM
    enriched