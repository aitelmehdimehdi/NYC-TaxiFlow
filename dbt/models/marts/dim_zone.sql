-- dbt/taxi_analytics/models/marts/dim_zone.sql
--
-- Clean, business-facing projection of the zone lookup reference data.
SELECT
    location_id,
    borough,
    zone,
    service_zone
FROM
    {{ref ('stg_zones')}}