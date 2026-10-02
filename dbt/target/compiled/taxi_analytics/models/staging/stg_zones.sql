-- dbt/taxi_analytics/models/staging/stg_zones.sql
--
-- Staging layer for the taxi zone lookup reference table (loaded via
-- `dbt seed` from dbt/seeds/taxi_zone_lookup.csv). Light renaming to
-- snake_case only, matching the convention used across every staging
-- model in this project.

with source as (
    select * from "nyc_taxi"."public_reference"."taxi_zone_lookup"
)

select
    "LocationID" as location_id,
    "Borough"    as borough,
    "Zone"       as zone,
    "service_zone" as service_zone

from source