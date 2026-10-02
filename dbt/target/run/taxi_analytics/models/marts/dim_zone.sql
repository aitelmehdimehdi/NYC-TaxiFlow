
  
    

  create  table "nyc_taxi"."public_analytics"."dim_zone__dbt_tmp"
  
  
    as
  
  (
    -- dbt/taxi_analytics/models/marts/dim_zone.sql
--
-- Clean, business-facing projection of the zone lookup reference data.
SELECT
    location_id,
    borough,
    zone,
    service_zone
FROM
    "nyc_taxi"."public_staging"."stg_zones"
  );
  