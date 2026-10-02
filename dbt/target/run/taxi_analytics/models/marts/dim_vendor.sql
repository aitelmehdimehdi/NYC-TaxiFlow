
  
    

  create  table "nyc_taxi"."public_analytics"."dim_vendor__dbt_tmp"
  
  
    as
  
  (
    -- dbt/taxi_analytics/models/marts/dim_vendor.sql
--
-- Static mapping of known vendor_id codes, verified against the
-- official TLC data dictionary during Phase 1 exploration. TLC has
-- added vendors over time (6 and 7 are relatively recent additions),
-- so this list should be re-verified if a future month's data
-- contains a vendor_id not covered here.
SELECT
    *
FROM
    (
        VALUES
            (1, 'Creative Mobile Technologies, LLC'),
            (2, 'Curb Mobility, LLC'),
            (6, 'Myle Technologies Inc'),
            (7, 'Helix')
    ) AS t (vendor_id, vendor_name)
  );
  