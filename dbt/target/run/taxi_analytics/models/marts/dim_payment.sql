
  
    

  create  table "nyc_taxi"."public_analytics"."dim_payment__dbt_tmp"
  
  
    as
  
  (
    -- dbt/taxi_analytics/models/marts/dim_payment.sql
--
-- Static mapping of every KNOWN payment_type code, verified against
-- the official TLC data dictionary during Phase 1 exploration (see
-- docs/data_dictionary.md). Deliberately includes codes not yet
-- observed in the current month's data (e.g. code 6, "Voided trip",
-- had zero rows in June 2026) so the dimension represents the full
-- documented domain, not just what happened to appear in one month.
-- This also means future months can join safely without requiring a
-- dimension reload just because a previously-unseen code shows up.
SELECT
    *
FROM
    (
        VALUES
            (0, 'Flex Fare trip'),
            (1, 'Credit card'),
            (2, 'Cash'),
            (3, 'No charge'),
            (4, 'Dispute'),
            (5, 'Unknown'),
            (6, 'Voided trip')
    ) AS t (payment_type_id, payment_type_name)
  );
  