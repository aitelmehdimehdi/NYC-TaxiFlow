
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dropoff_zone_id
from "nyc_taxi"."public_analytics"."fact_trips"
where dropoff_zone_id is null



  
  
      
    ) dbt_internal_test