
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pickup_date_key
from "nyc_taxi"."public_analytics"."fact_trips"
where pickup_date_key is null



  
  
      
    ) dbt_internal_test