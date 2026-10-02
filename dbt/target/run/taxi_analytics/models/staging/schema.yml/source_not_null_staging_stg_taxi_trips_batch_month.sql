
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select batch_month
from "nyc_taxi"."staging"."stg_taxi_trips"
where batch_month is null



  
  
      
    ) dbt_internal_test