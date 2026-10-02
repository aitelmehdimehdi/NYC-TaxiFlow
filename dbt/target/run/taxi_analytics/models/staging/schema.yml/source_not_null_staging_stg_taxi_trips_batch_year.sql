
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select batch_year
from "nyc_taxi"."staging"."stg_taxi_trips"
where batch_year is null



  
  
      
    ) dbt_internal_test