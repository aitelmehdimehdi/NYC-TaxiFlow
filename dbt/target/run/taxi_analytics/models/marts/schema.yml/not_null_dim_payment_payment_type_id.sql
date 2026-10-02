
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select payment_type_id
from "nyc_taxi"."public_analytics"."dim_payment"
where payment_type_id is null



  
  
      
    ) dbt_internal_test