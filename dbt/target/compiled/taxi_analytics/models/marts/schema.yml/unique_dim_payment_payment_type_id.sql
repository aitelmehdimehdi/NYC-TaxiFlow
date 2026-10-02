
    
    

select
    payment_type_id as unique_field,
    count(*) as n_records

from "nyc_taxi"."public_analytics"."dim_payment"
where payment_type_id is not null
group by payment_type_id
having count(*) > 1


