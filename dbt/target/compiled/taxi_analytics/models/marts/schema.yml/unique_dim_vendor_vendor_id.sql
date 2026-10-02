
    
    

select
    vendor_id as unique_field,
    count(*) as n_records

from "nyc_taxi"."public_analytics"."dim_vendor"
where vendor_id is not null
group by vendor_id
having count(*) > 1


