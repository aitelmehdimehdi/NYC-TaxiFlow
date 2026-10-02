
    
    

select
    location_id as unique_field,
    count(*) as n_records

from "nyc_taxi"."public_staging"."stg_zones"
where location_id is not null
group by location_id
having count(*) > 1


