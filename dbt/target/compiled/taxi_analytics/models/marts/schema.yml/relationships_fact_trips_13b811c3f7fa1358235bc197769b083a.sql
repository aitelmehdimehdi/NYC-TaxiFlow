
    
    

with child as (
    select dropoff_zone_id as from_field
    from "nyc_taxi"."public_analytics"."fact_trips"
    where dropoff_zone_id is not null
),

parent as (
    select location_id as to_field
    from "nyc_taxi"."public_analytics"."dim_zone"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


