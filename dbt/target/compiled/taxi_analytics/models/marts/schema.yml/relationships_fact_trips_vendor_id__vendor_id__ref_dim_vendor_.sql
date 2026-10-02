
    
    

with child as (
    select vendor_id as from_field
    from "nyc_taxi"."public_analytics"."fact_trips"
    where vendor_id is not null
),

parent as (
    select vendor_id as to_field
    from "nyc_taxi"."public_analytics"."dim_vendor"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


