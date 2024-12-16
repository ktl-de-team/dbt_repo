
    
    

with child as (
    select dv_hkey_hub_customer as from_field
    from integration_dp.sat_der_customer_los
    where dv_hkey_hub_customer is not null
),

parent as (
    select dv_hkey_hub_customer as to_field
    from integration_dp.hub_customer
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


