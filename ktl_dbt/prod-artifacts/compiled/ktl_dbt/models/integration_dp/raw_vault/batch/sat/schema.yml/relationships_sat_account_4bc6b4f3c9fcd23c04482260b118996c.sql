
    
    

with child as (
    select dv_hkey_hub_account as from_field
    from integration_dp.sat_account
    where dv_hkey_hub_account is not null
),

parent as (
    select dv_hkey_hub_account as to_field
    from integration_dp.hub_account
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


