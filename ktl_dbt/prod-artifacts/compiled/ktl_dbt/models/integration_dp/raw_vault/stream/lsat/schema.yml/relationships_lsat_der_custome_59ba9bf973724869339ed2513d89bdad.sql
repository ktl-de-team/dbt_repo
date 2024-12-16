
    
    

with child as (
    select dv_hkey_lnk_customer_account as from_field
    from integration_dp.lsat_der_customer_account
    where dv_hkey_lnk_customer_account is not null
),

parent as (
    select dv_hkey_lnk_customer_account as to_field
    from integration_dp.lnk_customer_account
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


