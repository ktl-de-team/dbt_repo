
    
    

select
    dv_hkey_hub_customer as unique_field,
    count(*) as n_records

from integration_dp.hub_customer
where dv_hkey_hub_customer is not null
group by dv_hkey_hub_customer
having count(*) > 1


