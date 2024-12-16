
    
    

select
    dv_hkey_hub_account as unique_field,
    count(*) as n_records

from integration_dp.hub_account
where dv_hkey_hub_account is not null
group by dv_hkey_hub_account
having count(*) > 1


