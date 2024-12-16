
    
    

select
    dv_hkey_hub_branch as unique_field,
    count(*) as n_records

from integration_dp.hub_branch
where dv_hkey_hub_branch is not null
group by dv_hkey_hub_branch
having count(*) > 1


