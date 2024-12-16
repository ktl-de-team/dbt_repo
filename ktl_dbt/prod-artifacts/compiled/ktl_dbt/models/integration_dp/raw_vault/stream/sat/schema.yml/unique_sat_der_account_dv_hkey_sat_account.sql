
    
    

select
    dv_hkey_sat_account as unique_field,
    count(*) as n_records

from integration_dp.sat_der_account
where dv_hkey_sat_account is not null
group by dv_hkey_sat_account
having count(*) > 1


