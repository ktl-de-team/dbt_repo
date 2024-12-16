
    
    

select
    dv_hkey_lnk_customer_account as unique_field,
    count(*) as n_records

from integration_dp.lnk_customer_account
where dv_hkey_lnk_customer_account is not null
group by dv_hkey_lnk_customer_account
having count(*) > 1


