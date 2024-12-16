
    
    

select
    dv_hkey_sat_customer as unique_field,
    count(*) as n_records

from integration_dp.sat_der_customer
where dv_hkey_sat_customer is not null
group by dv_hkey_sat_customer
having count(*) > 1


