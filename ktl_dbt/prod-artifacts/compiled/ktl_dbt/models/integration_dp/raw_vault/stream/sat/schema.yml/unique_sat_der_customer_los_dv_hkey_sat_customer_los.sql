
    
    

select
    dv_hkey_sat_customer_los as unique_field,
    count(*) as n_records

from integration_dp.sat_der_customer_los
where dv_hkey_sat_customer_los is not null
group by dv_hkey_sat_customer_los
having count(*) > 1


