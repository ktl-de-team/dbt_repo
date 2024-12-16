
    
    

select
    sat_der_customer_corecard as unique_field,
    count(*) as n_records

from integration_dp.sat_der_customer_corecard
where sat_der_customer_corecard is not null
group by sat_der_customer_corecard
having count(*) > 1


