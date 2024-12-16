with
    cte_rnk as (
        select
            *,
            row_number() over (
                partition by dv_hkey_hub_account, dv_src_ldt order by dv_ldt
            ) rnk
        from integration_dp.sat_der_account
        where 1 = 1
    ),
    cte_latest_rnk1 as (select * from cte_rnk where rnk = 1),
    cte_new_hsh_dif as (
        select
            *,
            lag(dv_hsh_dif) over (
                partition by dv_hkey_hub_account order by dv_src_ldt
            ) prev_dv_hsh_dif
        from cte_latest_rnk1
    ),
    cte_dim_account as (
        select
            hub.dv_hkey_hub_account sk_account,
            hub.ln_ac_nbr,
            maturity_date,
            amt_financed,
            sat.dv_src_ldt as dv_start_date,
            lead(sat.dv_src_ldt, 1, date '9999-12-31') over (
                partition by sat.dv_hkey_hub_account order by sat.dv_src_ldt
            ) dv_end_date,
            sat.dv_ldt
        from cte_new_hsh_dif sat
        left join
            integration_dp.hub_account hub
            on hub.dv_hkey_hub_account = sat.dv_hkey_hub_account
        where
            1 = 1
            and (prev_dv_hsh_dif <> dv_hsh_dif or prev_dv_hsh_dif is null)
            and sat.dv_cdc_ops <> 'D'
    )
    select * from cte_dim_account