-- depends_on: integration_dp.sat_der_account
    with
        cte_sat_der_latest_records as (
            select *
            from
                (
                    select
                        *,

                        row_number() over (
                            partition by
                                dv_hkey_hub_account
                                
                            order by
                                dv_src_ldt desc,dv_kaf_ldt desc,dv_kaf_ofs desc
                        ) as row_num

                    from integration_dp.sat_der_account
                    where
                        1 = 1
                        and dv_src_ldt < date'2024-06-19'
                )
            where row_num = 1
        ),

        cte_sat_snp_latest_records as (
            select *
            from
                (
                    select
                        *,

                        row_number() over (
                            partition by
                                dv_hkey_hub_account
                                
                            order by
                                dv_src_ldt desc,dv_kaf_ldt desc,dv_kaf_ofs desc
                        ) as row_num

                    from integration_dp.sat_snp_account
                )
            where row_num = 1
        )

    select
        dv_hkey_sat_account,
        dv_hkey_hub_account,
        dv_hsh_dif,

        

        br_cd,
        book_date,
        value_date,
        maturity_date,
        cst_no,
        amt_financed,
        close_date,
        account_status,
        sub_cd,
        ten_sp_ksp,
        ccy_cd,
        product_code,
        

        dv_kaf_ldt,
        dv_kaf_ofs,
        dv_cdc_ops,
        dv_src_ldt,
        dv_src_rec,
        dv_ldt
        

    from cte_sat_der_latest_records sat_der
    where
        not exists (
            select 1
            from cte_sat_snp_latest_records sat_snp
            where
                sat_der.dv_hkey_hub_account = sat_snp.dv_hkey_hub_account

                

                and sat_der.dv_hsh_dif = sat_snp.dv_hsh_dif
                and lower(sat_der.dv_cdc_ops) not in ('d', 't')
                and lower(sat_snp.dv_cdc_ops) not in ('d', 't')
        )