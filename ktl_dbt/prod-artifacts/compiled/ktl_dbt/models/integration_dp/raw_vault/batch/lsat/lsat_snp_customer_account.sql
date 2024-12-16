-- depends_on: integration_dp.lsat_der_customer_account
    with
        cte_lsat_der_latest_records as (
            select *
            from
                (
                    select
                        *,

                        row_number() over (
                            partition by
                                dv_hkey_lnk_customer_account
                                
                            order by
                                dv_src_ldt desc,dv_kaf_ldt desc,dv_kaf_ofs desc
                        ) as row_num

                    from integration_dp.lsat_der_customer_account
                    where
                        1 = 1
                        and dv_src_ldt < date'2024-06-19'
                )
            where row_num = 1
        ),

        cte_lsat_snp_latest_records as (
            select *
            from
                (
                    select
                        *,

                        row_number() over (
                            partition by
                                dv_hkey_lnk_customer_account
                                
                            order by
                                dv_src_ldt desc,dv_kaf_ldt desc,dv_kaf_ofs desc
                        ) as row_num

                    from integration_dp.lsat_snp_customer_account
                )
            where row_num = 1
        )

    select
        dv_hkey_lsat_customer_account,
        dv_hkey_lnk_customer_account,
        dv_hsh_dif,

        

        br_cd,
        book_date,
        value_date,
        maturity_date,
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
        

    from cte_lsat_der_latest_records lsat_der
    where
        not exists (
            select 1
            from cte_lsat_snp_latest_records lsat_snp
            where
                lsat_der.dv_hkey_lnk_customer_account = lsat_snp.dv_hkey_lnk_customer_account

                

                and lsat_der.dv_hsh_dif = lsat_snp.dv_hsh_dif
                and lower(lsat_der.dv_cdc_ops) not in ('d', 't')
                and lower(lsat_snp.dv_cdc_ops) not in ('d', 't')
        )