-- depends_on: integration_dp.sat_der_customer_corecard
    with
        cte_sat_der_latest_records as (
            select *
            from
                (
                    select
                        *,

                        row_number() over (
                            partition by
                                dv_hkey_hub_customer
                                
                            order by
                                dv_src_ldt desc,dv_kaf_ldt desc,dv_kaf_ofs desc
                        ) as row_num

                    from integration_dp.sat_der_customer_corecard
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
                                dv_hkey_hub_customer
                                
                            order by
                                dv_src_ldt desc,dv_kaf_ldt desc,dv_kaf_ofs desc
                        ) as row_num

                    from integration_dp.sat_snp_customer_corecard
                )
            where row_num = 1
        )

    select
        dv_hkey_sat_customer_corecard,
        dv_hkey_hub_customer,
        dv_hsh_dif,

        

        card_no,
        card_name,
        card_full_name,
        card_type,
        acct_no,
        addr_line1,
        addr_line2,
        

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
                sat_der.dv_hkey_hub_customer = sat_snp.dv_hkey_hub_customer

                

                and sat_der.dv_hsh_dif = sat_snp.dv_hsh_dif
                and lower(sat_der.dv_cdc_ops) not in ('d', 't')
                and lower(sat_snp.dv_cdc_ops) not in ('d', 't')
        )