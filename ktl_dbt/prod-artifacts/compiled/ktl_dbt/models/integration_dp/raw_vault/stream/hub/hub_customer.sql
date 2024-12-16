with
        cte_stg_hub as (
            select
                sha2(coalesce(nullif(rtrim(upper(cast(cst_no as string))), ''), '-1') || '#~!' || 'mdm', 256) as dv_hkey_hub_customer,

                cst_no,
                

                current_ts as dv_kaf_ldt,
                offset as dv_kaf_ofs,
                op_type as dv_cdc_ops,
                op_ts as dv_src_ldt,
                table as dv_src_rec,
                current_timestamp() as dv_ldt,
                

                'mdm' as dv_ccd

            from
                {source.customers_corebank}
            where
                1 = 1
                and cst_no is not null
                
        ),

        cte_stg_hub_latest_records as (
            select *
            from
                (
                    select
                        *,

                        row_number() over (
                            partition by dv_hkey_hub_customer
                            order by
                                dv_src_ldt asc,
                                dv_kaf_ldt asc,
                                dv_kaf_ofs asc
                                
                        ) as row_num

                    from cte_stg_hub
                )
            where row_num = 1
        ),

            cte_stg_hub_existed_keys as (
                select dv_hkey_hub_customer
                from cte_stg_hub src
                where
                    exists (
                        select 1
                        from integration_dp.hub_customer tgt
                        where tgt.dv_hkey_hub_customer = src.dv_hkey_hub_customer
                    )
            )

    select
        dv_hkey_hub_customer,

        cst_no,
        

        dv_kaf_ldt,
        dv_kaf_ofs,
        dv_cdc_ops,
        dv_src_ldt,
        dv_src_rec,
        dv_ldt,
        

        dv_ccd

    from cte_stg_hub_latest_records src

        where
            not exists (
                select 1
                from cte_stg_hub_existed_keys tgt
                where tgt.dv_hkey_hub_customer = src.dv_hkey_hub_customer
            )