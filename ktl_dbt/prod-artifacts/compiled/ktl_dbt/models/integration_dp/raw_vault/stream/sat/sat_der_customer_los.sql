select
        sha2(coalesce(nullif(rtrim(upper(cast(customer_no as string))), ''), '-1') || '#~!' || coalesce(nullif(rtrim(cast(op_ts as string)), ''), '-1') || '#~!' || coalesce(nullif(rtrim(cast(current_ts as string)), ''), '-1') || '#~!' || coalesce(nullif(rtrim(cast(offset as string)), ''), '-1') || '#~!' || 'mdm', 256) as dv_hkey_sat_customer_los,
        sha2(coalesce(nullif(rtrim(upper(cast(customer_no as string))), ''), '-1') || '#~!' || 'mdm', 256) as dv_hkey_hub_customer,
        sha2(coalesce(nullif(rtrim(cast(customer_name as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(credit_no as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(limit_value as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(income_levcd as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(occptn_cd as string)), ''), repeat('0', 16)), 256) as dv_hsh_dif,

        

        customer_name,
        credit_no,
        limit_value,
        income_levcd,
        occptn_cd,
        

        current_ts as dv_kaf_ldt,
        offset as dv_kaf_ofs,
        op_type as dv_cdc_ops,
        op_ts as dv_src_ldt,
        table as dv_src_rec,
        current_timestamp() as dv_ldt
        

    from {source.customers_los}
    where
        op_ts >= date'2024-01-01'

        and customer_no is not null
        