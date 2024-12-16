select
        sha2(coalesce(nullif(rtrim(upper(cast(customer_no as string))), ''), '-1') || '#~!' || coalesce(nullif(rtrim(cast(op_ts as string)), ''), '-1') || '#~!' || coalesce(nullif(rtrim(cast(current_ts as string)), ''), '-1') || '#~!' || coalesce(nullif(rtrim(cast(offset as string)), ''), '-1') || '#~!' || 'mdm', 256) as dv_hkey_sat_customer_corecard,
        sha2(coalesce(nullif(rtrim(upper(cast(customer_no as string))), ''), '-1') || '#~!' || 'mdm', 256) as dv_hkey_hub_customer,
        sha2(coalesce(nullif(rtrim(cast(card_no as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(card_name as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(card_full_name as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(card_type as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(acct_no as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(addr_line1 as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(addr_line2 as string)), ''), repeat('0', 16)), 256) as dv_hsh_dif,

        

        card_no,
        card_name,
        card_full_name,
        card_type,
        acct_no,
        addr_line1,
        addr_line2,
        

        current_ts as dv_kaf_ldt,
        offset as dv_kaf_ofs,
        op_type as dv_cdc_ops,
        op_ts as dv_src_ldt,
        table as dv_src_rec,
        current_timestamp() as dv_ldt
        

    from {source.customers_corecard}
    where
        op_ts >= date'2024-01-01'

        and customer_no is not null
        