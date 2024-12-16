select
        sha2(coalesce(nullif(rtrim(upper(cast(ln_ac_nbr as string))), ''), '-1') || '#~!' || coalesce(nullif(rtrim(upper(cast(cst_no as string))), ''), '-1') || '#~!' || coalesce(nullif(rtrim(cast(op_ts as string)), ''), '-1') || '#~!' || coalesce(nullif(rtrim(cast(current_ts as string)), ''), '-1') || '#~!' || coalesce(nullif(rtrim(cast(offset as string)), ''), '-1') || '#~!' || 'mdm', 256) as dv_hkey_lsat_customer_account,
        sha2(coalesce(nullif(rtrim(upper(cast(ln_ac_nbr as string))), ''), '-1') || '#~!' || coalesce(nullif(rtrim(upper(cast(cst_no as string))), ''), '-1') || '#~!' || 'mdm', 256) as dv_hkey_lnk_customer_account,
        sha2(coalesce(nullif(rtrim(cast(br_cd as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(book_date as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(value_date as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(maturity_date as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(amt_financed as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(close_date as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(account_status as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(sub_cd as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(ten_sp_ksp as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(ccy_cd as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(product_code as string)), ''), repeat('0', 16)), 256) as dv_hsh_dif,

        

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
        

        current_ts as dv_kaf_ldt,
        offset as dv_kaf_ofs,
        op_type as dv_cdc_ops,
        op_ts as dv_src_ldt,
        table as dv_src_rec,
        current_timestamp() as dv_ldt
        

    from {source.loan_info}
    where
        op_ts >= date'2024-01-01'

        and ln_ac_nbr is not null
        and cst_no is not null
        