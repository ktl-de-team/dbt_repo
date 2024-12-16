select
        sha2(coalesce(nullif(rtrim(upper(cast(cst_no as string))), ''), '-1') || '#~!' || coalesce(nullif(rtrim(cast(op_ts as string)), ''), '-1') || '#~!' || coalesce(nullif(rtrim(cast(current_ts as string)), ''), '-1') || '#~!' || coalesce(nullif(rtrim(cast(offset as string)), ''), '-1') || '#~!' || 'mdm', 256) as dv_hkey_sat_customer,
        sha2(coalesce(nullif(rtrim(upper(cast(cst_no as string))), ''), '-1') || '#~!' || 'mdm', 256) as dv_hkey_hub_customer,
        sha2(coalesce(nullif(rtrim(cast(cst_nm as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(cst_full_nm as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(cst_type as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(br_cd as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(addr_pri as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(address_line4 as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(address_line3 as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(create_dt as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(record_stat as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(sex as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(id_number as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(type_of_id as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(date_of_issue as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(place_of_issue as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(phone as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(birth_day as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(income_levcd as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(educ_levcd as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(mail_typcd as string)), ''), repeat('0', 16)) || '#~!' || coalesce(nullif(rtrim(cast(occptn_cd as string)), ''), repeat('0', 16)), 256) as dv_hsh_dif,

        

        cst_nm,
        cst_full_nm,
        cst_type,
        br_cd,
        addr_pri,
        address_line4,
        address_line3,
        create_dt,
        record_stat,
        sex,
        id_number,
        type_of_id,
        date_of_issue,
        place_of_issue,
        phone,
        birth_day,
        income_levcd,
        educ_levcd,
        mail_typcd,
        occptn_cd,
        

        current_ts as dv_kaf_ldt,
        offset as dv_kaf_ofs,
        op_type as dv_cdc_ops,
        op_ts as dv_src_ldt,
        table as dv_src_rec,
        current_timestamp() as dv_ldt
        

    from {source.customers_corebank}
    where
        op_ts >= date'2024-01-01'

        and cst_no is not null
        