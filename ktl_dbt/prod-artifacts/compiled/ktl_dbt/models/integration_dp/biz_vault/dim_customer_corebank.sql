with
    cte_rnk as (
        select
            *,
            row_number() over (
                partition by dv_hkey_hub_customer, dv_src_ldt order by dv_ldt
            ) rnk
        from integration_dp.sat_der_customer
        where 1 = 1
    ),
    cte_latest_rnk1 as (select * from cte_rnk where rnk = 1),
    cte_new_hsh_dif as (
        select
            *,
            lag(dv_hsh_dif) over (
                partition by dv_hkey_hub_customer order by dv_src_ldt
            ) prev_dv_hsh_dif
        from cte_latest_rnk1
    ),
    cte_dim_customer_corebank as (
        select
            hub.dv_hkey_hub_customer as sk_customer,
            hub.cst_no,
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
            sat.dv_src_ldt as dv_start_date,
            lead(sat.dv_src_ldt, 1, date '9999-12-31') over (
                partition by sat.dv_hkey_hub_customer order by sat.dv_src_ldt
            ) dv_end_date,
            sat.dv_ldt
        from cte_new_hsh_dif sat
        left join
            integration_dp.hub_customer hub
            on hub.dv_hkey_hub_customer = sat.dv_hkey_hub_customer
        where
            1 = 1
            and (prev_dv_hsh_dif <> dv_hsh_dif or prev_dv_hsh_dif is null)
            and sat.dv_cdc_ops <> 'D'
    ),
    cte_final as (
        select
            dv_ldt as dv_ldt,
            current_timestamp() as cob_date,
            cst_no as cn_ma_kh,
            cst_full_nm as cn_ho_ten_kh,
            case when sex = 'M' then 'ong' else 'ba' end as cn_danh_xung,
            cast(null as string) cn_biet_danh,
            sex as cn_gioi_tinh,
            'Kinh' as cn_dan_toc,
            birth_day as cn_ngay_sinh,
            addr_pri as cn_cu_tru,
            cast(null as string) cn_dc_thuongtru,
            cast(null as string) cn_dc_tamtru,
            'Vietnam' as cn_quoc_tich,
            phone as cn_sdt_dd,
            cast(null as string) cn_email,
            case when type_of_id = 'CMND' then id_number end as cn_so_cmnd,
            case when type_of_id = 'CMND' then date_of_issue end as cn_ngaycap_cmnd,
            case when type_of_id = 'CMND' then place_of_issue end as cn_noicap_cmnd,
            case when type_of_id = 'CCCD' then id_number end as cn_so_cccd,
            case when type_of_id = 'CCCD' then date_of_issue end as cn_ngaycap_cccd,
            case when type_of_id = 'CCCD' then date_of_issue end as cn_noicap_cccd,
            cast(null as string) cn_so_pp,
            cast(null as string) cn_ngaycap_pp,
            cast(null as string) cn_noicap_pp,
            cast(null as string) cn_so_mst,
            cast(null as string) cn_ngaycap_mst,
            cast(null as string) cn_noicap_mst,
            educ_levcd as cn_hoc_van,
            occptn_cd as cn_nghenghiep,
            cast(null as string) cn_tt_sohuunha,
            cast(null as string) cn_so_npt,
            cast(null as string) cn_hon_nhan,
            cast(null as string) cn_nguon_tn,
            cast(null as string) cn_so_tn,
            cast(null as string) cn_so_tn_cdt,
            create_dt as cn_ngaymo
        from cte_dim_customer_corebank
    )
select *
from cte_final