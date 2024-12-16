with
    cte_rnk as (
        select
            *,
            row_number() over (
                partition by dv_hkey_hub_customer, dv_src_ldt order by dv_ldt
            ) rnk
        from integration_dp.sat_der_customer_corecard
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
    cte_dim_customer_corecard as (
        select
            hub.dv_hkey_hub_customer sk_customer,
            hub.cst_no,
            card_no,
            card_name,
            card_full_name,
            card_type,
            acct_no,
            addr_line1,
            addr_line2,
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
            card_name as cn_ho_ten_kh,
            cast(null as string) cn_danh_xung,
            cast(null as string) cn_biet_danh,
            cast(null as string) cn_gioi_tinh,
            cast(null as string) cn_dan_toc,
            cast(null as string) cn_ngay_sinh,
            cast(null as string) cn_cu_tru,
            addr_line1 as cn_dc_thuongtru,
            addr_line2 as cn_dc_tamtru,
            'Vietnam' as cn_quoc_tich,
            cast(null as string) cn_sdt_dd,
            cast(null as string) cn_email,
            cast(null as string) cn_so_cmnd,
            cast(null as string) cn_ngaycap_cmnd,
            cast(null as string) cn_noicap_cmnd,
            cast(null as string) cn_so_cccd,
            cast(null as string) cn_ngaycap_cccd,
            cast(null as string) cn_noicap_cccd,
            cast(null as string) cn_so_pp,
            cast(null as string) cn_ngaycap_pp,
            cast(null as string) cn_noicap_pp,
            cast(null as string) cn_so_mst,
            cast(null as string) cn_ngaycap_mst,
            cast(null as string) cn_noicap_mst,
            cast(null as string) cn_hoc_van,
            cast(null as string) cn_nghenghiep,
            cast(null as string) cn_tt_sohuunha,
            cast(null as string) cn_so_npt,
            cast(null as string) cn_hon_nhan,
            cast(null as string) cn_nguon_tn,
            cast(null as string) cn_so_tn,
            cast(null as string) cn_so_tn_cdt,
            cast(null as string) cn_ngaymo
        from cte_dim_customer_corecard
    )
select *
from cte_final