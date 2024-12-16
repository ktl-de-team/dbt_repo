with source as (
      select * from source.branch
),
renamed as (
    select
        `BR_CD`,
        `BR_NM`,
        `BR_ADDR`,
        `PARENT_BR_CD`,
        `PARENT_BR_NM`,
        `PARENT_BR_CD_LV1`,
        `PARENT_BR_NM_LV1`,
        `AREA_4_CD`,
        `AREA_4_NM`,
        `AREA_6_CD`,
        `AREA_6_NM`,
        `BR_GRP`,
        `POST_CD`,
        `DISTRICT_CD`,
        `DISTRICT_NM`,
        `PROVINCE_CD`,
        `PROVINCE_NM`,
        `AREA_PO`,
        `AREA_NM_PO`,
        `CNTRY_CD`,
        `BR_OPEN_DT`,
        `table`,
        `scn`,
        `op_type`,
        `op_ts`,
        `current_ts`,
        `row_id`,
        `username`,
        `offset`,
        `partition`

    from source
)
select * from renamed