
{%- set model = dv_config('sat_crm_indi') -%}
{%- set dv_system = var("dv_system") -%}


{{ ktl_autovault.sat_snp_transform(model=model, dv_system=dv_system) }}
