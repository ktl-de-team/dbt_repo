{%-set project = 'KTLMDM' -%}
{%-set product = 'INDIVIDUAL' -%}
{%-set source = 'CRM' -%}

{%- set general_conf = ktl_mdm_utils_get_general_config(project) -%}
{%- set metadata_conf = ktl_mdm_utils_metadata_get_metadata_config(project,product,source) -%}
{%- set rule_desc_conf = ktl_mdm_utils_get_rule_desc_config(project) -%}
{%- set rule_template_config = ktl_mdm_utils_get_rule_template_config(project) -%}

{%- set rule_apply = ktl_mdm_utils_get_rule_field_apply_config(project,product,source,'VALIDATE') -%}

{%- set cleansing_tbl = ref('KTLMDM_CRM_INDIVIDUAL_CLEANSING') %}
{%- set invalid_tbl = ref('KTLMDM_CRM_INDIVIDUAL_INVALID') %}


{{ ktl_mdm_validate_validate(general_conf,metadata_conf,rule_apply,cleansing_tbl,invalid_tbl) }}

