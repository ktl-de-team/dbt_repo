{%-set project = 'KTLMDM' -%}
{%-set product = 'INDIVIDUAL' -%}
{%-set source = 'CRM' -%}

{# Get Config #}
{%- set general_conf = ktl_mdm_utils_get_general_config(project) -%}
{%- set rule_desc_conf = ktl_mdm_utils_get_rule_desc_config(project) -%}
{%- set rule_template_config = ktl_mdm_utils_get_rule_template_config(project) -%}

{%- set metadata_conf = ktl_mdm_utils_metadata_get_metadata_config(project,product,source) -%}
{%- set rule_apply = ktl_mdm_utils_get_rule_field_apply_config(project,product,source,'MATCH') -%}

{# Input Table #}
{%- set validate_tbl_incre = ref('KTLMDM_CRM_INDIVIDUAL_VALIDATE') -%}
{%- set dup_tbl = ref('KTLMDM_CRM_INDIVIDUAL_DUPLICATE') -%}
{%- set auto_match_tbl = ref('KTLMDM_CRM_INDIVIDUAL_AUTO_MATCH') -%}


{{ ktl_mdm_match_matched(general_conf,metadata_conf,rule_apply,validate_tbl_incre,dup_tbl,auto_match_tbl) }}
