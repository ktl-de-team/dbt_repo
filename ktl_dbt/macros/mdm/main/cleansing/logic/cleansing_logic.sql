{# LOGIC FOR ALL RULE CLEANSING #}


{# logic convert string to datetime format #}
{%- macro cleansing_logic_convert_str_to_date(obj, character) -%}
    TO_DATE({{obj}}, '{{character}}') AS {{obj}}
{%- endmacro -%}

{# logic replace character ---> blank #}
{%- macro cleansing_logic_remove_character(obj, character) -%}
    
    {%- set pattern_list = character | replace('[', '') | replace(']', '') -%}

    {% set ns = namespace(replace_stmt=obj) %}
    
    {%- for ele in pattern_list -%}
        {%- set ns.replace_stmt = "REPLACE(" + ns.replace_stmt + ", '" + ele + "', '')" -%}
    {%- endfor -%}
    {%- set finish -%}
        {{ ns.replace_stmt }} AS {{obj}}
    {%- endset -%}
    
    {{ finish }}
{%- endmacro -%}

{# logic replace first [+84,84] --- > 0 #}
{%- macro cleansing_logic_replace_character_84(obj, character = ['+84','84']) -%}
    DECODE(SUBSTR({{obj}},1,3), '{{character[0]}}', '0'||SUBSTR({{obj}},4), DECODE(SUBSTR({{obj}},1,2), '{{character[1]}}', '0'||SUBSTR({{obj}},3), {{obj}}))
{%- endmacro -%}

{# logic replace head phone #}
{%- macro cleansing_logic_replace_head_phone(obj, _old_phone, _new_phone) -%}
    CAST( COALESCE(
            CASE 
                WHEN LENGTH(catalog.{{_old_phone}}) = 4 THEN
                    CONCAT(catalog.{{_new_phone}}, SUBSTR({{obj}}, 5))
                WHEN LENGTH(catalog.{{_old_phone}}) = 3 THEN
                    CONCAT(catalog.{{_new_phone}}, SUBSTR({{obj}}, 4))
            ELSE
                NULL
            END,
            {{obj}} 
    ) as {{ktl_mdm_utils_types_string(255)}} )
{%- endmacro -%}

{# logic coalesce catalog column #}
{%- macro cleansing_logic_coalesce_catalog(obj, _standard_value, _alias) -%}
    COALESCE({{_alias}}.{{_standard_value}}, a.{{obj}}) AS {{obj}}
{%- endmacro -%}
