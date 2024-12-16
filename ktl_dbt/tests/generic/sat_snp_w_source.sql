{% test sat_snp_w_source(model, column_name, model_name) %}

    {%- set model_sat = dv_config(model_name) -%}
    {%- set dv_system = var("dv_system") -%}

    {%- set src_hkey_hub = ktl_autovault.render_list_hash_key_hub_component(model_sat) -%}
    {%- set src_dep_keys = ktl_autovault.render_list_source_dependent_key_name(model_sat) -%}
    {%- set src_ldt_keys = ktl_autovault.render_list_source_ldt_key_name(dv_system) -%}
    {%- set ldt_keys = ktl_autovault.render_list_dv_system_ldt_key_name(dv_system) -%}

    {%- set initial_date = var('initial_date', run_started_at.astimezone(modules.pytz.timezone("Asia/Ho_Chi_Minh")).strftime('%Y-%m-%d')) -%}
    {%- set start_date = var('incre_start_date', none) -%}
    {%- set end_date = var('incre_end_date', run_started_at.astimezone(modules.pytz.timezone("Asia/Ho_Chi_Minh")).strftime('%Y-%m-%d')) -%}


        select 
        {{ ktl_autovault.render_hash_key_hub_name(model_sat) }},
        {{ column_name }},
        {{ ktl_autovault.render_hash_diff_name(model_sat) }}
        from (
            select
                a.*,
                row_number() over (
                    partition by {{ column_name }}
                    order by dv_src_ldt desc, dv_kaf_ldt desc, dv_kaf_ofs desc) as row_num
            from (
                select 
                    {{ ktl_autovault.render_hash_key_sat_treatment(model_sat, dv_system) }},
                    {{ ktl_autovault.render_hash_key_hub_treatment(model_sat) }},
                    {{ ktl_autovault.render_hash_diff_treatment(model_sat) }},
                    {% for expr in ktl_autovault.render_list_dv_system_column_treatment(dv_system) -%}
                        {{ expr }} {{- ',' if not loop.last }}
                    {% endfor %}                    
                from 
                    {{ ktl_autovault.render_source_table_name(model_sat) }}
                where 
                1=1  
                {% for expr in src_hkey_hub + src_dep_keys -%}
                    and {{ expr }} is not null
                {% endfor %}

                and {{ src_ldt_keys[0] }} >= {{ ktl_autovault.timestamp(initial_date) }}
                    {# dv_src_ldt >= initial_date #}
            ) a
        )    where row_num = 1
        minus
        select 
        {{ ktl_autovault.render_hash_key_hub_name(model_sat) }},
        {{ column_name }},
        {{ ktl_autovault.render_hash_diff_name(model_sat) }}
        from {{ model }}

{% endtest %}