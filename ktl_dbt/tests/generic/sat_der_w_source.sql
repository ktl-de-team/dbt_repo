{% test sat_der_w_source(model, column_name, model_name) %}

    {%- set model_sat_der = dv_config(model_name) -%}
    {%- set dv_system = var("dv_system") -%}

    {%- set src_hkey_hub = ktl_autovault.render_list_hash_key_hub_component(model_sat_der) -%}
    {%- set src_dep_keys = ktl_autovault.render_list_source_dependent_key_name(model_sat_der) -%}
    {%- set src_ldt_keys = ktl_autovault.render_list_source_ldt_key_name(dv_system) -%}
    {%- set ldt_keys = ktl_autovault.render_list_dv_system_ldt_key_name(dv_system) -%}

    {%- set initial_date = var('initial_date', run_started_at.astimezone(modules.pytz.timezone("Asia/Ho_Chi_Minh")).strftime('%Y-%m-%d')) -%}


        select 
        {{ ktl_autovault.render_hash_key_hub_name(model_sat_der) }},
        {{ column_name }},
        {{ ktl_autovault.render_hash_diff_name(model_sat_der) }}
        from (
                select 
                    {{ ktl_autovault.render_hash_key_sat_treatment(model_sat_der, dv_system) }},
                    {{ ktl_autovault.render_hash_key_hub_treatment(model_sat_der) }},
                    {{ ktl_autovault.render_hash_diff_treatment(model_sat_der) }},
                    {% for expr in ktl_autovault.render_list_dv_system_column_treatment(dv_system) -%}
                        {{ expr }} {{- ',' if not loop.last }}
                    {% endfor %}                    
                from 
                {% if config.get('materialized') == "streaming" -%}
                    {{ ktl_autovault.render_source_view_name(model_sat_der) }}
                {%- else -%}
                    {{ ktl_autovault.render_source_table_name(model_sat_der) }}
                {%- endif %}
                where 
                1=1  
                {% for expr in src_hkey_hub + src_dep_keys -%}
                    and {{ expr }} is not null
                {% endfor %}

                and {{ src_ldt_keys[0] }} >= {{ ktl_autovault.timestamp(initial_date) }}
                {# dv_src_dt > initial date #}
                {% if is_incremental() -%}
                    and {{ src_ldt_keys[0] }} > coalesce((select max({{ ldt_keys[0] }}) from {{ this }}), {{ ktl_autovault.timestamp('1900-01-01') }})
                {%- endif %}        
                {# nếu load incremental thì lấy từ source thời điểm mới nhất của sat der #}
            ) 
        minus
        select 
        {{ ktl_autovault.render_hash_key_hub_name(model_sat_der) }},
        {{ column_name }},
        {{ ktl_autovault.render_hash_diff_name(model_sat_der) }}
        from {{ model }}

{% endtest %}