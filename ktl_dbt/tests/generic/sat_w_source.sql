{% test sat_w_source(model, column_name, model_name) %}

    {%- set model_sat = dv_config(model_name) -%}
    {%- set dv_system = var("dv_system") -%}

    {%- set src_hkey_hub = ktl_autovault.render_list_hash_key_hub_component(model_sat) -%}
    {%- set hkey_hub_name = ktl_autovault.render_hash_key_hub_name(model_sat) -%}
    {%- set dep_keys = ktl_autovault.render_list_dependent_key_name(model_sat) -%}
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
                    partition by {% for key in [hkey_hub_name] + dep_keys -%}
                            {{ key }} {{- ',' if not loop.last }}
                        {% endfor %}
                    order by
                        {% for key in ldt_keys -%}
                            {{ key }} desc {{- ',' if not loop.last }}
                        {%- endfor %} ) as row_num
            from (
                select 
                    {{ ktl_autovault.render_hash_key_sat_treatment(model_sat, dv_system) }},
                    {{ ktl_autovault.render_hash_key_hub_treatment(model_sat) }},
                    {{ ktl_autovault.render_hash_diff_treatment(model_sat) }},
                    {% for name in ktl_autovault.render_list_dependent_key_name(model_sat) -%}
                        {{ name }},
                    {% endfor %}                    
                    {% for expr in ktl_autovault.render_list_dv_system_column_treatment(dv_system) -%}
                        {{ expr }} {{- ',' if not loop.last }}
                    {% endfor %}                    
                from 
                {% if config.get('materialized') == "streaming" -%}
                    {{ ktl_autovault.render_source_view_name(model_sat) }}
                {%- else -%}
                    {{ ktl_autovault.render_source_table_name(model_sat) }}
                {%- endif %}
                where 
                1=1  
                {% for expr in src_hkey_hub + dep_keys -%}
                    and {{ expr }} is not null
                {% endfor %}             
            ) a
        )    where row_num = 1
        minus
        select 
        {{ ktl_autovault.render_hash_key_hub_name(model_sat) }},
        {{ column_name }},
        {{ ktl_autovault.render_hash_diff_name(model_sat) }}
        from {{ model }}

{% endtest %}