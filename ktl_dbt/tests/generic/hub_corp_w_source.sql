{% test hub_corp_w_source(model, column_name, model_name_core, model_name_card) %}

    {%- set model_hub_core = dv_config(model_name_core) -%}
    {%- set model_hub_card = dv_config(model_name_card) -%}

    {%- set dv_system = var("dv_system") -%}

    {%- set method = var('dv_hash_method', "sha256") -%}

    {%- if method == 'md5' -%}
        {%- set length = 32 -%}
    {%- elif method == 'sha1' -%}
        {%- set length = 40 -%}
    {%- elif method == 'sha256' -%}
        {%- set length = 64 -%}
    {%- elif method == 'sha384' -%}
        {%- set length = 96 -%}
    {%- elif method == 'sha512' -%}
        {%- set length = 128 -%}
    {%- endif -%}

    {%- set src_ldt_keys = ktl_autovault.render_list_source_ldt_key_name(dv_system) -%}
    {%- set ldt_keys = ktl_autovault.render_list_dv_system_ldt_key_name(dv_system) -%}
    {%- set src_ldt_keys = ktl_autovault.render_list_source_ldt_key_name(dv_system) -%}

    {%- set initial_date = var('initial_date', run_started_at.astimezone(modules.pytz.timezone("Asia/Ho_Chi_Minh")).strftime('%Y-%m-%d')) -%}
    {%- set start_date = var('incre_start_date', none) -%}
    {%- set end_date = var('incre_end_date', run_started_at.astimezone(modules.pytz.timezone("Asia/Ho_Chi_Minh")).strftime('%Y-%m-%d')) -%}

    select source.{{ column_name }} as hub_redundant
    from (
        select {{ ktl_autovault.render_hash_key_hub_treatment(model_hub_core) }}
        from 
            (select 
            distinct  {% for expr in ktl_autovault.render_list_hash_key_hub_component(model_hub_core) -%}
                    {{ expr }} {{- ',' if not loop.last }}
                {% endfor %}
            from                 
                {% if config.get('materialized') == "streaming" -%}
                    {{ ktl_autovault.render_source_view_name(model_hub_core) }}
                {%- else -%}
                    {{ ktl_autovault.render_source_table_name(model_hub_core) }}
                {%- endif %}
            where
                1 = 1
                {% for expr in ktl_autovault.render_list_hash_key_hub_component(model_hub_core) -%}
                    and {{ expr }} is not null
                {% endfor %}
                
                {%- if model.get('filter', None) -%}
                    and {{ model.get('filter') }}
                {% endif -%}
                )
        union all
        select {{ ktl_autovault.render_hash_key_hub_treatment(model_hub_card) }}
        from 
            (select 
            distinct  {% for expr in ktl_autovault.render_list_hash_key_hub_component(model_hub_card) -%}
                    {{ expr }} {{- ',' if not loop.last }}
                {% endfor %}
            from                 
                {% if config.get('materialized') == "streaming" -%}
                    {{ ktl_autovault.render_source_view_name(model_hub_card) }}
                {%- else -%}
                    {{ ktl_autovault.render_source_table_name(model_hub_card) }}
                {%- endif %}
            where
                1 = 1
                {% for expr in ktl_autovault.render_list_hash_key_hub_component(model_hub_card) -%}
                    and {{ expr }} is not null
                {% endfor %}
                
                {%- if model.get('filter', None) -%}
                    and {{ model.get('filter') }}
                {% endif -%}     
                )
                ) source
    MINUS (SELECT {{ column_name }}  FROM {{ model }})
    {# source có hub không có #}
    union all 
    select {{ column_name }} as source_redundant
    from (select * 
        from {{ model}} 
        where {{ column_name}} <> cast(repeat('0', {{ length }}) as string )
        {# loại ghost record #}
        )
    MINUS(
    select {{ ktl_autovault.render_hash_key_hub_treatment(model_hub_core) }}
        from 
        (
        select 
            distinct  {% for expr in ktl_autovault.render_list_hash_key_hub_component(model_hub_core) -%}
                    {{ expr }} {{- ',' if not loop.last }}
                {% endfor %}
            from                 
                {% if config.get('materialized') == "streaming" -%}
                    {{ ktl_autovault.render_source_view_name(model_hub_core) }}
                {%- else -%}
                    {{ ktl_autovault.render_source_table_name(model_hub_core) }}
                {%- endif %}
            where
                1 = 1
                {% for expr in ktl_autovault.render_list_hash_key_hub_component(model_hub_core) -%}
                    and {{ expr }} is not null
                {% endfor %}
                
                {%- if model.get('filter', None) -%}
                    and {{ model.get('filter') }}
                {% endif -%}
                )
        union all
        select {{ ktl_autovault.render_hash_key_hub_treatment(model_hub_card) }}
        from 
            (select 
            distinct  {% for expr in ktl_autovault.render_list_hash_key_hub_component(model_hub_card) -%}
                    {{ expr }} {{- ',' if not loop.last }}
                {% endfor %}
            from                 
                {% if config.get('materialized') == "streaming" -%}
                    {{ ktl_autovault.render_source_view_name(model_hub_card) }}
                {%- else -%}
                    {{ ktl_autovault.render_source_table_name(model_hub_card) }}
                {%- endif %}
            where
                1 = 1
                {% for expr in ktl_autovault.render_list_hash_key_hub_component(model_hub_card) -%}
                    and {{ expr }} is not null
                {% endfor %}
                
                {%- if model.get('filter', None) -%}
                    and {{ model.get('filter') }}
                {% endif -%}
                )
                )  
    {# hub có source không có #}
{% endtest %}
