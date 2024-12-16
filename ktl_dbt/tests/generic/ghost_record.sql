{% test ghost_record(model, column_name) %}

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

    select * from (
    SELECT 
    CASE WHEN cnt = 1 THEN 1 ELSE null END AS test
    {# nếu có số dòng có giá trị là ghost record là 1 thì trả về 1, nếu không thì trả về null #}
    {# cte table này chỉ có hoặc là 1 dòng (test = 1 hoặc test null) #}
    FROM (
        select count(*) as cnt from {{ model }} where {{ column_name }} = cast(repeat('0', {{ length }}) as string )
        {# đếm só dòng có giá trị là ghost_record #}
    )
    ) where test is null
    {# select test is null, nếu test null thì ghost record sẽ đếm 1 (!0) và báo về là test fail
        nếu test not null thì ghost record sẽ đếm 0 và test ok #}

{% endtest %}

