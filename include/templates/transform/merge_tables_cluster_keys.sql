{% set table_schema = params.table_schema %}
{% set cluster_keys = params.cluster_keys|default([],True) %}
MERGE INTO {{ params.transform_db }}.{{ params.transform_schema }}.{{ params.table }} as dest
USING (   
    SELECT * 
    ,{{ table_schema.get('hash_diff').get('algorithm') }}(
        CONCAT(
        {% for col in table_schema.get('hash_diff').get('columns') -%}
            {%- if not loop.first %}
        ,{%- endif -%}
        {%- set default_val = table_schema.get(col).get('default','missing_value') -%}
                COALESCE({{col}}, '{{default_val}}')
        {%- endfor %}
    )) as hash_diff
    FROM 
    {{ params.loading_db }}.{{ params.loading_schema }}.{{params.table}}_{{ ts_nodash }}
) as stg
ON dest.hash_diff = stg.hash_diff
{%- if cluster_keys %}
{%- for key in cluster_keys %}
AND dest.{{ key }} = stg.{{ key }}
{%- endfor %}
{%- endif %}
WHEN NOT MATCHED THEN
INSERT (
    {%- for name, col_dict in table_schema.items() -%}
    {%- if loop.first %}
    {{ name }}
    {%- else %}
    ,{{ name }}
    {%- endif %}
    {%- endfor %}
) 
VALUES 
(
    {% for name, col_dict in table_schema.items() %}
    {%- if not loop.first %}
    ,{%- endif -%}
    {%- if 'timestamp' in col_dict.get('type') -%} 
        {%- if name == 'execution_date' -%}
            TO_TIMESTAMP_NTZ(stg.{{ name }})
        {%- elif name == 'insert_timestamp' -%}
            TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
        {%- else -%}
            TO_TIMESTAMP_NTZ(stg.{{ name }})
        {%- endif -%}
    {%- else -%}
        {%- if 'default' in col_dict.keys() -%}
            COALESCE(stg.{{ name }}, '{{col_dict.get('default', 'missing_value')}}')
        {%- else -%}
            stg.{{ name }}
        {%- endif -%}
    {%- endif %}
    {%- endfor %}
)
