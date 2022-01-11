CREATE SCHEMA IF NOT EXISTS {{ params.transform_db }}.{{ params.transform_schema }};
CREATE TABLE IF NOT EXISTS {{ params.transform_db }}.{{ params.transform_schema }}.{{ params.table }}(
  {%- for name, col_dict in params.table_schema.items() %}
  {{ name }} {{ col_dict.get('type') }}
  {%- if not loop.last -%},{%- endif -%}
  {%- endfor %}
)
