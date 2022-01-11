CREATE OR REPLACE TABLE {{ params.loading_db }}.{{ params.loading_schema }}.{{ params.table }}_{{ ts_nodash }}(
  {%- for name, col_dict in params.table_schema.items() 
          if name not in [
            "insert_timestamp",
            "hash_diff"
          ]
  %}
  {{ name }} {{ col_dict.get('type') }}
  {%- if not loop.last -%},{%- endif -%}
  {%- endfor %}
)
