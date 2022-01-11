COPY INTO {{ params.loading_db }}.{{ params.loading_schema }}.{{ params.table }}_{{ ts_nodash }}
    FROM (
        SELECT {{ params.col_string }} FROM @{{ params.stage_name }}/{{ params.table }}/{{execution_date.year}}/{{execution_date.month}}/{{execution_date.day}}/(pattern => '.*(?i){{ params.table }}\_{{ts_nodash}}\.csv')
        )
file_format = {{ params.loading_db }}.{{ params.loading_schema }}.legacy_etl_csv
on_error='abort_statement'