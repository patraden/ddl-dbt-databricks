{% macro external_table_create_template(src_schema, src_table, env, dir) %}
CREATE TABLE IF NOT EXISTS {{ source(src_schema, src_table) }} USING parquet LOCATION 'gs://staging-databricks-{{ env }}/{{ dir }}'
{% endmacro %}