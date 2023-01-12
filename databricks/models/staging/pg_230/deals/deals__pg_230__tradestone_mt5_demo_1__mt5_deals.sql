{% set src_table = 'deals__pg_230__tradestone_mt5_demo_1__mt5_deals' %}
{% set dir = 'pg_230__deals/tradestone_mt5_demo_1__mt5_deals/' %}

{{ config(
        pre_hook=external_table_create_template(
        src_schema='dw',
        src_table=src_table,
        env=var('env'),
        dir=dir
        )
    ) }}
{{ pg_230_mt5_deals_model_template(
    src_schema='dw',
    src_table=src_table,
   ) }}
{% if is_incremental() %}
 and timestamp between '{{ var('data_interval_start') }}' and '{{ var('data_interval_end') }}'
{% endif %}
