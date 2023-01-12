{% set src_table = 'deals__pg_230__fbs_mt4_real6__mt4_trades' %}
{% set dir = 'pg_230__deals/fbs_mt4_real6__mt4_trades/' %}

{{ config(
        pre_hook=external_table_create_template(
        src_schema='dw',
        src_table=src_table,
        env=var('env'),
        dir=dir
        )
    ) }}
{{ pg_230_mt4_trades_model_template(
    src_schema='dw',
    src_table=src_table,
   ) }}
{% if is_incremental() %}
 and timestamp between '{{ var('data_interval_start') }}' and '{{ var('data_interval_end') }}'
{% endif %}
