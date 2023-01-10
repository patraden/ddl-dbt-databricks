{% set src_table = 'deals__pg_230__tradestone_tp_real_cry_1__mt5_users' %}
{% set trg_table = 'deals__pg_230__tradestone_tp_real_cry_1__mt5_users_history' %}
{% set dir = 'pg_230__deals/tradestone_tp_real_cry_1__mt5_users/' %}

{{ config(
        pre_hook=external_table_create_template(
        src_schema='dw',
        src_table=src_table,
        env=var('env'),
        dir=dir
        )
    ) }}
{{ pg_230_mt5_users_history_model_template(
    src_schema='dw',
    src_table=src_table,
    trg_schema='staging',
    trg_table=trg_table
   ) }}
{% if is_incremental() %}
 and timestamp between '{{ var('data_interval_start') }}' and '{{ var('data_interval_end') }}'
{% endif %}