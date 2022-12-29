{{ config(materialized='view') }}

select symbol, count(*)
from mt5.deals
{% if var("symbols_mt5_ts") is none -%}
where 1 = 0
{%- else -%}
where time < timestamp '{{ var("symbols_mt5_ts") }}'
{%- endif %}
group by symbol
