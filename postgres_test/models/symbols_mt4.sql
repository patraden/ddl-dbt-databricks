{{ config(materialized='view') }}

select symbol, count(*) qty
from mt4.trades
where open_time < timestamp '{{ var("symbols_mt4_ts") }}'
group by symbol