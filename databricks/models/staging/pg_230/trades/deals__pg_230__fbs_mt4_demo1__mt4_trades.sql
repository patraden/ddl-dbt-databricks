{{ config(
        pre_hook="CREATE TABLE if not exists {{ source('dw', 'deals__pg_230__fbs_mt4_demo1__mt4_trades') }} USING parquet LOCATION 'gs://staging-databricks-{{ var('env') }}/pg_230__deals/fbs_mt4_demo1__mt4_trades/'",
    ) }}
with src as
(
select
 *,
 row_number() over (partition by ticket order by timestamp desc) rn
from {{ source('dw', 'deals__pg_230__fbs_mt4_demo1__mt4_trades') }}
)
select
  cast(ticket as int) ticket,
  cast(login as int) login,
  cast(symbol as string) symbol,
  cast(digits as int) digits,
  cast(cmd as int) cmd,
  cast(volume as int) volume,
  cast(open_time as timestamp) as open_time,
  cast(state as int) state,
  cast(open_price as decimal(38,18)) open_price,
  cast(sl as decimal(38,18)) sl,
  cast(tp as decimal(38,18)) tp,
  cast(close_time as timestamp) as close_time,
  cast(gw_volume as decimal(38,18)) gw_volume,
  cast(expiration as timestamp) as expiration,
  cast(reason as int) reason,
  cast(conv_rate1 as decimal(38,18)) conv_rate1,
  cast(conv_rate2 as decimal(38,18)) conv_rate2,
  cast(commission as decimal(38,18)) commission,
  cast(commission_agent as decimal(38,18)) commission_agent,
  cast(swaps as decimal(38,18)) swaps,
  cast(close_price as decimal(38,18)) close_price,
  cast(profit as decimal(38,18)) profit,
  cast(taxes as decimal(38,18)) taxes,
  cast(magic as int) magic,
  cast(comment as string) as comment,
  cast(gw_order as int) gw_order,
  cast(gw_open_price as int) gw_open_price,
  cast(gw_close_price as int) gw_close_price,
  cast(margin_rate as decimal(38,18)) margin_rate,
  cast(timestamp as timestamp) as timestamp
from src
{% if is_incremental() %}
  where rn = 1 and timestamp between '{{ var('data_interval_start') }}' and '{{ var('data_interval_end') }}'
{% endif %}

