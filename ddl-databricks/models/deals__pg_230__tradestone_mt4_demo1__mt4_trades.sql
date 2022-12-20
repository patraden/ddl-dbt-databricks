{{ config(
        pre_hook="CREATE TABLE if not exists dw.deals__pg_230__tradestone_mt4_demo1__mt4_trades USING parquet LOCATION 'gs://staging-databricks-test/pg_230__deals/tradestone_mt4_demo1__mt4_trades'",
        materialized='incremental',
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='fail',
        unique_key=['ticket'],
        schema='staging'
    )
}}

select
  cast(ticket as int) ticket,
  cast(login as int) login,
  symbol,
  cast(digits as int) digits,
  cast(cmd as int) cmd,
  cast(volume as int) volume,
  open_time,
  cast(state as int) state,
  cast(open_price as decimal(38,18)) open_price,
  cast(sl as decimal(38,18)) sl,
  cast(tp as decimal(38,18)) tp,
  close_time,
  cast(gw_volume as decimal(38,18)) gw_volume,
  expiration,
  cast(reason as integer) reason,
  cast(conv_rate1 as decimal(38,18)) conv_rate1,
  cast(conv_rate2 as decimal(38,18)) conv_rate2,
  cast(commission as decimal(38,18)) commission,
  cast(commission_agent as decimal(38,18)) commission_agent,
  cast(swaps as decimal(38,18)) swaps,
  cast(close_price as decimal(38,18)) close_price,
  cast(profit as decimal(38,18)) profit,
  cast(taxes as decimal(38,18)) taxes,
  cast(magic as int) magic,
  comment,
  cast(gw_order as int) gw_order,
  cast(gw_open_price as int) gw_open_price,
  cast(gw_close_price as int) gw_close_price,
  cast(margin_rate as decimal(38,18)) margin_rate,
  timestamp,
  group,
  zipcode,
  id,
  status,
  cast(agent_account as int) agent_account,
  isreal,
  iscent,
  cast(rateprofit as decimal(38,18)) rateprofit,
  cast(ratevolume as decimal(38,18)) ratevolume,
  currency,
  cast(profit_usd as decimal(38,18)) profit_usd,
  cast(commission_usd as decimal(38,18)) commission_usd,
  cast(swaps_usd as decimal(38,18)) swaps_usd,
  cast(balance_type as int) balance_type
from {{ source('dw', 'deals__pg_230__tradestone_mt4_demo1__mt4_trades') }}
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
--  where input_file_name() = 'gs://staging-databricks-test/pg_230__deals/fbs_mt4_real5__mt4_trades/part-0-1671097589689.gzip.parquet'
    where 1 = 1
{% endif %}

