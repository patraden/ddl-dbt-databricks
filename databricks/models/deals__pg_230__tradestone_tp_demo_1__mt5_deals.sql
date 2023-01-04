{{ config(
        pre_hook="CREATE TABLE if not exists dw.deals__pg_230__tradestone_tp_demo_1__mt5_deals USING parquet LOCATION 'gs://staging-databricks-test/pg_230__deals/tradestone_tp_demo_1__mt5_deals'",
        materialized='incremental',
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='fail',
        unique_key=['deal'],
        schema='staging'
    )
}}
select
 cast(deal as decimal(20,0)) deal,
 timestamp,
 externalid,
 cast(login as decimal(20,0)) login,
 cast(dealer as decimal(20,0)) dealer,
 cast(order as decimal(20,0)) order,
 cast(action as decimal(11,0)) action,
 cast(entry as decimal(11,0)) entry,
 cast(reason as decimal(11,0)) reason,
 cast(digits as decimal(11,0)) digits,
 cast(digitscurrency as decimal(11,0)) digitscurrency,
 contractsize,
 time,
 timemsc,
 symbol,
 price,
 cast(volumeext as decimal(20,0)) volumeext,
 profit,
 storage,
 commission,
 rateprofit,
 ratemargin,
 cast(expertid as decimal(20,0)) expertid,
 cast(positionid as decimal(20,0)) positionid,
 comment,
 profitraw,
 priceposition,
 pricesl,
 pricetp,
 cast(volumeclosedext as decimal(20,0)) volumeclosedext,
 tickvalue,
 ticksize,
 cast(flags as decimal(20,0)) flags,
 gateway,
 pricegateway,
 cast(modifyflags as decimal(11,0)) modifyflags,
 cast(volume as decimal(20,0)) volume,
 cast(volumeclosed as decimal(20,0)) volumeclosed,
 apidata,
 fee,
 marketbid,
 marketask,
 cast(leverage as decimal(11,0)) leverage,
 cast(agent as decimal(20,0)) agent,
 status,
 id,
 group
from {{ source('dw', 'deals__pg_230__tradestone_tp_demo_1__mt5_deals') }}
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where 1 = 1
{% endif %}

