{{ config(
        pre_hook="CREATE TABLE if not exists dw.deals__pg_230__fbs_mt5_real_cry_1__mt5_deals USING parquet LOCATION 'gs://staging-databricks-test/pg_230__deals/fbs_mt5_real_cry_1__mt5_deals'",
        materialized='incremental',
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='fail',
        unique_key=['deal'],
        schema='staging'
    )
}}
select
 deal,
 timestamp,
 externalid,
 login,
 dealer,
 order,
 action,
 entry,
 reason,
 digits,
 digitscurrency,
 contractsize,
 time,
 timemsc,
 symbol,
 price,
 volumeext,
 profit,
 storage,
 commission,
 rateprofit,
 ratemargin,
 expertid,
 positionid,
 comment,
 profitraw,
 priceposition,
 pricesl,
 pricetp,
 volumeclosedext,
 tickvalue,
 ticksize,
 flags,
 gateway,
 pricegateway,
 modifyflags,
 volume,
 volumeclosed,
 apidata,
 fee,
 marketbid,
 marketask,
 leverage,
 agent,
 status,
 id,
 group,
 cast("1970-01-01" as date) _pg_export_dt,
 cast(0 as bigint) __index_level_0__
from {{ source('dw', 'deals__pg_230__fbs_mt5_real_cry_1__mt5_deals') }}
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where 1 = 1
{% endif %}
