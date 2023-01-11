with mt5_deals_all_base as
(
    select 'mt5_real_1' server,cast(time as date) day, time dt,deal ticket,login,symbol,comment,profit mt_profit,cast(from_unixtime(timestamp) as timestamp) timestamp,action
    from staging.deals__pg_230__fbs_mt5_real_1__mt5_deals
    union all
    select 'mt5_real_2' server,cast(time as date) day, time dt,deal ticket,login,symbol,comment,profit mt_profit,cast(from_unixtime(timestamp) as timestamp) timestamp,action
    from staging.deals__pg_230__fbs_mt5_real_2__mt5_deals
    union all
    select 'mt5_real_cry_1' server,cast(time as date) day, time dt,deal ticket,login,symbol,comment,profit mt_profit,cast(from_unixtime(timestamp) as timestamp) timestamp,action
    from staging.deals__pg_230__fbs_mt5_real_cry_1__mt5_deals
    union all
    select 'tp_real_1' server,cast(time as date) day, time dt,deal ticket,login,symbol,comment,profit mt_profit,cast(from_unixtime(timestamp) as timestamp) timestamp,action
    from staging.deals__pg_230__fbs_tp_real_1__mt5_deals
    union all
    select 'tp_real_cry_1' server,cast(time as date) day, time dt,deal ticket,login,symbol,comment,profit mt_profit,cast(from_unixtime(timestamp) as timestamp) timestamp,action
    from staging.deals__pg_230__fbs_tp_real_cry_1__mt5_deals
), mt5_deals_all as
(
select
  server,
  ticket,
  login,
  day,
  dt,
  symbol,
  comment,
  mt_profit,
  timestamp,
  {{ mt_deposits__comm_column_macro() }}
from mt5_deals_all_base
where action not in (0,1)
)
select * from mt5_deals_all