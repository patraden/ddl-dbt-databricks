with mt4_trades_all_base as
(
    select 'mt4_real1' server,cast(close_time as date) day, close_time dt,ticket,login,symbol,comment,profit mt_profit,profit_usd mt_profit_usd,currency mt_currency,timestamp,cmd
    from staging.deals__pg_230__fbs_mt4_real1__mt4_trades
    union all
    select 'mt4_real2' server,cast(close_time as date) day, close_time dt,ticket,login,symbol,comment,profit mt_profit,profit_usd mt_profit_usd,currency mt_currency,timestamp,cmd
    from staging.deals__pg_230__fbs_mt4_real2__mt4_trades
    union all
    select 'mt4_real3' server,cast(close_time as date) day, close_time dt,ticket,login,symbol,comment,profit mt_profit,profit_usd mt_profit_usd,currency mt_currency,timestamp,cmd
    from staging.deals__pg_230__fbs_mt4_real3__mt4_trades
    union all
    select 'mt4_real4' server,cast(close_time as date) day, close_time dt,ticket,login,symbol,comment,profit mt_profit,profit_usd mt_profit_usd,currency mt_currency,timestamp,cmd
    from staging.deals__pg_230__fbs_mt4_real4__mt4_trades
    union all
    select 'mt4_real5' server,cast(close_time as date) day, close_time dt,ticket,login,symbol,comment,profit mt_profit,profit_usd mt_profit_usd,currency mt_currency,timestamp,cmd
    from staging.deals__pg_230__fbs_mt4_real5__mt4_trades
    union all
    select 'mt4_real6' server,cast(close_time as date) day, close_time dt,ticket,login,symbol,comment,profit mt_profit,profit_usd mt_profit_usd,currency mt_currency,timestamp,cmd
    from staging.deals__pg_230__fbs_mt4_real6__mt4_trades
    union all
    select 'mt4_real7' server,cast(close_time as date) day, close_time dt,ticket,login,symbol,comment,profit mt_profit,profit_usd mt_profit_usd,currency mt_currency,timestamp,cmd
    from staging.deals__pg_230__fbs_mt4_real7__mt4_trades
    union all
    select 'mt4_real8' server,cast(close_time as date) day, close_time dt,ticket,login,symbol,comment,profit mt_profit,profit_usd mt_profit_usd,currency mt_currency,timestamp,cmd
    from staging.deals__pg_230__fbs_mt4_real8__mt4_trades
    union all
    select 'mt4_real9' server,cast(close_time as date) day, close_time dt,ticket,login,symbol,comment,profit mt_profit,profit_usd mt_profit_usd,currency mt_currency,timestamp,cmd
    from staging.deals__pg_230__fbs_mt4_real9__mt4_trades
    union all
    select 'mt4_real10' server,cast(close_time as date) day, close_time dt,ticket,login,symbol,comment,profit mt_profit,profit_usd mt_profit_usd,currency mt_currency,timestamp,cmd
    from staging.deals__pg_230__fbs_mt4_real10__mt4_trades
    union all
    select 'mt4_real11' server,cast(close_time as date) day, close_time dt,ticket,login,symbol,comment,profit mt_profit,profit_usd mt_profit_usd,currency mt_currency,timestamp,cmd
    from staging.deals__pg_230__fbs_mt4_real11__mt4_trades
    union all
    select 'mt4_real12' server,cast(close_time as date) day, close_time dt,ticket,login,symbol,comment,profit mt_profit,profit_usd mt_profit_usd,currency mt_currency,timestamp,cmd
    from staging.deals__pg_230__fbs_mt4_real12__mt4_trades
    union all
    select 'mt4_real13' server,cast(close_time as date) day, close_time dt,ticket,login,symbol,comment,profit mt_profit,profit_usd mt_profit_usd,currency mt_currency,timestamp,cmd
    from staging.deals__pg_230__fbs_mt4_real13__mt4_trades
), mt4_trades_all as
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
  mt_profit_usd,
  mt_currency,
  timestamp,
  {{ mt_deposits__comm_column_macro() }}
from mt4_trades_all_base
where cmd not in (0,1)
)
select * from mt4_trades_all