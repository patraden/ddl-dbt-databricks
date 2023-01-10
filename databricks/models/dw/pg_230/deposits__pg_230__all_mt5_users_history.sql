with mt5_users as
(
    select 'mt5_real_1' server,login,group,country,id,status,timestamp
    from staging.deals__pg_230__fbs_mt5_real_1__mt5_users_history
    union all
    select 'mt5_real_2' server,login,group,country,id,status,timestamp
    from staging.deals__pg_230__fbs_mt5_real_2__mt5_users_history
    union all
    select 'tp_real_1' server,login,group,country,id,status,timestamp
    from staging.deals__pg_230__fbs_tp_real_1__mt5_users_history
    union all
    select 'mt5_real_cry_1' server,login,group,country,id,status,timestamp
    from staging.deals__pg_230__fbs_mt5_real_cry_1__mt5_users_history
    union all
    select 'tp_real_cry_1' server,login,group,country,id,status,timestamp
    from staging.deals__pg_230__tradestone_tp_real_cry_1__mt5_users_history

)
select
    server,login,group,country,id,status,timestamp,
    lag(timestamp, 1, '1999-01-01') over(partition by server, login order by timestamp) __FROM__,
    case when row_number() over (partition by server, login order by timestamp desc) = 1 then cast('2999-01-01' as timestamp) else timestamp end __TO__
from mt5_users