with mt4_users as
(
    select 'mt4_real1' server,login,group,country,id,status,timestamp
    from staging.deals__pg_230__fbs_mt4_real1__mt4_users_history
    union all
    select 'mt4_real2' server,login,group,country,id,status,timestamp
    from staging.deals__pg_230__fbs_mt4_real2__mt4_users_history
    union all
    select 'mt4_real3' server,login,group,country,id,status,timestamp
    from staging.deals__pg_230__fbs_mt4_real3__mt4_users_history
    union all
    select 'mt4_real4' server,login,group,country,id,status,timestamp
    from staging.deals__pg_230__fbs_mt4_real4__mt4_users_history
    union all
    select 'mt4_real5' server,login,group,country,id,status,timestamp
    from staging.deals__pg_230__fbs_mt4_real5__mt4_users_history
    union all
    select 'mt4_real6' server,login,group,country,id,status,timestamp
    from staging.deals__pg_230__fbs_mt4_real6__mt4_users_history
    union all
    select 'mt4_real7' server,login,group,country,id,status,timestamp
    from staging.deals__pg_230__fbs_mt4_real7__mt4_users_history
    union all
    select 'mt4_real8' server,login,group,country,id,status,timestamp
    from staging.deals__pg_230__fbs_mt4_real8__mt4_users_history
    union all
    select 'mt4_real9' server,login,group,country,id,status,timestamp
    from staging.deals__pg_230__fbs_mt4_real9__mt4_users_history
    union all
    select 'mt4_real10' server,login,group,country,id,status,timestamp
    from staging.deals__pg_230__fbs_mt4_real10__mt4_users_history
    union all
    select 'mt4_real11' server,login,group,country,id,status,timestamp
    from staging.deals__pg_230__fbs_mt4_real11__mt4_users_history
    union all
    select 'mt4_real12' server,login,group,country,id,status,timestamp
    from staging.deals__pg_230__fbs_mt4_real12__mt4_users_history
    union all
    select 'mt4_real13' server,login,group,country,id,status,timestamp
    from staging.deals__pg_230__fbs_mt4_real13__mt4_users_history
)
select
    server,login,group,country,id,status,timestamp,
    lag(timestamp, 1, '1999-01-01') over(partition by server, login order by timestamp) __FROM__,
    case when row_number() over (partition by server, login order by timestamp desc) = 1 then cast('2999-01-01' as timestamp) else timestamp end __TO__
from mt4_users