{{ config(
        pre_hook="CREATE TABLE if not exists {{ source('dw', 'deals__pg_230__fbs_tp_real_cry_1__mt5_deals') }} USING parquet LOCATION 'gs://staging-databricks-{{ var('env') }}/pg_230__deals/fbs_tp_real_cry_1__mt5_deals/'",
    ) }}
select
 cast(deal as decimal(20,0)) as deal,
 cast(timestamp as long) as timestamp,
 cast(externalid as string) as externalid,
 cast(login as decimal(20,0)) as login,
 cast(dealer as decimal(20,0)) as dealer,
 cast(order as decimal(20,0)) as order,
 cast(action as decimal(11,0)) as action,
 cast(entry as decimal(11,0)) as entry,
 cast(reason as decimal(11,0)) as reason,
 cast(digits as decimal(11,0)) as digits,
 cast(digitscurrency as decimal(11,0)) as digitscurrency,
 cast(contractsize as double) as contractsize,
 cast(time as timestamp) as time,
 cast(timemsc as timestamp) as timemsc,
 cast(symbol as string) as symbol,
 cast(price as double) as price,
 cast(volumeext as decimal(20,0)) as volumeext,
 cast(profit as double) as profit,
 cast(storage as double) as storage,
 cast(commission as double) as commission,
 cast(rateprofit as double) as rateprofit,
 cast(ratemargin as double) as ratemargin,
 cast(expertid as decimal(20,0)) as expertid,
 cast(positionid as decimal(20,0)) as positionid,
 cast(comment as string) as comment,
 cast(profitraw as double) as profitraw,
 cast(priceposition as double) as priceposition,
 cast(pricesl as double) as pricesl,
 cast(pricetp as double) as pricetp,
 cast(volumeclosedext as decimal(20,0)) as volumeclosedext,
 cast(tickvalue as double) as tickvalue,
 cast(ticksize as double) as ticksize,
 cast(flags as decimal(20,0)) as flags,
 cast(gateway as string) as gateway,
 cast(pricegateway as double) as pricegateway,
 cast(modifyflags as decimal(11,0)) as modifyflags,
 cast(volume as decimal(20,0)) as volume,
 cast(volumeclosed as decimal(20,0)) as volumeclosed,
 cast(apidata as string) as apidata,
 cast(fee as double) as fee,
 cast(marketbid as double) as marketbid,
 cast(marketask as double) as marketask,
 cast(leverage as decimal(11,0)) as leverage,
 cast(agent as decimal(20,0)) as agent,
 cast(status as string) as status,
 cast(id as string) as id,
 cast(group as string) as group
from {{ source('dw', 'deals__pg_230__fbs_tp_real_cry_1__mt5_deals') }}
{% if is_incremental() %}
  where timestamp between '{{ var('data_interval_start') }}' and '{{ var('data_interval_end') }}'
{% endif %}