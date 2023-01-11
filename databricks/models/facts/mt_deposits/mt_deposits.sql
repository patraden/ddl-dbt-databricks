with trades_all as
(
select
  mt4_trades.server srv,
  mt4_trades.day,
  mt4_trades.dt,
  mt4_trades.ticket,
  mt4_trades.login,
  mt4_users.id user_id,
  mt4_trades.symbol,
  mt4_trades.comment,
  mt4_trades.mt_profit,
  mt4_trades.mt_profit_usd,
  mt4_trades.mt_currency tr_currency,
  mt4_users.group user_group,
  mt4_users.status user_status,
  mt4_users.country user_country,
  mt4_trades.comm,
  mt4_trades.timestamp
from {{ ref('deposits__pg_230__all_mt4_trades') }} mt4_trades
left join {{ ref('deposits__pg_230__all_mt4_users_history') }} mt4_users
  on mt4_users.login = mt4_trades.login
  and mt4_users.server = mt4_trades.server
  and mt4_trades.timestamp >= mt4_users.__FROM__
  and mt4_trades.timestamp < mt4_users.__TO__

union all

select
  mt5_deals.server srv,
  mt5_deals.day,
  mt5_deals.dt,
  mt5_deals.ticket,
  mt5_deals.login,
  mt5_users.id user_id,
  mt5_deals.symbol,
  mt5_deals.comment,
  mt5_deals.mt_profit,
  mt5_users.rateprofit * mt5_deals.mt_profit mt_profit_usd,
  mt5_users.currency tr_currency,
  mt5_users.group user_group,
  mt5_users.status user_status,
  mt5_users.country user_country,
  mt5_deals.comm,
  mt5_deals.timestamp
from {{ ref('deposits__pg_230__all_mt5_deals') }} mt5_deals
left join {{ ref('deposits__pg_230__all_mt5_users_history') }} mt5_users
  on mt5_users.login = mt5_deals.login
  and mt5_users.server = mt5_deals.server
  and mt5_deals.timestamp >= mt5_users.__FROM__
  and mt5_deals.timestamp < mt5_users.__TO__
)
select
  trades_all.srv,
  trades_all.day,
  trades_all.dt,
  trades_all.ticket,
  trades_all.login,
  trades_all.user_id,
  trades_all.symbol,
  trades_all.comment,
  trades_all.mt_profit,
  trades_all.mt_profit_usd,
  trades_all.tr_currency,
  trades_all.user_group grp,
  countries.country country,
  groups.currency,
  groups.is_real,
  groups.is_cent,
  trades_all.user_status,
  trades_all.comm,
  case when comments.comm_group is not null then comments.comm_group
       when trades_all.mt_profit > 0 then 'deps'
       when trades_all.mt_profit < 0 then 'withs'
       else 'archive'
  end comm_group,
  trades_all.mt_profit * groups.factor amount,
  case when groups.currency='USD' then trades_all.mt_profit * groups.factor
       else trades_all.mt_profit * groups.factor/currencies.price
  end usd_amount
from trades_all
left join dict.deals_mt4_groups groups
  on groups.group = trades_all.user_group
left join dict.deals_mt_countries countries
  on countries.mt_country = trades_all.user_country
left join dict.deal_currency_rates currencies
  on currencies.currency = groups.currency
  and currencies.date = trades_all.day
left join dict.deals_comment_dict comments
  on comments.comm = trades_all.comm
{% if is_incremental() %}
  where trades_all.timestamp between '{{ var('data_interval_start') }}' and '{{ var('data_interval_end') }}'
{% endif %}