{{ config(
        pre_hook="CREATE TABLE if not exists {{ source('dw', 'deals__pg_230__fbs_tp_real_1__mt5_users') }} USING parquet LOCATION 'gs://staging-databricks-{{ var('env') }}/pg_230__deals/fbs_tp_real_1__mt5_users/'",
    ) }}
with src as
(
select
 *,
 row_number() over (partition by login order by timestamp desc) rn
from {{ source('dw', 'deals__pg_230__fbs_tp_real_1__mt5_users') }}
)
select
 cast(login as decimal(20,0)) as login,
 cast(isarchive as boolean) as isarchive,
 cast(group as string) as group,
 cast(certserialnumber as decimal(20,0)) as certserialnumber,
 cast(rights as decimal(20,0)) as rights,
 cast(registration as timestamp) as registration,
 cast(lastaccess as timestamp) as lastaccess,
 cast(lastpasschange as timestamp) as lastpasschange,
 cast(name as string) as name,
 cast(company as string) as company,
 cast(account as string) as account,
 cast(country as string) as country,
 cast(language as decimal(11,0)) as language,
 cast(clientid as decimal(20,0)) as clientid,
 cast(city as string) as city,
 cast(state as string) as state,
 cast(zipcode as string) as zipcode,
 cast(address as string) as address,
 cast(phone as string) as phone,
 cast(email as string) as email,
 cast(id as string) as id,
 cast(status as string) as status,
 cast(comment as string) as comment,
 cast(color as decimal(11,0)) as color,
 cast(phonepassword as string) as phonepassword,
 cast(leverage as decimal(11,0)) as leverage,
 cast(agent as decimal(20,0)) as agent,
 cast(tradeaccounts as string) as tradeaccounts,
 cast(leadcampaign as string) as leadcampaign,
 cast(leadsource as string) as leadsource,
 cast(balance as double) as balance,
 cast(credit as double) as credit,
 cast(interestrate as double) as interestrate,
 cast(commissiondaily as double) as commissiondaily,
 cast(commissionmonthly as double) as commissionmonthly,
 cast(balanceprevday as double) as balanceprevday,
 cast(balanceprevmonth as double) as balanceprevmonth,
 cast(equityprevday as double) as equityprevday,
 cast(equityprevmonth as double) as equityprevmonth,
 cast(mqid as string) as mqid,
 cast(lastip as string) as lastip,
 cast(apidata as string) as apidata,
 cast(firstname as string) as firstname,
 cast(lastname as string) as lastname,
 cast(middlename as string) as middlename,
 cast(timestamp as timestamp) as timestamp,
 cast(timestamptrade as timestamp) as timestamptrade,
 cast(equity as double) as equity,
 cast(margin as double) as margin,
 cast(marginfree as double) as marginfree,
 cast(marginlevel as double) as marginlevel
from src
{% if is_incremental() %}
  where rn = 1 and timestamp between '{{ var('data_interval_start') }}' and '{{ var('data_interval_end') }}'
{% endif %}

