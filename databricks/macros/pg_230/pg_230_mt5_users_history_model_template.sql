{% macro pg_230_mt5_users_history_model_template(src_schema, src_table, trg_schema, trg_table) %}
with src as (
select
 distinct
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
 cast(marginlevel as double) as marginlevel,
 xxhash64(
    login, isarchive, group, certserialnumber, rights, registration, lastaccess,
    lastpasschange, name, company, account, country, language, clientid, city,
    state, zipcode, address, phone, email, id, status, comment, color, phonepassword,
    leverage, agent, tradeaccounts, leadcampaign, leadsource, balance, credit,
    interestrate, commissiondaily, commissionmonthly, balanceprevday, balanceprevmonth,
    equityprevday, equityprevmonth, mqid, lastip, apidata, firstname, lastname,
    middlename, timestamp, timestamptrade, equity, margin, marginfree, marginlevel
 ) __HASH__
from {{ source(src_schema, src_table) }}
)
select *, current_timestamp() __UPDATE_TS__
from src
left anti join {{ trg_schema }}.{{ trg_table }} trg
 on src.__HASH__ = trg.__HASH__
{% endmacro %}