{{ config(
        pre_hook="CREATE TABLE if not exists dw.deals__pg_230__fbs_mt4_real12__mt4_users USING parquet LOCATION 'gs://staging-databricks-prod/pg_230__deals/fbs_mt4_real12__mt4_users/'",
        materialized='incremental',
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='append_new_columns',
        unique_key=['login'],
        schema='staging'
    )
}}

select
 cast(login as int) as login,
 cast(group as string) as group,
 cast(password as string) as password,
 cast(enable as int) as enable,
 cast(enable_change_password as int) as enable_change_password,
 cast(enable_read_only as int) as enable_read_only,
 cast(enable_otp as int) as enable_otp,
 cast(password_investor as string) as password_investor,
 cast(password_phone as string) as password_phone,
 cast(name as string) as name,
 cast(country as string) as country,
 cast(city as string) as city,
 cast(state as string) as state,
 cast(enable_change_pass as int) as enable_change_pass,
 cast(enable_readonly as int) as enable_readonly,
 cast(zipcode as string) as zipcode,
 cast(address as string) as address,
 cast(lead_source as string) as lead_source,
 cast(phone as string) as phone,
 cast(email as string) as email,
 cast(comment as string) as comment,
 cast(id as string) as id,
 cast(status as string) as status,
 cast(regdate as timestamp) as regdate,
 cast(lastdate as timestamp) as lastdate,
 cast(leverage as int) as leverage,
 cast(agent_account as int) as agent_account,
 cast(timestamp as timestamp) as timestamp,
 cast(last_ip as int) as last_ip,
 cast(balance as decimal(38,18)) as balance,
 cast(prevmonthbalance as decimal(38,18)) as prevmonthbalance,
 cast(prevbalance as decimal(38,18)) as prevbalance,
 cast(credit as decimal(38,18)) as credit,
 cast(interestrate as decimal(38,18)) as interestrate,
 cast(taxes as double) as taxes,
 cast(prevmonthequity as double) as prevmonthequity,
 cast(prevequity as decimal(38,18)) as prevequity,
 cast(otp_secret as string) as otp_secret,
 cast(send_reports as int) as send_reports,
 cast(mqid as long) as mqid,
 cast(user_color as long) as user_color,
 cast(equity as decimal(38,18)) as equity,
 cast(margin as decimal(38,18)) as margin,
 cast(margin_level as decimal(38,18)) as margin_level,
 cast(margin_free as decimal(38,18)) as margin_free,
 cast(conv_rates2 as decimal(38,18)) as conv_rates2,
 cast(modify_time as timestamp) as modify_time,
 cast(isarchive as boolean) as isarchive
from {{ source('dw', 'deals__pg_230__fbs_mt4_real12__mt4_users') }}
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
--  where input_file_name() = 'gs://staging-databricks-prod/pg_230__deals/fbs_mt4_real12__mt4_users/part-0-1672023621823.gzip.parquet'
--  where input_file_name() = 'gs://staging-databricks-prod/pg_230__deals/fbs_mt4_real12__mt4_users/part-0-1672066856129.gzip.parquet'
--  where input_file_name() = 'gs://staging-databricks-prod/pg_230__deals/fbs_mt4_real12__mt4_users/part-0-1672110102450.gzip.parquet'
--  where input_file_name() = 'gs://staging-databricks-prod/pg_230__deals/fbs_mt4_real12__mt4_users/part-0-1672153251809.gzip.parquet'
{% endif %}

