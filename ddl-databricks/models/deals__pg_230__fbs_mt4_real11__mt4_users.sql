{{ config(
        pre_hook="CREATE TABLE if not exists dw.deals__pg_230__fbs_mt4_real11__mt4_users USING parquet LOCATION 'gs://staging-databricks-test/pg_230__deals/fbs_mt4_real11__mt4_users'",
        materialized='incremental',
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='append_new_columns',
        unique_key=['login'],
        schema='staging'
    )
}}

select
 cast(login as bigint) login,
 group,
 password,
 cast(enable as bigint) enable,
 cast(enable_change_password as bigint) enable_change_password,
 cast(enable_read_only as bigint) enable_read_only,
 cast(enable_otp as bigint) enable_otp,
 password_investor,
 password_phone,
 name,
 country,
 city,
 state,
 cast(enable_change_pass as double) enable_change_pass,
 cast(enable_readonly as double) enable_readonly,
 zipcode,
 address,
 lead_source,
 phone,
 email,
 comment,
 id,
 status,
 regdate,
 lastdate,
 cast(leverage as bigint) leverage,
 cast(agent_account as bigint) agent_account,
 timestamp,
 cast(last_ip as bigint) last_ip,
 balance,
 prevmonthbalance,
 prevbalance,
 credit,
 interestrate,
 taxes,
 prevmonthequity,
 prevequity,
 otp_secret,
 cast(send_reports as bigint) send_reports,
 cast(mqid as bigint) mqid,
 cast(user_color as bigint) user_color,
 equity,
 margin,
 margin_level,
 margin_free,
 conv_rates2,
 modify_time,
 isarchive,
 "1970-01-01" _pg_export_dt,
 0 __index_level_0__
from {{ source('dw', 'deals__pg_230__fbs_mt4_real11__mt4_users') }}
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where 1 = 1
{% endif %}

