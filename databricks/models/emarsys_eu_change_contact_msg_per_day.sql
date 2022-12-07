{{ config(
    materialized='table',
    file_format='delta',
    schema='dw'
    ) }}


select
  date(timestamp) dt,
  count(*) qty
from
  default.emarsys_eu_change_contact
group by
  date(timestamp)
order by
  date(timestamp)