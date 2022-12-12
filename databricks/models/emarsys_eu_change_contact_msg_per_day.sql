{{ config(
    pre_hook="CREATE TABLE if not exists default.emarsys_eu_change_contact USING parquet LOCATION 'gs://databricks-emarsys/eu/change_contact'",
    materialized='table',
    file_format='delta',
    schema='sandbox'
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