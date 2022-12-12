{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='fail',
        unique_key=['event_date'],
        partition_by=['event_date'],
        schema='sandbox'
    )
}}


select event_date, count(*) as qty
from {{ source('sandbox', 'events__bq_fbscom_5fb80__analytics_158994318__events') }}
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where to_date(event_date, 'yyyyMMdd') = current_date() - 2
{% endif %}
group by event_date
