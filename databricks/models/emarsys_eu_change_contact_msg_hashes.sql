{{ config(
    materialized='table',
    file_format='delta',
    schema='sandbox'
    ) }}


select *, xxhash64(*) hash64
from default.emarsys_eu_change_contact