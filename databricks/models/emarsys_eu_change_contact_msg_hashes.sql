{{ config(
    materialized='table',
    file_format='delta',
    schema='dw'
    ) }}


select *, xxhash64(*) hash64
from default.emarsys_eu_change_contact