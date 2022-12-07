{{ config(materialized='view') }}

with customer_orders as (

    select sum(qty) qty from {{ ref("customer_orders_count") }}

)

select *
from customer_orders