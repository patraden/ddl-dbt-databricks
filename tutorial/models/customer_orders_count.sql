/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view') }}

with customer_orders_count as (
select c.first_name, c.last_name, count(s.id) qty
from public.customers c
left join public.sales_orders s
on s.user_id = c.id
group by c.first_name, c.last_name
)

select * from customer_orders_count