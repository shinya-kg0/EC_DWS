{{
    config(
        materialized='incremental',
        unique_key='order_date',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

with orders as (
    select * from {{ ref('fct_orders') }}

    {% if is_incremental() %}
    where order_date >= (
        select dateadd('day', -1, max(order_date)) from {{ this }}
    )
    {% endif %}
)

select
    order_date,
    order_month,
    order_year,
    count(distinct order_id)                            as order_count,
    sum(product_revenue)                                as revenue,
    sum(total_freight_value)                            as freight_revenue,
    avg(review_score)                                   as avg_review_score,
    sum(case when is_delayed then 1 else 0 end)         as delayed_count
from orders
group by 1, 2, 3
order by 1
