{{
    config(
        materialized='incremental',
        unique_key=['category_en', 'product_category_name'],
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

with category_orders as (
    select * from {{ ref('agg_category_sales') }}
)

select
    category_en,
    product_category_name,
    sum(order_count)                                        as total_orders,
    sum(revenue)                                            as total_revenue,
    avg(avg_review_score)                                   as avg_review_score,
    count(distinct order_month)                             as active_months,
    sum(revenue) / nullif(count(distinct order_month), 0)   as avg_monthly_revenue
from category_orders
group by 1, 2
order by total_revenue desc