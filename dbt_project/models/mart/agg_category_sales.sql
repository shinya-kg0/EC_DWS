{{
    config(
        materialized='incremental',
        unique_key=['order_month', 'order_year', 'category_en'],
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

with max_ym as (
    {% if is_incremental() %}
    select
        max(order_year * 100 + order_month) as cutoff_ym
    from {{ this }}
    {% else %}
    select 0 as cutoff_ym
    {% endif %}
),

orders as (
    select o.*
    from {{ ref('fct_orders') }} o
    cross join max_ym
    where o.order_year * 100 + o.order_month >= max_ym.cutoff_ym - 1
),

products as (
    select * from {{ ref('stg_dim_products') }}
),

joined as (
    select
        o.order_date,
        o.order_month,
        o.order_year,
        p.category_en,
        p.product_category_name,
        o.order_id,
        p.price,
        p.freight_value,
        o.review_score
    from orders o
    join products p on o.order_id = p.order_id
)

select
    order_month,
    order_year,
    category_en,
    product_category_name,
    count(distinct order_id)    as order_count,
    sum(price)                  as revenue,
    sum(freight_value)          as freight_total,
    avg(review_score)           as avg_review_score
from joined
group by 1, 2, 3, 4
order by 1, revenue desc