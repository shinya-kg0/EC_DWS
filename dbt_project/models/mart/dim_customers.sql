with orders as (
    select * from {{ ref('stg_fact_orders') }}
),

customer_base as (
    select
        customer_unique_id,
        customer_state,
        customer_city,
        count(distinct order_id)            as order_count,
        sum(total_payment_value)            as lifetime_value,
        min(order_date)                     as first_order_date,
        max(order_date)                     as last_order_date,
        avg(review_score)                   as avg_review_score,
        sum(case when is_delayed then 1 else 0 end) as delayed_orders
    from orders
    where order_status = 'delivered'
    group by 1, 2, 3
)

select
    customer_unique_id,
    customer_state,
    customer_city,
    order_count,
    lifetime_value,
    first_order_date,
    last_order_date,
    avg_review_score,
    delayed_orders,
    datediff('day', first_order_date, last_order_date) as customer_tenure_days
from customer_base
