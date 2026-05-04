{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

with orders as (
    select * from {{ ref('stg_fact_orders') }}

    {% if is_incremental() %}
    where order_date > (select max(order_date) from {{ this }})
    {% endif %}
)

select
    order_id,
    customer_id,
    customer_unique_id,
    order_status,
    order_date,
    order_month,
    order_year,
    delivery_days,
    is_delayed,
    total_item_price,
    total_freight_value,
    total_payment_value,
    primary_payment_type,
    max_installments,
    item_count,
    review_score,
    has_comment,
    customer_state,
    customer_city,

    -- RFM用カラム（Streamlitで使用）
    order_purchase_timestamp as purchased_at,
    total_payment_value      as product_revenue
from orders
where order_status = 'delivered'
