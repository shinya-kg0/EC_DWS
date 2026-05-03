with source as (
    select * from {{ source('raw', 'fact_orders') }}
)

select
    "order_id"                        as order_id,
    "customer_id"                     as customer_id,
    "customer_unique_id"              as customer_unique_id,
    "order_status"                    as order_status,
    "order_date"                      as order_date,
    "order_month"                     as order_month,
    "order_year"                      as order_year,
    "order_purchase_timestamp"        as order_purchase_timestamp,
    "order_estimated_delivery_date"   as order_estimated_delivery_date,
    "order_delivered_customer_date"   as order_delivered_customer_date,
    "delivery_days"                   as delivery_days,
    "is_delayed"                      as is_delayed,
    "total_item_price"                as total_item_price,
    "total_freight_value"             as total_freight_value,
    "total_payment_value"             as total_payment_value,
    "primary_payment_type"            as primary_payment_type,
    "max_installments"                as max_installments,
    "item_count"                      as item_count,
    "review_score"                    as review_score,
    "has_comment"                     as has_comment,
    "customer_state"                  as customer_state,
    "customer_city"                   as customer_city
from source