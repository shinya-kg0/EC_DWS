with source as (
    select * from {{ source('raw', 'fact_orders') }}
),

flattened as (
    select
        VALUE:order_id::string               as order_id,
        VALUE:customer_id::string            as customer_id,
        VALUE:customer_unique_id::string     as customer_unique_id,
        VALUE:order_status::string           as order_status,
        VALUE:order_date::date               as order_date,
        VALUE:order_month::int               as order_month,
        VALUE:order_year::int                as order_year,
        VALUE:order_purchase_timestamp::timestamp as order_purchase_timestamp,
        VALUE:order_estimated_delivery_date::date as order_estimated_delivery_date,
        VALUE:order_delivered_customer_date::date as order_delivered_customer_date,
        VALUE:delivery_days::int             as delivery_days,
        VALUE:is_delayed::boolean            as is_delayed,
        VALUE:total_item_price::float        as total_item_price,
        VALUE:total_freight_value::float     as total_freight_value,
        VALUE:total_payment_value::float     as total_payment_value,
        VALUE:primary_payment_type::string   as primary_payment_type,
        VALUE:max_installments::int          as max_installments,
        VALUE:item_count::int                as item_count,
        VALUE:review_score::float            as review_score,
        VALUE:has_comment::boolean           as has_comment,
        VALUE:customer_state::string         as customer_state,
        VALUE:customer_city::string          as customer_city
    from source
)

select * from flattened