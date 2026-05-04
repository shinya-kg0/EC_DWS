with source as (
    select * from {{ source('raw', 'dim_products') }}
),

flattened as (
    select
        VALUE:order_id::string               as order_id,
        VALUE:product_id::string             as product_id,
        VALUE:product_category_name::string  as product_category_name,
        VALUE:category_en::string            as category_en,
        VALUE:price::float                   as price,
        VALUE:freight_value::float           as freight_value
    from source
)

select * from flattened