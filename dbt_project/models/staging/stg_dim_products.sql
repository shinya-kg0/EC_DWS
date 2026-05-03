with source as (
    select * from {{ source('raw', 'dim_products') }}
)

select
    "order_id"              as order_id,
    "product_id"            as product_id,
    "product_category_name" as product_category_name,
    "category_en"           as category_en,
    "price"                 as price,
    "freight_value"         as freight_value
from source