with source as (
    select * from {{ source('raw', 'reviews_for_tttc') }}
)

select
    "review_id"                as review_id,
    "order_id"                 as order_id,
    "order_date"               as order_date,
    "order_status"             as order_status,
    "review_score"             as review_score,
    "sentiment_label"          as sentiment_label,
    "text"                     as review_text,
    "review_comment_title"     as review_comment_title,
    "review_creation_date"     as review_creation_date,
    "product_category_name"    as product_category_name,
    "category_en"              as category_en
from source
where "text" is not null
and length("text") > 10