with source as (
    select * from {{ source('raw', 'reviews_for_tttc') }}
),

flattened as (
    select
        VALUE:review_id::string               as review_id,
        VALUE:order_id::string                as order_id,
        VALUE:order_date::date                as order_date,
        VALUE:order_status::string            as order_status,
        VALUE:review_score::float             as review_score,
        VALUE:sentiment_label::string         as sentiment_label,
        VALUE:text::string                    as review_text,
        VALUE:review_comment_title::string    as review_comment_title,
        VALUE:review_creation_date::timestamp as review_creation_date,
        VALUE:product_category_name::string   as product_category_name,
        VALUE:category_en::string             as category_en
    from source
    where VALUE:text::string is not null
    and length(VALUE:text::string) > 10
)

select * from flattened