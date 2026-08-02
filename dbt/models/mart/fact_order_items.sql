{{ config(materialized='table') }}

select
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.price,
    oi.freight_value,

    p.product_category_name_english,

    s.seller_city,
    s.seller_state,

    d.order_purchase_timestamp,
    d.delivery_days,
    d.on_time_flag,
    d.review_score

from {{ ref('stag_order_items') }} oi
left join {{ ref('stag_products') }} p
    on oi.product_id = p.product_id
left join {{ ref('stag_sellers') }} s
    on oi.seller_id = s.seller_id
left join {{ ref('fact_order_logistics') }} d
    on oi.order_id = d.order_id