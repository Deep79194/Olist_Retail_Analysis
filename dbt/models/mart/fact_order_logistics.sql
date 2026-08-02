{{ config(materialized='table') }}

with items_agg as (

    select
        order_id,
        sum(price) as total_price,
        sum(freight_value) as total_freight,
        count(order_item_id) as total_items
    from {{ ref('stag_order_items') }}
    group by order_id

),

reviews_dedup as (

    select
        order_id,
        review_score,
        row_number() over (
            partition by order_id
            order by review_answer_timestamp desc
        ) as rn
    from {{ ref('stag_order_reviews') }}

)

select
    d.order_id,
    d.customer_id,
    d.order_status,
    d.order_purchase_timestamp,
    d.order_delivered_customer_date,
    d.order_estimated_delivery_date,
    d.processing_days,
    d.delivery_days,
    d.days_early_or_late,
    d.on_time_flag,

    i.total_price,
    i.total_freight,
    i.total_items,

    r.review_score

from {{ ref('int_order_delivery_performance') }} d
left join items_agg i
    on d.order_id = i.order_id
left join reviews_dedup r
    on d.order_id = r.order_id
   and r.rn = 1