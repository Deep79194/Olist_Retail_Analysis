{{ config(materialized='table') }}

select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    datediff(day, order_purchase_timestamp, order_approved_at) as processing_days,
    datediff(day, order_purchase_timestamp, order_delivered_customer_date) as delivery_days,
    datediff(day, order_delivered_customer_date, order_estimated_delivery_date) as days_early_or_late,
    case
        when order_delivered_customer_date is null then null
        when order_delivered_customer_date <= order_estimated_delivery_date then 1
        else 0
    end as on_time_flag
from {{ ref('stag_orders') }}
where order_status = 'delivered'