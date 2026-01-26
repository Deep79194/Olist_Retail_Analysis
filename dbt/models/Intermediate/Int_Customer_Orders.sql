{{ config(materialized='table') }}

select
    c.customer_unique_id,
    o.customer_id,
    o.order_id,
    o.order_purchase_timestamp as order_date,

    row_number() over (
        partition by c.customer_unique_id
        order by o.order_purchase_timestamp
    ) as order_sequence,

    datediff(
        day,
        lag(o.order_purchase_timestamp) over (
            partition by c.customer_unique_id
            order by o.order_purchase_timestamp
        ),
        o.order_purchase_timestamp
    ) as days_since_last_order

from {{ ref('stag_orders') }} o
join {{ ref('stag_customers') }} c
  on o.customer_id = c.customer_id
