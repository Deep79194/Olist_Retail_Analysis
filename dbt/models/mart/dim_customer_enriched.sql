{{ config(materialized='table') }}

with latest_address as (
    select
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        row_number() over (
            partition by c.customer_unique_id
            order by o.order_purchase_timestamp desc
        ) as rn
    from {{ ref('stag_customers') }} c
    join {{ ref('stag_orders') }} o on o.customer_id = c.customer_id
)

select
    a.customer_unique_id,
    a.customer_city,
    a.customer_state,
    l.lifecycle_stage,
    l.total_orders,
    l.first_order_date,
    l.last_order_date,
    l.customer_tenure_days,
    l.avg_days_between_orders,
    l.days_since_last_order_current
from latest_address a
join {{ ref('int_customer_lifecycle_stage') }} l
    on a.customer_unique_id = l.customer_unique_id
where a.rn = 1