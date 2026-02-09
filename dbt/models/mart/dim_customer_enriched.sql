{{ config(materialized='table') }}

select
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    l.lifecycle_stage,
    l.total_orders,
    l.first_order_date,
    l.last_order_date,
    l.customer_tenure_days,
    l.avg_days_between_orders,
    l.days_since_last_order_current

from {{ ref('stag_customers') }} c
join {{ ref('int_customer_lifecycle_stage') }} l
  on c.customer_unique_id = l.customer_unique_id
