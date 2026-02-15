{{ config(materialized='table') }}

with customer_orders as (

    select
        customer_unique_id,
        order_id
    from {{ ref('Int_Customer_Orders') }}

),

order_payments as (

    select
        order_id,
        sum(payment_value) as order_revenue
    from {{ ref('stag_payments') }}
    group by order_id
),

customer_revenue as (

    select
        co.customer_unique_id,

        count(distinct co.order_id) as total_orders,
        sum(op.order_revenue) as total_revenue

    from customer_orders co
    join order_payments op
      on co.order_id = op.order_id
    group by co.customer_unique_id
)

select
    cr.customer_unique_id,
    cr.total_orders,
    cr.total_revenue,

    cr.total_revenue / nullif(cr.total_orders, 0) as avg_order_value,

    cr.total_revenue / greatest(l.customer_tenure_days, 1) as revenue_per_day

from customer_revenue cr
join {{ ref('int_customer_lifecycle_stage') }} l
  on cr.customer_unique_id = l.customer_unique_id
