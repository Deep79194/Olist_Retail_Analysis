{{config(materialized='table')}}

with customer_orders as (

    select
        customer_unique_id,
        order_id,
        order_date,
        days_since_last_order
    from {{ ref('Int_Customer_Orders') }}

),
customer_agg as (

    select
        customer_unique_id,

        count(order_id) as total_orders,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,

        avg(days_since_last_order) as avg_days_between_orders

    from customer_orders
    group by customer_unique_id
),
as_of_date as (

    select
        max(order_date) as as_of_date
    from customer_orders
)
select
    c.*,
    a.as_of_date,
    datediff(day, c.last_order_date, a.as_of_date) as days_since_last_order_current,
    datediff(day, c.first_order_date, c.last_order_date) as customer_tenure_days

from customer_agg c
cross join as_of_date a
