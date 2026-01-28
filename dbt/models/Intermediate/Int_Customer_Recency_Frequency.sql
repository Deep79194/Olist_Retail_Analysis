{{config(materialized='table')}}

With fetch_Int_Customer_Orders as (

    select
        customer_unique_id,
        customer_id,
        order_id,
        order_date,
        days_since_last_order
    from {{ ref('Int_Customer_Orders') }}

),
aggregated_data as (
    select 
        customer_unique_id,
        count(order_id) as total_orders,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        avg(days_since_last_order) as avg_days_between_orders,
        datediff(
            day,
            max(order_date),
            current_date
        ) as days_since_last_order,
        datediff(
            day,
            min(order_date),
            max(order_date)
        ) as customer_tenure_days
    from fetch_Int_Customer_Orders
    group by customer_unique_id
)
select * from aggregated_data