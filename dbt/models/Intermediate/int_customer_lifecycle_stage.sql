{{ config(materialized='table') }}

select
    customer_unique_id,
    total_orders,
    first_order_date,
    last_order_date,
    avg_days_between_orders,
    customer_tenure_days,
    days_since_last_order_current,

    case
        when total_orders = 1 then 'New'
        when total_orders between 2 and 3
             and days_since_last_order_current <= 90 then 'Active'
        when total_orders >= 4
             and days_since_last_order_current <= 90 then 'Loyal'
        else 'Churned'
    end as lifecycle_stage

from {{ ref('Int_Customer_Recency_Frequency') }}
