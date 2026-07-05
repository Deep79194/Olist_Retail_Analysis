{{ config(materialized='table') }}

select
    d.customer_unique_id,
    d.customer_city,
    d.customer_state,
    d.lifecycle_stage,
    d.total_orders,
    d.first_order_date,
    d.last_order_date,
    d.customer_tenure_days,
    d.avg_days_between_orders,
    d.days_since_last_order_current,

    f.total_revenue,
    f.avg_order_value,
    f.revenue_per_day

from {{ ref('dim_customer_enriched') }} d
join {{ ref('fact_customer_value') }} f
    on d.customer_unique_id = f.customer_unique_id