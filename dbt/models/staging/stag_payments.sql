{{ config(materialized='table') }}

select
    p.order_id,
    p.payment_sequential,
    p.payment_type,
    p.payment_installments,
    p.payment_value
from {{ source('olist_raw', 'payments') }} p