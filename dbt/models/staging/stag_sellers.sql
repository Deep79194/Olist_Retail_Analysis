{{ config(materialized='view') }}

select
    seller_id,
    seller_city,
    seller_state
from {{ source('olist_raw', 'sellers') }}