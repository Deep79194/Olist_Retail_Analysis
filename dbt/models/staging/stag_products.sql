{{ config(materialized='view') }}

select
    product_id,
    product_category_name
from {{ source('olist_raw', 'products') }}