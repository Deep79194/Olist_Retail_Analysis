{{ config(materialized='view') }}

select
    c1 as product_category_name,
    c2 as product_category_name_english
from {{ source('olist_raw', 'category_name_translation') }}