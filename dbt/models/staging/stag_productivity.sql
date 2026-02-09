{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['START_PERIOD', 'NAME', 'STATION','LABOR_ACTIVITY']
    )
}}

with productivity_data as(
    select 
        SHIFT_START_TIME,
        START_PERIOD,
        NAME,
        STATION,
        LABOR_ACTIVITY,
        GANG_TYPE,
        SUM(PACKS) as TOTAL_PACKS,
        UOM
        from {{ source('olist_raw', 'productivity') }}
    group by all

)
select * from productivity_data