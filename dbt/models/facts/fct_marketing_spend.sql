{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with marketing_data as (
    -- Source the raw marketing data 
    select * from {{ ref('stg_marketing_spend') }}
),

final as (
    select
        -- Renamed Time Dimension (Grain)
        cast(spend_date as date) as date,
        
        -- Renamed Dimension
        marketing_channel as channel,
        
        -- Renamed Measure
        spend_amount_try as spend_try,
        
        -- Placeholder for missing data, set to NULL to indicate absence
        -- We use NULL instead of 0 as we don't know the true value.
        cast(NULL as numeric) as clicks
        
    from marketing_data
)

select * from final