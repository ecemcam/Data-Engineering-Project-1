{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with daily_spend as (
    -- 1. Aggregate daily marketing spend
    select
        cast(date as date) as date,
        sum(spend_try) as total_marketing_spend
    from {{ ref('fct_marketing_spend') }}
    group by 1
),

daily_new_subscribers as (
    -- 2. Pull daily new subscribers from the subscription mart
    select
        date,
        new_subscribers
    from {{ ref('mart_subscription_daily') }}
),

final as (
    select
        -- Join date (ensures we get all dates from either spend or subscribers)
        coalesce(ds.date, dns.date) as date,

        -- Metrics used for calculation
        coalesce(ds.total_marketing_spend, 0) as total_marketing_spend,
        coalesce(dns.new_subscribers, 0) as new_subscribers,

        -- CALCULATE CAC: Spend / New Subscribers
        -- FIX: Apply ROUND to two decimal places for cleaner reporting.
        case 
            when coalesce(dns.new_subscribers, 0) > 0 then 
                round(coalesce(ds.total_marketing_spend, 0) / dns.new_subscribers, 2)
            else NULL -- Use NULL when there is no acquisition event (or cost is zero)
        end as daily_customer_acquisition_cost
        
    from daily_spend ds
    full outer join daily_new_subscribers dns
        on ds.date = dns.date
    where 
        -- Only include rows where we have either spend or new subscribers (to avoid empty dates)
        coalesce(ds.total_marketing_spend, 0) > 0 OR coalesce(dns.new_subscribers, 0) > 0
    order by 1
)

select * from final