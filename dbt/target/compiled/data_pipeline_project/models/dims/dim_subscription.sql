
with subscriptions as (
    select * from `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_subscriptions`
),

final_parsing as (
    select
        subscription_id,
        user_id as customer_id, 
        start_date,  
        case
            when is_active = FALSE then 'Cancelled'
            else 'Active'
        end as status,
        -- Business logic: end_date only exists upon explicit cancellation
        case
            when is_active = FALSE then cast(NULL as timestamp)  -- Would be cancellation date
            else cast(NULL as timestamp)                         -- Active = no end date
        end as end_date

        from subscriptions
)

select * from final_parsing