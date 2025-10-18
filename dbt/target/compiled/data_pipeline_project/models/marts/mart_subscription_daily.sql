with subscriptions as (
    select 
        subscription_id,
        customer_id,
        -- start_date is already TIMESTAMP → just cast to DATE
        date(start_date) as start_date,
        -- end_date is now TIMESTAMP (after your fix) → safe to cast to DATE
        date(end_date) as end_date,
        status
    from `data-pipeline-project-474812`.`analytics_staging_marts`.`dim_subscription`
),

-- Generate a full series of dates from the first subscription to today
date_series as (
    select date_day
    from unnest(generate_date_array(
        (select min(start_date) from subscriptions),
        current_date(),
        interval 1 day
    )) as date_day
),

-- Daily metrics calculation
daily_metrics as (
    select
        ds.date_day as date,

        -- Active subscribers
        count(case 
            when s.start_date <= ds.date_day 
            and (s.end_date is null or s.end_date > ds.date_day)
            and s.status = 'Active'
            then 1 
        end) as active_subscribers,

        -- New subscribers
        count(case 
            when s.start_date = ds.date_day 
            then 1 
        end) as new_subscribers,

        -- Cancellations
        count(case 
            when s.end_date = ds.date_day 
            then 1 
        end) as cancellations

    from date_series ds
    cross join subscriptions s
    group by ds.date_day
)

select * from daily_metrics
order by date