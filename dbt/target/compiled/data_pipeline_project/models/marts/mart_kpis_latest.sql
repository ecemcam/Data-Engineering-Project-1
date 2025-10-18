

with latest_subscription_data as (
    -- Get the latest date and active subscribers in separate steps
    select 
        max(date) as latest_date
    from `data-pipeline-project-474812`.`analytics_staging_marts`.`mart_subscription_daily`
),

snapshot_kpis as (
    select
        lsd.latest_date,
        -- Get active subscribers for the latest date
        (select active_subscribers 
         from `data-pipeline-project-474812`.`analytics_staging_marts`.`mart_subscription_daily` 
         where date = lsd.latest_date) as active_subscribers_latest
    from latest_subscription_data lsd
),

-- CTE: Calculate the Average Revenue Per Active Subscriber (ARPAS) over the latest 30 days.
monthly_revenue_metrics as (
    select
        -- Total gross revenue from delivered orders over the last 30 days
        sum(mr.daily_revenue) as total_gross_revenue_30d,
        -- Total sum of active subscribers across those 30 days
        sum(msd.active_subscribers) as total_active_subs_30d
    from `data-pipeline-project-474812`.`analytics_staging_marts`.`mart_revenue_daily` mr
    inner join `data-pipeline-project-474812`.`analytics_staging_marts`.`mart_subscription_daily` msd
        on mr.date = msd.date
    where mr.date >= date_sub(
        (select max(date) from `data-pipeline-project-474812`.`analytics_staging_marts`.`mart_subscription_daily`), 
        interval 30 day
    )
)

-- Final assembly with lookback calculations
select
    t1.latest_date,
    t1.active_subscribers_latest,

    -- 2. AVERAGE REVENUE PER ACTIVE SUB (ARPAS)
    round(
        (t2.total_gross_revenue_30d / nullif(t2.total_active_subs_30d, 0)) * 30
    , 2) as monthly_revenue_per_active_sub,

    -- 3. MONTHLY RECURRING REVENUE (MRR)
    round(
        t1.active_subscribers_latest * (t2.total_gross_revenue_30d / nullif(t2.total_active_subs_30d, 0)) * 30
    , 2) as monthly_recurring_revenue,

    -- 4. MONTHLY CHURN RATE %
    round(
        (
            -- Total cancellations in the last 30 days
            (select sum(cancellations) from `data-pipeline-project-474812`.`analytics_staging_marts`.`mart_subscription_daily` where date >= date_sub(t1.latest_date, interval 30 day))
            / 
            -- Active subscribers 30 days ago
            nullif((select active_subscribers from `data-pipeline-project-474812`.`analytics_staging_marts`.`mart_subscription_daily` where date = date_sub(t1.latest_date, interval 30 day)), 0)
        ) * 100
        , 2
    ) as monthly_churn_rate_pct,

    -- 5. CAC PAYBACK PERIOD
    round(
        (
            -- Average CAC over the last 30 days
            (select avg(daily_customer_acquisition_cost) from `data-pipeline-project-474812`.`analytics_staging_marts`.`mart_acquisition_efficiency` where date >= date_sub(t1.latest_date, interval 30 day))
            / 
            -- Monthly Revenue Per Active Sub (ARPAS)
            nullif(
                (t2.total_gross_revenue_30d / nullif(t2.total_active_subs_30d, 0)) * 30
                , 0
            )
        )
        , 2
    ) as cac_payback_months

from snapshot_kpis t1
cross join monthly_revenue_metrics t2