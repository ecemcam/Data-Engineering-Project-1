-- Query 1: Top 10 cities by active subscribers in the last 1 month
-- NOTE: Replace [YOUR_ANALYTICS_TABLE_PATH] with your BigQuery project.dataset (e.g., prod_db.marts)
-- This query now estimates the subscriber count based on the existing customer geographic distribution.

with latest_metrics as (
    -- 1. Get the MAX active subscriber count from the latest date in the mart
    select
        active_subscribers as total_active_subscribers
    from `data-pipeline-project-474812.analytics_staging_marts.mart_subscription_daily`
    order by date desc
    limit 1
),

customer_distribution as (
    -- 2. Calculate the number of customers in each city
    select
        city,
        count(customer_id) as city_customer_count
    from `data-pipeline-project-474812.analytics_staging_marts.dim_customer`
    where city is not null
    group by 1
),

total_customer_count as (
    -- Calculate the grand total of all customers for the division ratio
    select count(customer_id) as grand_total_customers from `data-pipeline-project-474812.analytics_staging_marts.dim_customer`
)

select
    t1.city,
    -- 3. Calculate the subscriber estimate:
    -- (City Customer Count / Grand Total Customer Count) * Total Active Subscribers
    cast(t2.total_active_subscribers * (t1.city_customer_count / t3.grand_total_customers) as int64) as estimated_active_subscribers
from customer_distribution t1
cross join latest_metrics t2
cross join total_customer_count t3 -- These cross joins bring the fixed total values to every row
order by estimated_active_subscribers desc
limit 10