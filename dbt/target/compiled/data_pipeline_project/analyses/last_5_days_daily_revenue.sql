select  
        date, 
        delivered_orders, 
        daily_revenue

        from `data-pipeline-project-474812.analytics_staging_marts.mart_revenue_daily`

order by date desc
limit 5;