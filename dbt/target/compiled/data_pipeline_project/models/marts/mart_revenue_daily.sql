with daily_revenue as (
    select
        -- Use created_at as fallback when delivered_at is NULL
        coalesce(cast(fs.delivered_at as date), cast(fo.order_date as date)) as date,
        
        sum(fo.items_total) as daily_revenue,
        count(distinct fo.order_id) as delivered_orders
        
    from `data-pipeline-project-474812`.`analytics_staging_marts`.`fct_order` fo
    inner join `data-pipeline-project-474812`.`analytics_staging_marts`.`fct_shipment` fs
        on fo.order_id = fs.order_id
    where fs.latest_status = 'DELIVERED' 
    group by 1
)

select * from daily_revenue
order by date