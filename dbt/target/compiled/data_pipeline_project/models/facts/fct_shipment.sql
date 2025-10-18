

with parsed_shipments as (
    -- CORRECTED: Referencing the new intermediate model
    select * from `data-pipeline-project-474812`.`analytics_staging_staging`.`int_shipments_decomposed`  
),

orders as (
    select 
        order_id,
        order_status as latest_status
    from `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_orders`
),

final as (
    select
        ps.shipment_id,
        ps.order_id,
        
        -- Use status from orders table (assuming it holds the definitive latest status)
        o.latest_status,
        
        ps.delivered_at,
        ps.carrier

    from parsed_shipments ps
    left join orders o 
        on ps.order_id = o.order_id
)

select * from final