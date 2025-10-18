with orders as (
    select * from `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_orders`
),

pricing_extracted as (
    select
        order_id,
        user_id,
        created_at,
        order_status,
        
        -- EXTRACT REQUIRED FIELDS FROM PRICE JSON
        cast(json_extract_scalar(price_data, '$.grossOriginalAmount') as numeric) as items_total,
        cast(json_extract_scalar(price_data, '$.grossPromoDiscountAmount') as numeric) as discount_total,
        cast(json_extract_scalar(price_data, '$.shipmentFeeAmount') as numeric) as shipping_fee,
        cast(json_extract_scalar(price_data, '$.chargedAmount') as numeric) as charged_amount

    from orders
    where order_id is not null
)

select 
    order_id,
    user_id as customer_id,
    created_at as order_date,
    order_status as payment_status,
    items_total,
    discount_total,
    shipping_fee,
    charged_amount
from pricing_extracted