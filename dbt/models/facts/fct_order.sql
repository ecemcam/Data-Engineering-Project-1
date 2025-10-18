{{
    config(
        materialized='table',
        schema='marts'
    )
}}
with base_orders as (
    select * from {{ ref('int_orders_decomposed') }}
),

subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
),

final as (
    select
        bo.order_id,
        bo.customer_id,
        s.subscription_id,  -- ← THIS COMES FROM THE JOIN!
        bo.order_date,
        bo.items_total,
        bo.discount_total,
        bo.shipping_fee,
        case
            when bo.payment_status = 'PAID' then
                bo.items_total - bo.discount_total + bo.shipping_fee
            else 0
        end as net_revenue,
        bo.payment_status
    from base_orders bo
    left join subscriptions s 
        on bo.customer_id = s.user_id
)

select * from final