

  create or replace view `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_orders`
  OPTIONS()
  as with source as (

    select * from `data-pipeline-project-474812`.`raw_data`.`orders`

),

renamed_and_cleaned as (

    select

        -- Deduplication logic: Prioritize the first record if an ID appears more than once
        row_number() over (partition by _id order by createdAt asc) as rn,

        -- Primary Key
        _id as order_id,

        -- Foreign Keys
        _user as user_id,
        _deliveryAddress as delivery_address_id,
        _invoiceAddress as invoice_address_id,

        -- Standardized Timestamps
        cast(createdAt as timestamp) as created_at,

        -- Metrics: JSON Extraction
        -- Extract the final charged amount (in TRY, as confirmed)
        cast(json_extract_scalar(price, '$.chargedAmount') as numeric) as charged_amount_try,
        
        -- Attributes
        status as order_status,

        -- Complex/JSON Fields (Selected as raw strings)
        price as price_data,
        oneTimePurchase as one_time_purchase_data,
        subscriptions as subscription_data

    from source
    where _id is not null
    -- Simple cleaning: Filter out records where we cannot extract a charged amount
    and json_extract_scalar(price, '$.chargedAmount') is not null
    and cast(json_extract_scalar(price, '$.chargedAmount') as numeric) > 0

)

select 
    order_id,
    user_id,
    delivery_address_id,
    invoice_address_id,
    created_at,
    charged_amount_try,
    order_status,
    price_data,
    one_time_purchase_data,
    subscription_data
from renamed_and_cleaned
-- Final selection: ONLY select the single, canonical record (rn = 1)
where rn = 1;

