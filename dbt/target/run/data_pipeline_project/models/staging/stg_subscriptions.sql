

  create or replace view `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_subscriptions`
  OPTIONS()
  as with source as (

    select * from `data-pipeline-project-474812`.`raw_data`.`subscriptions`

),

renamed_and_cleaned as (

    select

        -- Deduplication logic: Prioritize the latest record (newest created_at)
        row_number() over (partition by _id order by createdAt desc) as rn,

        -- Primary Key
        _id as subscription_id,

        -- Foreign Key
        _user as user_id,

        -- Standardized Timestamps
        cast(createdAt as timestamp) as created_at,
        cast(nextOrderDate as timestamp) as next_order_date,
        cast(startDate as timestamp) as start_date,

        -- Metrics
        cast(totalQuantity as integer) as total_quantity,

        -- Flags (Casting to BOOLEAN for clean usage)
        cast(isActive as boolean) as is_active,
        cast(isSkip as boolean) as is_skip,

        -- Complex/JSON Field (Selected as raw string for later parsing)
        products as products_data

    from source
    where _id is not null
    -- Simple cleaning: Ensure start_date and quantity are valid
    and startDate is not null
    and totalQuantity is not null and totalQuantity >= 0
)

select
    subscription_id,
    user_id,
    created_at,
    next_order_date,
    start_date,
    total_quantity,
    is_active,
    is_skip,
    products_data
from renamed_and_cleaned
-- Final selection: only include the latest, unique instance
where rn = 1;

