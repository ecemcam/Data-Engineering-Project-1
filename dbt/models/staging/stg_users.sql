with source as (

    select * from {{ source('raw_data', 'users') }}

),

-- Get the earliest subscription date for each user
subscription_dates as (
    select
        _user as user_id,
        min(cast(createdAt as timestamp)) as first_subscription_date
    from {{ source('raw_data', 'subscriptions') }}
    where _user is not null
    group by 1
),

renamed_and_cleaned as (

    select

        -- Primary Key: Standardized to user_id
        _id as user_id,

        -- SMART TIMESTAMP LOGIC: Use subscription date as fallback for missing created_at
        cast(
            case 
                when u.createdAt is not null then u.createdAt
                else s.first_subscription_date  -- Fallback to first subscription date
            end as timestamp
        ) as created_at,
        
        -- Deduplication logic: Assign a row number based on the creation time
        row_number() over (partition by _id order by 
            case 
                when u.createdAt is not null then u.createdAt
                else s.first_subscription_date 
            end desc
        ) as rn

    from source u
    left join subscription_dates s on u._id = s.user_id
    -- Simple cleaning: ensure that _id column is not null
    where _id is not null
)

select
    user_id,
    created_at

from renamed_and_cleaned
-- Final selection: only include the latest, unique instance of a user_id
where rn = 1