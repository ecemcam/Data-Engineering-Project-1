

  create or replace view `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_marketing_spend`
  OPTIONS()
  as with source as (

    select * from `data-pipeline-project-474812`.`raw_data`.`marketing_spend`

),

unpivot_spend as (

    -- Manual UNPIVOT for each date column
    select
        channel as marketing_channel,
        '2025-09-20' as spend_date_raw,
        cast(replace(`2025-09-20`, ',', '') as numeric) as spend_amount_try
    from source
    where `2025-09-20` is not null and cast(replace(`2025-09-20`, ',', '') as numeric) > 0

    union all

    select
        channel as marketing_channel,
        '2025-09-21' as spend_date_raw,
        cast(replace(`2025-09-21`, ',', '') as numeric) as spend_amount_try
    from source
    where `2025-09-21` is not null and cast(replace(`2025-09-21`, ',', '') as numeric) > 0

    union all

    select
        channel as marketing_channel,
        '2025-09-22' as spend_date_raw,
        cast(replace(`2025-09-22`, ',', '') as numeric) as spend_amount_try
    from source
    where `2025-09-22` is not null and cast(replace(`2025-09-22`, ',', '') as numeric) > 0

    union all

    select
        channel as marketing_channel,
        '2025-09-23' as spend_date_raw,
        cast(replace(`2025-09-23`, ',', '') as numeric) as spend_amount_try
    from source
    where `2025-09-23` is not null and cast(replace(`2025-09-23`, ',', '') as numeric) > 0

    union all

    select
        channel as marketing_channel,
        '2025-09-24' as spend_date_raw,
        cast(replace(`2025-09-24`, ',', '') as numeric) as spend_amount_try
    from source
    where `2025-09-24` is not null and cast(replace(`2025-09-24`, ',', '') as numeric) > 0

    union all

    select
        channel as marketing_channel,
        '2025-09-25' as spend_date_raw,
        cast(replace(`2025-09-25`, ',', '') as numeric) as spend_amount_try
    from source
    where `2025-09-25` is not null and cast(replace(`2025-09-25`, ',', '') as numeric) > 0

    union all

    select
        channel as marketing_channel,
        '2025-09-26' as spend_date_raw,
        cast(replace(`2025-09-26`, ',', '') as numeric) as spend_amount_try
    from source
    where `2025-09-26` is not null and cast(replace(`2025-09-26`, ',', '') as numeric) > 0

    union all

    select
        channel as marketing_channel,
        '2025-09-27' as spend_date_raw,
        cast(replace(`2025-09-27`, ',', '') as numeric) as spend_amount_try
    from source
    where `2025-09-27` is not null and cast(replace(`2025-09-27`, ',', '') as numeric) > 0

),

final_conversion as (

    select
        parse_date('%Y-%m-%d', spend_date_raw) as spend_date,
        marketing_channel,
        spend_amount_try
    
    from unpivot_spend

)

select * from final_conversion;

