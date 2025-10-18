

  create or replace view `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_shipments`
  OPTIONS()
  as with source as (

    select * from `data-pipeline-project-474812`.`raw_data`.`shipments`

),

renamed_and_cleaned as (

    select

        -- Deduplication logic: Prioritize the latest record (newest created_at, if available, otherwise just use a distinct ID)
        -- Since this table is likely event-based, we prioritize the latest row for any given ID.
        row_number() over (partition by _id order by collectDate desc) as rn,

        -- Primary Key
        _id as shipment_id,

        -- Foreign Keys
        _order as order_id,
        _user as user_id,

        -- Standardized Timestamps
        -- Assuming 'createdAt' still exists as a standard column for the record's creation time
        cast(collectDate as timestamp) as collected_at,

        -- Complex/JSON Fields (Selected as raw strings for later parsing in Intermediate models)
        details as details_json,
        label as label_json

    from source
    where _id is not null
    and _order is not null -- Ensure the shipment links to a valid order
)

select
    shipment_id,
    order_id,
    user_id,
    collected_at,
    details_json,
    label_json
from renamed_and_cleaned
-- Final selection: only include the latest, unique instance
where rn = 1;

