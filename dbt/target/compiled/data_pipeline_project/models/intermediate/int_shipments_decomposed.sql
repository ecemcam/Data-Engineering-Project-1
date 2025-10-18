with shipments as (
    select * from `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_shipments`
),

-- Step 1: Extract the comma-separated numeric components string (using only ONE capturing group)
numeric_components_extracted as (
    select
        *,
        -- Captures the numbers inside the parentheses (e.g., '2023, 10, 27, 10, 30, 0')
        regexp_extract(details_json, r"deliveryDate': datetime\.datetime\(([^)]+)\)") as delivery_components_str,
        regexp_extract(details_json, r"collectDate': datetime\.datetime\(([^)]+)\)") as collect_components_str
    from shipments
),

-- Step 2: Split the components and construct the DATETIME object
parsed_shipments as (
    select
        s.shipment_id,
        s.order_id,
        s.user_id,
        
        -- DATETIME Construction for delivered_at using SPLIT and SAFE_OFFSET
        case
            when s.delivery_components_str is not null then
                datetime(
                    cast(split(s.delivery_components_str, ', ')[safe_offset(0)] as int64), -- Year
                    cast(split(s.delivery_components_str, ', ')[safe_offset(1)] as int64), -- Month
                    cast(split(s.delivery_components_str, ', ')[safe_offset(2)] as int64), -- Day
                    cast(split(s.delivery_components_str, ', ')[safe_offset(3)] as int64), -- Hour
                    cast(split(s.delivery_components_str, ', ')[safe_offset(4)] as int64), -- Minute
                    cast(split(s.delivery_components_str, ', ')[safe_offset(5)] as int64)  -- Second
                )
            else null
        end as delivered_at,

        -- DATETIME Construction for collect_date
        case
            when s.collect_components_str is not null then
                datetime(
                    cast(split(s.collect_components_str, ', ')[safe_offset(0)] as int64), -- Year
                    cast(split(s.collect_components_str, ', ')[safe_offset(1)] as int64), -- Month
                    cast(split(s.collect_components_str, ', ')[safe_offset(2)] as int64), -- Day
                    cast(split(s.collect_components_str, ', ')[safe_offset(3)] as int64), -- Hour
                    cast(split(s.collect_components_str, ', ')[safe_offset(4)] as int64), -- Minute
                    cast(split(s.collect_components_str, ', ')[safe_offset(5)] as int64)  -- Second
                )
            else null
        end as collect_date,

        -- Extract carrier information
        json_extract_scalar(s.label_json, '$.provider') as carrier

    from numeric_components_extracted s
    where s.shipment_id is not null
)

select 
    shipment_id,
    order_id,
    user_id,
    delivered_at,
    collect_date,
    carrier,
    -- We don't need inferred_status here, we just need the final dates/carrier for the Fact table
    'TEMP' as latest_status -- Placeholder. Status comes from FCT_ORDER join.
from parsed_shipments