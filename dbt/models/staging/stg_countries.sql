with source as (

    select * from {{ source('raw_data', 'countries') }}

)

select distinct
    -- Primary Key: Standardize to country_id
    _id as country_id,
    
    -- Attributes
    name as country_name

from source
where _id is not null