with source as (

    select * from {{ source('raw_data', 'states') }}

)

select distinct
    -- Primary Key
    _id as state_id,
    
    -- Foreign Key (to countries)
    _country as country_id,
    
    -- Attributes
    name as state_name

from source
where _id is not null