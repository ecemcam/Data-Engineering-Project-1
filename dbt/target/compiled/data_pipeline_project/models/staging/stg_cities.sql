with source as (

    select * from `data-pipeline-project-474812`.`raw_data`.`cities`

)

select distinct
    -- Primary Key
    _id as city_id,
    
    -- Foreign Keys
    _state as state_id,
    _country as country_id,
    
    -- Attributes
    name as city_name

from source
where _id is not null