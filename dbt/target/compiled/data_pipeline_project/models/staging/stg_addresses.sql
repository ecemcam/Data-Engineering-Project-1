with source as (

    select * from `data-pipeline-project-474812`.`raw_data`.`addresses`

)

select distinct
    -- Primary Key
    _id as address_id,
    
    -- Foreign Keys (Geographical and User Links)
    _city as city_id,
    _state as state_id,
    _country as country_id,
    _user as user_id, 
    _neighborhood as neighborhood_id, -- Maps to the neighborhood table ID
    
    -- Attributes
    invoiceType as invoice_type

from source
where _id is not null