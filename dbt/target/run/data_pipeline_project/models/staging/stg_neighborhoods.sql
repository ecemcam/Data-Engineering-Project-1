

  create or replace view `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_neighborhoods`
  OPTIONS()
  as with source as (

    select * from `data-pipeline-project-474812`.`raw_data`.`neighborhoods`

)

select distinct
    -- Primary Key
    _id as neighborhood_id,
    
    -- Foreign Keys
    _city as city_id,
    _country as country_id,
    
    -- Attributes
    name as neighborhood_name,
    postalCode as postal_code

from source
where _id is not null;

