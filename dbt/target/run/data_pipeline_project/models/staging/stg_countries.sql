

  create or replace view `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_countries`
  OPTIONS()
  as with source as (

    select * from `data-pipeline-project-474812`.`raw_data`.`countries`

)

select distinct
    -- Primary Key: Standardize to country_id
    _id as country_id,
    
    -- Attributes
    name as country_name

from source
where _id is not null;

