
    
    

with dbt_test__target as (

  select shipment_id as unique_field
  from `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_shipments`
  where shipment_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


