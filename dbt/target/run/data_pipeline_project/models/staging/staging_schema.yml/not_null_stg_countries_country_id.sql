
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select country_id
from `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_countries`
where country_id is null



  
  
      
    ) dbt_internal_test