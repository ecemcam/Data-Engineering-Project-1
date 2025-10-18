
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select user_id
from `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_shipments`
where user_id is null



  
  
      
    ) dbt_internal_test