
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select created_at
from `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_users`
where created_at is null



  
  
      
    ) dbt_internal_test