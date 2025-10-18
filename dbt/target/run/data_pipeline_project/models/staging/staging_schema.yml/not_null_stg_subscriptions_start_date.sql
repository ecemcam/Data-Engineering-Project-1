
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select start_date
from `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_subscriptions`
where start_date is null



  
  
      
    ) dbt_internal_test