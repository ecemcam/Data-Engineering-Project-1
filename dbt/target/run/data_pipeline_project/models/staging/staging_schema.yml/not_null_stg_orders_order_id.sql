
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_id
from `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_orders`
where order_id is null



  
  
      
    ) dbt_internal_test