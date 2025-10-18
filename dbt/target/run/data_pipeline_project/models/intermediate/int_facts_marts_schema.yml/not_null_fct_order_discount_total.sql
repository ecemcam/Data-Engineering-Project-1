
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select discount_total
from `data-pipeline-project-474812`.`analytics_staging_marts`.`fct_order`
where discount_total is null



  
  
      
    ) dbt_internal_test