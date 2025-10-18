
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date
from `data-pipeline-project-474812`.`analytics_staging_marts`.`fct_marketing_spend`
where date is null



  
  
      
    ) dbt_internal_test