
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select delivered_at
from `data-pipeline-project-474812`.`analytics_staging_staging`.`int_shipments_decomposed`
where delivered_at is null



  
  
      
    ) dbt_internal_test