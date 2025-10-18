
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select latest_status
from `data-pipeline-project-474812`.`analytics_staging_staging`.`int_shipments_decomposed`
where latest_status is null



  
  
      
    ) dbt_internal_test