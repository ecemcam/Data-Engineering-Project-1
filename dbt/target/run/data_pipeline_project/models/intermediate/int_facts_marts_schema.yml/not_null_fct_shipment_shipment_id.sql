
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select shipment_id
from `data-pipeline-project-474812`.`analytics_staging_marts`.`fct_shipment`
where shipment_id is null



  
  
      
    ) dbt_internal_test