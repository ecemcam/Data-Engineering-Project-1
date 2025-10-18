
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select _order
from `data-pipeline-project-474812`.`raw_data`.`shipments`
where _order is null



  
  
      
    ) dbt_internal_test