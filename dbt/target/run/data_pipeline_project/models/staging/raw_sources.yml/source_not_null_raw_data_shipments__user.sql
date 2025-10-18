
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select _user
from `data-pipeline-project-474812`.`raw_data`.`shipments`
where _user is null



  
  
      
    ) dbt_internal_test