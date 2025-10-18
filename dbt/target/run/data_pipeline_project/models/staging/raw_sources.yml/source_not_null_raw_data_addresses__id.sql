
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select _id
from `data-pipeline-project-474812`.`raw_data`.`addresses`
where _id is null



  
  
      
    ) dbt_internal_test