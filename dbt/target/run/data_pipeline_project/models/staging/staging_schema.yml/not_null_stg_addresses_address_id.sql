
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select address_id
from `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_addresses`
where address_id is null



  
  
      
    ) dbt_internal_test