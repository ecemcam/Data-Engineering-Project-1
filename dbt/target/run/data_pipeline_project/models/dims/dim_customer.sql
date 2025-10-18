
  
    

    create or replace table `data-pipeline-project-474812`.`analytics_staging_marts`.`dim_customer`
      
    
    

    
    OPTIONS()
    as (
      


with users as (
    -- Start with the clean user base (PK: user_id)
    select distinct -- ADDED: This guarantees only one record per user_id in the dimension table
        user_id,
        created_at
    from `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_users`
),

addresses as (
    -- Link users to their primary address/location (FK: user_id)
    select * from `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_addresses`
),

neighborhoods as (
    -- Descriptive geography (FK: neighborhood_id, city_id, country_id)
    select * from `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_neighborhoods`
),

cities as (
    select * from `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_cities`
),

states as (
    select * from `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_states`
),

countries as (
    select * from `data-pipeline-project-474812`.`analytics_staging_staging`.`stg_countries`
),

-- Join everything into a single, comprehensive dimension
final as (
    select
        u.user_id as customer_id, -- Rename PK to customer_id for the dim table
        u.created_at as signup_date,
     
        -- Address and Invoice Details
        a.invoice_type,
        
        -- Neighborhood Details
        n.neighborhood_name as neighborhood, 
        
        -- City, State, Country Details (The full hierarchy)
        c.city_name as city,
        s.state_name as state,
        co.country_name as country,

        n.postal_code
        
    from users u
    -- Join to get the address link (assuming a user has one primary address record for simplicity)
    left join addresses a
        on u.user_id = a.user_id
    -- Join up the geography hierarchy
    left join neighborhoods n
        on a.neighborhood_id = n.neighborhood_id
    left join cities c
        on n.city_id = c.city_id
    left join states s
        on c.state_id = s.state_id
    left join countries co
        on c.country_id = co.country_id
)

select * from final
    );
  