

with meet_condition as(
  select *
  from `data-pipeline-project-474812`.`analytics_staging_marts`.`mart_acquisition_efficiency`
),

validation_errors as (
  select *
  from meet_condition
  where
    -- never true, defaults to an empty result set. Exists to ensure any combo of the `or` clauses below succeeds
    1 = 2
    -- records with a value >= min_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not daily_customer_acquisition_cost >= 0
)

select *
from validation_errors

