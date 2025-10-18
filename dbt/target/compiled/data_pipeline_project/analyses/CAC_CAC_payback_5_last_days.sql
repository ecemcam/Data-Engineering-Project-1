-- Query 3: CAC and Payback Period by Channel - Last 5 Days
WITH max_data_date AS (
  SELECT MAX(date) AS max_date
  FROM `data-pipeline-project-474812.analytics_staging_marts.fct_marketing_spend`
),

channel_cac AS (
  SELECT
    t1.channel,
    ROUND(AVG(
      CASE 
        WHEN COALESCE(t2.new_subscribers, 0) > 0 
        THEN t1.spend_try / t2.new_subscribers
        ELSE NULL
      END
    ), 2) AS avg_cac_tl
  FROM `data-pipeline-project-474812.analytics_staging_marts.fct_marketing_spend` t1
  LEFT JOIN `data-pipeline-project-474812.analytics_staging_marts.mart_subscription_daily` t2
    ON t1.date = t2.date
  WHERE t1.date >= DATE_SUB((SELECT max_date FROM max_data_date), INTERVAL 5 DAY)
  GROUP BY t1.channel
),

kpi_snapshot AS (
  SELECT
    monthly_revenue_per_active_sub,
    cac_payback_months
  FROM `data-pipeline-project-474812.analytics_staging_marts.mart_kpis_latest`
  LIMIT 1
)

SELECT
  cc.channel,
  cc.avg_cac_tl AS cac_tl,
  ROUND(
    cc.avg_cac_tl / NULLIF(k.monthly_revenue_per_active_sub, 0),
    2
  ) AS payback_period_months
FROM channel_cac cc
CROSS JOIN kpi_snapshot k
ORDER BY cac_tl ASC