  --- CTE to identify cohort data
WITH
  cohort_data AS (
  SELECT
    user_pseudo_id,
    DATE(MIN(subscription_start)) AS start_date,
    DATE(MAX(subscription_end)) AS end_date,
    CASE WHEN EXTRACT(WEEK FROM DATE(MIN(subscription_start))) = 0 THEN 52 
         ELSE EXTRACT(WEEK FROM DATE(MIN(subscription_start))) END AS subscription_week
  FROM
    turing_data_analytics.subscriptions
  GROUP BY
    user_pseudo_id )
SELECT
  subscription_week,
  COUNT(DISTINCT user_pseudo_id) AS total_users,
  COUNT(DISTINCT
    CASE WHEN end_date IS NULL OR end_date >= start_date + INTERVAL 7 day 
         THEN user_pseudo_id END)/COUNT(DISTINCT user_pseudo_id) AS week_0,
  CASE WHEN subscription_week <> 5 THEN 
  COUNT(DISTINCT 
    CASE WHEN end_date IS NULL OR end_date >= start_date + INTERVAL 14 day 
         THEN user_pseudo_id END)/COUNT(DISTINCT user_pseudo_id) ELSE NULL END AS week_1,
  CASE WHEN subscription_week NOT IN (4, 5) THEN 
  COUNT(DISTINCT 
    CASE WHEN end_date IS NULL OR end_date >= start_date + INTERVAL 21 day 
         THEN user_pseudo_id END)/COUNT(DISTINCT user_pseudo_id) ELSE NULL END AS week_2,
  CASE WHEN subscription_week NOT IN (3, 4, 5) THEN 
  COUNT(DISTINCT 
    CASE WHEN end_date IS NULL OR end_date >= start_date + INTERVAL 28 day 
         THEN user_pseudo_id END)/COUNT(DISTINCT user_pseudo_id) ELSE NULL END AS week_3,
  CASE WHEN subscription_week NOT IN (2, 3, 4, 5) THEN 
  COUNT(DISTINCT 
    CASE WHEN end_date IS NULL OR end_date >= start_date + INTERVAL 35 day 
         THEN user_pseudo_id END)/COUNT(DISTINCT user_pseudo_id) ELSE NULL END AS week_4,
  CASE WHEN subscription_week NOT IN (1, 2, 3, 4, 5) THEN 
  COUNT(DISTINCT 
    CASE WHEN end_date IS NULL OR end_date >= start_date + INTERVAL 42 day 
         THEN user_pseudo_id END)/COUNT(DISTINCT user_pseudo_id) ELSE NULL END AS week_5,
  CASE WHEN subscription_week NOT IN (52, 1, 2, 3, 4, 5) THEN 
  COUNT(DISTINCT 
    CASE WHEN end_date IS NULL OR end_date >= start_date + INTERVAL 49 day 
         THEN user_pseudo_id END)/COUNT(DISTINCT user_pseudo_id) ELSE NULL END AS week_6
FROM
  cohort_data 
GROUP BY
  subscription_week;