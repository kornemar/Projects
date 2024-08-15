-- CTE to calculate revenue per purchase week
WITH purchases AS (
SELECT
    user_pseudo_id,
    DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK) AS purchase_week,
    SUM(purchase_revenue_in_usd) AS revenue
  FROM
    `tc-da-1.turing_data_analytics.raw_events` AS events
  WHERE
    event_date < '20210131' 
    AND purchase_revenue_in_usd > 0
  GROUP BY
    user_pseudo_id, 
    purchase_week
),
-- CTE to find registration week
registrations AS (
  SELECT
    user_pseudo_id,
    MIN(DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK)) AS reg_week
  FROM
    `tc-da-1.turing_data_analytics.raw_events` AS events 
  WHERE
    event_date < '20210131'
  GROUP BY
    user_pseudo_id 
),
-- CTE to gather all metrics together
metrics AS (
  SELECT 
    reg_week,
    purchase_week,
    registrations.user_pseudo_id,
    SUM(revenue) AS revenue
  FROM registrations
  LEFT JOIN purchases
  ON registrations.user_pseudo_id = purchases.user_pseudo_id
  GROUP BY 
    reg_week, 
    purchase_week, 
    registrations.user_pseudo_id
  ORDER BY 
    reg_week, 
    purchase_week
),
-- CTE to calculate average revenue per user by registration weeks
calculations AS (
  SELECT 
    reg_week,
    COUNT(DISTINCT user_pseudo_id) AS users,
    SUM(CASE WHEN purchase_week = reg_week THEN revenue END)/COUNT(DISTINCT user_pseudo_id) AS week_0,
    SUM(CASE WHEN purchase_week = reg_week + INTERVAL 1 WEEK THEN revenue END)/COUNT(DISTINCT user_pseudo_id) AS week_1,
    SUM(CASE WHEN purchase_week = reg_week + INTERVAL 2 WEEK THEN revenue END)/COUNT(DISTINCT user_pseudo_id) AS week_2,
    SUM(CASE WHEN purchase_week = reg_week + INTERVAL 3 WEEK THEN revenue END)/COUNT(DISTINCT user_pseudo_id) AS week_3,
    SUM(CASE WHEN purchase_week = reg_week + INTERVAL 4 WEEK THEN revenue END)/COUNT(DISTINCT user_pseudo_id) AS week_4,
    SUM(CASE WHEN purchase_week = reg_week + INTERVAL 5 WEEK THEN revenue END)/COUNT(DISTINCT user_pseudo_id) AS week_5,
    SUM(CASE WHEN purchase_week = reg_week + INTERVAL 6 WEEK THEN revenue END)/COUNT(DISTINCT user_pseudo_id) AS week_6,
    SUM(CASE WHEN purchase_week = reg_week + INTERVAL 7 WEEK THEN revenue END)/COUNT(DISTINCT user_pseudo_id) AS week_7,
    SUM(CASE WHEN purchase_week = reg_week + INTERVAL 8 WEEK THEN revenue END)/COUNT(DISTINCT user_pseudo_id) AS week_8,
    SUM(CASE WHEN purchase_week = reg_week + INTERVAL 9 WEEK THEN revenue END)/COUNT(DISTINCT user_pseudo_id) AS week_9,
    SUM(CASE WHEN purchase_week = reg_week + INTERVAL 10 WEEK THEN revenue END)/COUNT(DISTINCT user_pseudo_id) AS week_10,
    SUM(CASE WHEN purchase_week = reg_week + INTERVAL 11 WEEK THEN revenue END)/COUNT(DISTINCT user_pseudo_id) AS week_11,
    SUM(CASE WHEN purchase_week = reg_week + INTERVAL 12 WEEK THEN revenue END)/COUNT(DISTINCT user_pseudo_id) AS week_12,
    SUM(revenue) AS total_revenue,
    SUM(revenue)/COUNT(DISTINCT user_pseudo_id) AS revenue_per_user
  FROM metrics
  GROUP BY 
    reg_week
  ORDER BY
    reg_week
)
-- main query
SELECT * 
FROM calculations;