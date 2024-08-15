-- CTE to identify the first visit date and timestamp for each user
WITH first_visit AS (
  SELECT 
    user_pseudo_id AS user_id,
    MIN(event_date) AS first_visit_date,
    MIN(event_timestamp) AS first_timestamp
  FROM `tc-da-1.turing_data_analytics.raw_events`
  GROUP BY user_pseudo_id
),
-- CTE to identify the first visit timestamp for each user on each date
first_daily_visit AS (
  SELECT 
    event_date,
    user_pseudo_id AS user_id,
    MIN(event_timestamp) AS first_daily_timestamp
  FROM `tc-da-1.turing_data_analytics.raw_events`
  GROUP BY event_date, user_pseudo_id
),
-- CTE to aggregate purchase data for each user
purchases AS (
  SELECT 
    event_date AS purchase_date,
    user_pseudo_id AS user_id,
    event_timestamp AS purchase_timestamp,
    category,
    operating_system,
    browser,
    country,
    traffic_source,
    campaign,
    SUM(purchase_revenue_in_usd) AS revenue,
    COUNT(*) OVER (PARTITION BY user_pseudo_id) AS purchase_count
  FROM `tc-da-1.turing_data_analytics.raw_events`
  WHERE purchase_revenue_in_usd > 0 
  GROUP BY ALL
)
-- main query 
SELECT 
  PARSE_DATE('%Y%m%d', fdv.event_date) AS event_date,
  fdv.user_id,
  FORMAT_TIMESTAMP('%A', TIMESTAMP_SECONDS(CAST(p.purchase_timestamp / 1000000 AS INT64))) AS day_of_week, 
  IF(p.purchase_count = 1, 'new', 'returning') AS user_type,
  p.category,
  p.operating_system,
  p.browser,
  p.country,
  p.traffic_source,
  p.campaign,
  ROW_NUMBER() OVER (PARTITION BY fdv.event_date, fdv.user_id ORDER BY p.purchase_timestamp) AS ranked_purchases, -- assigning a rank to each purchase event per user and date (to identify a first purchase on that same day)
  (p.purchase_timestamp - fdv.first_daily_timestamp) / 1000000 AS duration_in_sec,
  (p.purchase_timestamp - fdv.first_daily_timestamp) / 1000000 / 60 AS duration_in_min,
  p.revenue
FROM first_daily_visit fdv
JOIN purchases p ON fdv.user_id = p.user_id AND fdv.event_date = p.purchase_date
JOIN first_visit fv ON fdv.user_id = fv.user_id
ORDER BY 
  fdv.event_date,
  fdv.user_id;
