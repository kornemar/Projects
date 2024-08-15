  -- CTE to define the events
WITH events AS (
  SELECT
    event_name,
    CASE event_name
      WHEN 'session_start' THEN 1
      WHEN 'view_item' THEN 2
      WHEN 'add_to_cart' THEN 3
      WHEN 'begin_checkout' THEN 4
      WHEN 'purchase' THEN 5
  END AS event_order
  FROM UNNEST(['session_start', 'view_item', 'add_to_cart', 'begin_checkout', 'purchase']) AS event_name ),

  -- CTE to rank countries
  ranked_countries AS (
  SELECT
    event_name,
    country,
    COUNT(DISTINCT user_pseudo_id) AS events,
    RANK() OVER (PARTITION BY event_name ORDER BY COUNT(DISTINCT user_pseudo_id) DESC) AS country_rank
  FROM `tc-da-1.turing_data_analytics.raw_events`
  GROUP BY
    event_name,
    country ),

  -- CTE to add columns for top 3 countries
  top_countries AS (
  SELECT
    event_order,
    events.event_name,
    MAX(CASE WHEN country_rank = 1 THEN ranked_countries.events END) AS first_country_events,
    MAX(CASE WHEN country_rank = 2 THEN ranked_countries.events END) AS second_country_events,
    MAX(CASE WHEN country_rank = 3 THEN ranked_countries.events END) AS third_country_events
  FROM events
  LEFT JOIN ranked_countries
  ON events.event_name = ranked_countries.event_name
  GROUP BY
    event_order,
    events.event_name)

  -- main query to calculate all metrics
SELECT
  *,
  -- a column for conversion rate (vs first stage)
  (first_country_events + second_country_events + third_country_events) / FIRST_VALUE(first_country_events + second_country_events + third_country_events) OVER (ORDER BY event_order) AS conversion_rate,

  -- columns for conversion rate (vs first stage) for each country
  first_country_events / FIRST_VALUE(first_country_events) OVER (ORDER BY event_order) AS first_country_conversion_rate,
  second_country_events / FIRST_VALUE(second_country_events) OVER (ORDER BY event_order) AS second_country_conversion_rate,
  third_country_events / FIRST_VALUE(third_country_events) OVER (ORDER BY event_order) AS third_country_conversion_rate,

  -- a column for drop off in users (from stage to stage)
  (LAG(first_country_events + second_country_events + third_country_events) OVER (ORDER BY event_order) - (first_country_events + second_country_events + third_country_events)) AS drop_off_users,

  -- columns for drop off in users (from stage to stage) for each country
  (LAG(first_country_events) OVER (ORDER BY event_order) - first_country_events) AS first_country_drop_off_users,
  (LAG(second_country_events) OVER (ORDER BY event_order) - second_country_events) AS second_country_drop_off_users,
  (LAG(third_country_events) OVER (ORDER BY event_order) - third_country_events) AS third_country_drop_off_users,

   -- a column for drop off percentage (from stage to stage)
  (LAG(first_country_events + second_country_events + third_country_events) OVER (ORDER BY event_order) - (first_country_events + second_country_events + third_country_events)) / LAG(first_country_events + second_country_events + third_country_events) OVER (ORDER BY event_order) AS drop_off_percentage,

  -- columns for drop off percentage (from stage to stage) for each country
  (LAG(first_country_events) OVER (ORDER BY event_order) - first_country_events) / LAG(first_country_events) OVER (ORDER BY event_order) AS first_country_drop_off_percentage,
  (LAG(second_country_events) OVER (ORDER BY event_order) - second_country_events) / LAG(second_country_events) OVER (ORDER BY event_order) AS second_country_drop_off_percentage,
  (LAG(third_country_events) OVER (ORDER BY event_order) - third_country_events) / LAG(third_country_events) OVER (ORDER BY event_order) AS third_country_drop_off_percentage,

  -- a column for percentage of users who left from the previous stage
  (first_country_events + second_country_events + third_country_events) / LAG(first_country_events + second_country_events + third_country_events) OVER (ORDER BY event_order) AS users_to_next_stage,

  -- columns for percentage of users for each country who left from the previous stage
  first_country_events / LAG(first_country_events) OVER (ORDER BY event_order) AS first_country_users_to_next_stage,
  second_country_events / LAG(second_country_events) OVER (ORDER BY event_order) AS second_country_users_to_next_stage,
  third_country_events / LAG(third_country_events) OVER (ORDER BY event_order) AS third_country_users_to_next_stage

FROM
  top_countries;