-- CTE extracts initial event data and computes the previous event timestamp for each user
WITH prev_events AS (
  SELECT
    user_pseudo_id,
    event_timestamp,
    TIMESTAMP_SECONDS(CAST(event_timestamp / 1000000 AS INT64)) AS converted_timestamp, -- converts from microseconds to seconds and then into a timestamp format
    event_name,
    category,
    operating_system,
    browser,
    country,
    traffic_source,
    page_title,
    campaign,
    LAG(event_timestamp) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS prev_event, -- calculates the previous event timestamp for each user, partioned by user_pseudo_id and ordered by event_timestamp
    purchase_revenue_in_usd AS revenue
  FROM
    `tc-da-1.turing_data_analytics.raw_events`
),
-- CTE determines whether an event starts a new session based on a 30-minute inactivity period
new_sessions AS (
  SELECT
    *,
    IF((event_timestamp - prev_event) >= (60 * 30 * 1000000) OR prev_event IS NULL, 1, 0) AS new_session
  FROM
    prev_events
),
-- CTE assigns session IDs by summing the new_session flags and calculates additional metrics
session_data AS (
  SELECT
    user_pseudo_id,
    event_timestamp,
    converted_timestamp,
    event_name,
    category,
    operating_system,
    browser,
    country,
    traffic_source,
    page_title,
    campaign,
    IF(campaign IS NULL OR campaign IN('(direct)','(referral)','(organic)','<Other>','(data deleted)'), 0, 1) AS is_campaign,
    revenue,
    SUM(new_session) OVER (ORDER BY user_pseudo_id, event_timestamp) AS session_id,
    SUM(new_session) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS user_session_id,
    IF(new_session = 0, (event_timestamp - prev_event)/1000000, 0) AS duration_in_sec
  FROM
    new_sessions
),
-- CTE computes the start time of each session
session_start_data AS (
  SELECT
    session_id,
    MIN(converted_timestamp) AS session_start
  FROM
    session_data
  GROUP BY
    session_id
)
-- Final query aggregates the final metrics for each session
SELECT
  sd.session_id,
  user_session_id,
  user_pseudo_id,
  FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP(session_start)) AS session_start_formatted,
  FORMAT_TIMESTAMP('%A', session_start) AS day_of_week, -- formats the timestamp to display the full name of the day of the week
  IF(EXTRACT(DAYOFWEEK FROM session_start) IN (1, 7), 'Weekend', 'Weekday') AS day_type,
  category,
  operating_system,
  browser,
  #STRING_AGG(DISTINCT browser LIMIT 1) AS browser,
  country,
  STRING_AGG(DISTINCT traffic_source LIMIT 1) AS traffic_source, -- aggregate function, that concatenates unique values from multiple rows into a single string, limiting the result to one value
  IF(campaign IS NULL OR campaign IN('(direct)','(referral)','(organic)','<Other>','(data deleted)'), 'no data', campaign) AS campaign_name,
  is_campaign,
  COUNT(event_name) AS event_count,
  COUNT(DISTINCT page_title) AS page_count,
  IF(COUNT(DISTINCT page_title) = 1, 1, 0) AS is_single_page_session,
  SUM(IF(event_name = 'page_view', 1, 0)) AS page_views,
  SUM(revenue) AS revenue,
  IF(SUM(revenue) > 0, 1, 0) AS is_purchase,
  SUM(duration_in_sec) AS ttl_duration_in_sec,
  IF(SUM(duration_in_sec) = 0, 1, 0) AS is_0_sec_session
FROM
  session_data sd
JOIN
  session_start_data ssd
ON
  sd.session_id = ssd.session_id
GROUP BY ALL
ORDER BY
  sd.session_id,
  sd.user_pseudo_id;

