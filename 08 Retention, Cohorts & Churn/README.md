> **Cohort analysis** groups users into coherent cohorts, providing more insight than simple overall metrics like Monthly Active Users (MAU) or Daily Active Users (DAU). It lays the foundation for a better understanding of user behavior.
>
> **Churn and retention analyses** are crucial for web, e-commerce, and subscription-based businesses. These analyses, often based on cohort analysis, help determine how long users stay with a product and how much revenue they generate during their lifetime.

## Objectives

- Understand what cohort analysis is and its benefits compared to other conventional methods of understanding customers
- Learn about retention/churn analysis, including different types and when to use each. Gain hands-on experience conducting retention/churn analysis using spreadsheets and SQL

## Data Source
BigQuery `turing_data_analytics.subscriptions`

## Task

Analyze weekly subscription data to determine how many subscribers started their subscriptions in a particular week and how many remained active over the following six weeks. The final result should display weekly retention cohorts, showing retention from week 0 to week 6 for each week in the dataset.

## Result
- [Cohort Analysis](https://docs.google.com/spreadsheets/d/10wNPVGrrLjut911UMVo3CuD4bLdumWizypyo0nrE4TE/edit?usp=sharing) in Google Sheets
- [SQL](https://github.com/kornemar/Projects/blob/main/08%20Retention%2C%20Cohorts%20%26%20Churn/Cohort%20Analysis.sql)
<details>
  <summary>
    Expand SQL code
  </summary>

```sql
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
```

</details>

