> **Customer Lifetime Value (CLV)** predicts how much revenue (or profit) you can expect from a single customer using models like Predictive CLV or Historical CLV. CLV not only provides additional insights into your business and customers but also helps determine how much you can afford to spend on acquiring customers. This technique is crucial for sales and marketing analysis, as well as for the overall business strategy. With an accurate CLV measurement, you can track it over time to understand how changes, strategies, or other factors impact CLV and whether you can grow it.

## Objectives
- Learn basic terms used in CLV modeling
- Understand the benefits of calculating CLV and how this technique can improve your business
- Learn how to use cohorts in CLV modeling
- Learn how to predict customers’ CLV

 
## Data Source
`turing_data_analytics.raw_events`

## Task
Using cohort analysis, calculate what revenue you can expect in the future:

1. Calculate **Weekly Average Revenue by Cohorts**: determine the weekly average revenue by dividing the weekly revenue by the number of weekly registrations. Since the concept of registration does not exist on this site, use the first visit to the website as the registration date (registration cohort). 
2. Calculate **Cumulative Revenue by Cohorts**: create a chart that shows cumulative revenue per cohort week, calculated as the cumulative sum of revenue divided by registrations for each week. Below this chart, calculate the averages for all weeks since registration, and further down, calculate the percentage growth based on these average numbers.
3. Calculate **Revenue Prediction by Cohorts**: estimate future revenue by predicting missing data, particularly the revenue expected from later-acquired user cohorts.

## Result

Add SQL file  
[Google Sheets](https://docs.google.com/spreadsheets/d/1_ngT6uBGt8Ij-Y600t0SYe-8VSpaXJp6CN7ipINExGo/edit?usp=sharing)
