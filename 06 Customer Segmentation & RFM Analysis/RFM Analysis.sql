-- CTE to calculate F and M
WITH f_m AS (
  SELECT
    CustomerID,
    Country,
    MAX(InvoiceDate) AS last_purchase_date,
    COUNT(DISTINCT InvoiceNo) AS frequency,
    ROUND(SUM(Quantity * UnitPrice), 2) AS monetary
  FROM `tc-da-1.turing_data_analytics.rfm` AS rfm
  WHERE CustomerID IS NOT NULL
    AND InvoiceDate BETWEEN '2010-12-01' AND '2011-12-02' 
    AND UnitPrice > 0
    AND Quantity > 0
  GROUP BY CustomerID, Country
),
-- CTE to calculate R
r AS (
  SELECT 
    *,
    DATE_DIFF(reference_date, last_purchase_date, DAY) AS recency
    FROM (
        SELECT *,
        MAX(last_purchase_date) OVER () AS reference_date
        FROM f_m
    )
),
-- CTE to calculate quartiles
quartiles AS (
  SELECT
    a.*,
    --All percentiles for RECENCY
    b.percentiles[offset(25)] AS r25,
    b.percentiles[offset(50)] AS r50,
    b.percentiles[offset(75)] AS r75,
    b.percentiles[offset(100)] AS r100,
    --All percentiles for FREQUENCY
    c.percentiles[offset(25)] AS f25,
    c.percentiles[offset(50)] AS f50,
    c.percentiles[offset(75)] AS f75,
    c.percentiles[offset(100)] AS f100,
    --All percentiles for MONETARY
    d.percentiles[offset(25)] AS m25,
    d.percentiles[offset(50)] AS m50,
    d.percentiles[offset(75)] AS m75,
    d.percentiles[offset(100)] AS m100
  FROM r a,
    (SELECT APPROX_QUANTILES(recency, 100) percentiles 
    FROM r) b,
        (SELECT APPROX_QUANTILES(frequency, 100) percentiles 
    FROM r) c,
        (SELECT APPROX_QUANTILES(monetary, 100) percentiles 
    FROM r) d
),
-- CTE to assign scores for each RFM metric
scores AS (
  SELECT 
    *,
    CAST(ROUND((f_score + m_score) / 2, 0) AS INT64) AS fm_score,
    r_score || f_score || m_score AS rfm_score
    FROM (
        SELECT 
          *,
        --Recency scoring is reversed
          CASE WHEN recency <= r25 THEN 4
              WHEN recency <= r50 AND recency > r25 THEN 3
              WHEN recency <= r75 AND recency > r50 THEN 2
              WHEN recency > r75 THEN 1
          END AS r_score,
          CASE WHEN frequency <= f25 THEN 1
              WHEN frequency <= f50 AND frequency > f25 THEN 2
              WHEN frequency <= f75 AND frequency > f50 THEN 3
              WHEN frequency > f75 THEN 4
          END AS f_score,
          CASE WHEN monetary <= m25 THEN 1
              WHEN monetary <= m50 AND monetary > m25 THEN 2
              WHEN monetary <= m75 AND monetary > m50 THEN 3
              WHEN monetary > m75 THEN 4
          END AS m_score
        FROM quartiles
        )
),
-- CTE to assign RFM scores to segments
segments AS (
  SELECT
    CustomerID,
    Country,
    recency,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    rfm_score,
    CASE WHEN rfm_score IN('444','344') THEN 'Champions'
         WHEN rfm_score IN('434','334','233') THEN 'Loyal Customers'
         WHEN rfm_score IN('443','433','424','423','343','333','324','323') THEN 'Potential Loyalists'
         WHEN rfm_score IN('422','421','414','413','412','411','314','313') THEN 'Recent Customers'
         WHEN rfm_score IN('442','441','432','431','342','341','332','331','322','321','312','311') THEN 'Promising Customers'
         WHEN rfm_score = '244' THEN 'Needing Attention'
         WHEN rfm_score IN('231','133','132','131','123','113','232','224','223','214','242','241','142','141','124','114') THEN 'About to Sleep'
         WHEN rfm_score IN('243','234') THEN 'At Risk'
         WHEN rfm_score IN('144','143','134') THEN 'Can\'t Lose Them'
         WHEN rfm_score IN ('222','221','213','212','211','122','121','112') THEN 'Hibernating'
         WHEN rfm_score = '111' THEN 'Lost'
    ELSE 'Uncategorized'
    END AS rfm_segment
  FROM scores
)
-- main query
SELECT *
FROM segments;
