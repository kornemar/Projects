> SQL is an essential skill for data analysts and serves as a great introduction to databases, data storage, and data retrieval intricacies.

## Objectives

Learn the fundamentals of SQL and its use databases, followed by an exploration of advanced SQL functionalities (subqueries, CTEs, window functions), and the basic concepts of data cleaning and validation. 

## Data Source
- BigQuery `adwentureworks_db`
- [AdventureWorks_2005.pdf](https://drive.google.com/file/d/1-Qsnn3bg0_PYgY5kKJOUDG8xdKLvOLPK/view?usp=sharing)

## Tasks with Solutions

- ### **Task 1**

> Create a detailed **overview of all individual customers**. Copy only the top 200 rows from your query, ordered by total amount (including tax). Some customers have multiple addresses; to avoid duplicate data, select their latest available address by choosing the maximum `AddressId`.

<details>
  <summary>
    Expand Solution 1
  </summary>
  
```sql
  -- CTE to encapsulate the main query's logic and simplify the query
WITH
  customer_overview AS (
    -- CTE to find the latest address for each customer with multiple addresses (nested within customer_overview CTE)
  WITH
    latest_address AS (
    SELECT
      customer_address.CustomerID,
      MAX(customer_address.AddressID) AS latest_address_id
    FROM
      tc-da-1.adwentureworks_db.customeraddress AS customer_address
    GROUP BY
      customer_address.CustomerID )
    -- the end of the latest_address CTE
  SELECT
    customer.CustomerID,
    contact.FirstName,
    contact.LastName,
    CONCAT(FirstName, ' ', LastName) AS full_name,
  IF
    (Title IS NULL, CONCAT('Dear', ' ', LastName), CONCAT(Title, ' ', LastName)) AS addressing_title,
    contact.EmailAddress,
    contact.Phone,
    customer.AccountNumber,
    customer.CustomerType,
    address.City,
    address.AddressLine1,
    address.AddressLine2,
    state.Name AS State,
    country.Name AS Country,
    COUNT(sales.SalesOrderID) AS number_orders,
    ROUND(SUM(sales.TotalDue), 3) AS total_amount,
    MAX(sales.OrderDate) AS date_last_order
  FROM
    tc-da-1.adwentureworks_db.customer AS customer
  JOIN
    tc-da-1.adwentureworks_db.individual AS individual
  ON
    customer.CustomerID = individual.CustomerID
  JOIN
    tc-da-1.adwentureworks_db.contact AS contact
  ON
    individual.ContactID = contact.ContactID
  JOIN
    latest_address  -- joining the latest_address CTE to get the latest address for each customer
  ON
    customer.CustomerID = latest_address.CustomerID
  JOIN
    tc-da-1.adwentureworks_db.address AS address
  ON
    latest_address.latest_address_id = address.AddressID
  JOIN
    tc-da-1.adwentureworks_db.salesorderheader AS sales
  ON
    customer.CustomerID = sales.CustomerID
  JOIN
    tc-da-1.adwentureworks_db.stateprovince AS state
  ON
    address.StateProvinceID = state.StateProvinceID
  JOIN
    tc-da-1.adwentureworks_db.countryregion AS country
  ON
    state.CountryRegionCode = country.CountryRegionCode
  WHERE
    CustomerType = 'I'
  GROUP BY
    customer.CustomerID,
    FirstName,
    LastName,
    Title,
    EmailAddress,
    Phone,
    AccountNumber,
    CustomerType,
    City,
    AddressLine1,
    AddressLine2,
    State,
    Country)
  -- end of CTE customer_overview
  -- querying customer_overview CTE to get top 200 customers ordered by total amount (with tax)
SELECT
  *
FROM
  customer_overview
ORDER BY
  total_amount DESC
LIMIT
  200;
```

</details>

- ### **Task 2**

> Retrieve data for the **top 200 customers with the highest total amount (including tax) who have not placed an order in the last 365 days**. Note that the database is outdated, so define the current date by finding the latest order date in the orders table.

<details>
  <summary>
    Expand Solution 2
  </summary>

```sql
  -- CTE to encapsulate the main query's logic and simplify the query
WITH
  customer_overview AS (
    -- CTE to find the latest address for each customer with multiple addresses (nested within customer_overview CTE)
  WITH
    latest_address AS (
    SELECT
      customer_address.CustomerID,
      MAX(customer_address.AddressID) AS latest_address_id
    FROM
      tc-da-1.adwentureworks_db.customeraddress AS customer_address
    GROUP BY
      customer_address.CustomerID ),
    -- CTE to find the current date which (the latest order date)
    cur_date AS (
    SELECT
      MAX(sales.OrderDate) AS cur_date
    FROM
      tc-da-1.adwentureworks_db.salesorderheader AS sales )
    -- main query
  SELECT
    customer.CustomerID,
    contact.FirstName,
    contact.LastName,
    CONCAT(FirstName, ' ', LastName) AS full_name,
  IF
    (Title IS NULL, CONCAT('Dear', ' ', LastName), CONCAT(Title, ' ', LastName)) AS addressing_title,
    contact.EmailAddress,
    contact.Phone,
    customer.AccountNumber,
    customer.CustomerType,
    address.City,
    address.AddressLine1,
    address.AddressLine2,
    state.Name AS State,
    country.Name AS Country,
    COUNT(sales.SalesOrderID) AS number_orders,
    ROUND(SUM(sales.TotalDue), 3) AS total_amount,
    MAX(sales.OrderDate) AS date_last_order,
    cur_date.cur_date,
    TIMESTAMP_DIFF(cur_date.cur_date, MAX(sales.OrderDate), DAY) AS cur_vs_last
  FROM
    tc-da-1.adwentureworks_db.customer AS customer
  JOIN
    tc-da-1.adwentureworks_db.individual AS individual
  ON
    customer.CustomerID = individual.CustomerID
  JOIN
    tc-da-1.adwentureworks_db.contact AS contact
  ON
    individual.ContactID = contact.ContactID
  JOIN
    latest_address
  ON
    customer.CustomerID = latest_address.CustomerID
  JOIN
    tc-da-1.adwentureworks_db.address AS address
  ON
    latest_address.latest_address_id = address.AddressID
  JOIN
    tc-da-1.adwentureworks_db.salesorderheader AS sales
  ON
    customer.CustomerID = sales.CustomerID
  JOIN
    tc-da-1.adwentureworks_db.stateprovince AS state
  ON
    address.StateProvinceID = state.StateProvinceID
  JOIN
    tc-da-1.adwentureworks_db.countryregion AS country
  ON
    state.CountryRegionCode = country.CountryRegionCode
  CROSS JOIN
    -- to include the current date for every row of the main query
    cur_date
  WHERE
    CustomerType = 'I'
  GROUP BY
    customer.CustomerID,
    FirstName,
    LastName,
    Title,
    EmailAddress,
    Phone,
    AccountNumber,
    CustomerType,
    City,
    AddressLine1,
    AddressLine2,
    State,
    Country,
    cur_date)
  -- end of CTE customer_overview
  -- querying CTE to get top 200 customers with the highest total amount (with tax) who have not ordered for the last 365 days
SELECT
  CustomerID,
  FirstName,
  LastName,
  full_name,
  addressing_title,
  EmailAddress,
  Phone,
  AccountNumber,
  CustomerType,
  City,
  AddressLine1,
  AddressLine2,
  State,
  Country,
  number_orders,
  total_amount,
  date_last_order
FROM
  customer_overview
WHERE
  cur_vs_last > 365
ORDER BY
  total_amount DESC
LIMIT
  200;
```

</details>

- ### **Task 3**

> Create a **new column in the view that marks customers as active or inactive based on whether they have placed an order in the last 365 days**. Copy only the top 500 rows from your query, ordered by `CustomerId` in descending order.

<details>
  <summary>
    Expand Solution 3
  </summary>

```sql
  -- CTE to encapsulate the main query's logic and simplify the query
WITH
  customer_overview AS (
    -- CTE to find the latest address for each customer with multiple addresses (nested within customer_overview CTE)
  WITH
    latest_address AS (
    SELECT
      customer_address.CustomerID,
      MAX(customer_address.AddressID) AS latest_address_id
    FROM
      tc-da-1.adwentureworks_db.customeraddress AS customer_address
    GROUP BY
      customer_address.CustomerID ),
    -- CTE to find the current date which (the latest order date)
    cur_date AS (
    SELECT
      MAX(sales.OrderDate) AS cur_date
    FROM
      tc-da-1.adwentureworks_db.salesorderheader AS sales )
    -- main query
  SELECT
    customer.CustomerID,
    contact.FirstName,
    contact.LastName,
    CONCAT(FirstName, ' ', LastName) AS full_name,
  IF
    (Title IS NULL, CONCAT('Dear', ' ', LastName), CONCAT(Title, ' ', LastName)) AS addressing_title,
    contact.EmailAddress,
    contact.Phone,
    customer.AccountNumber,
    customer.CustomerType,
    address.City,
    address.AddressLine1,
    address.AddressLine2,
    state.Name AS State,
    country.Name AS Country,
    COUNT(sales.SalesOrderID) AS number_orders,
    ROUND(SUM(sales.TotalDue), 3) AS total_amount,
    MAX(sales.OrderDate) AS date_last_order,
    cur_date.cur_date,
    TIMESTAMP_DIFF(cur_date.cur_date, MAX(sales.OrderDate), DAY) AS cur_vs_last,
    -- the column marks active & inactive customers based on whether they have ordered anything during the last 365 days
    CASE
      WHEN TIMESTAMP_DIFF(cur_date.cur_date, MAX(sales.OrderDate), DAY) <= 365 THEN 'active'
    ELSE
    'inactive'
  END
    AS active_customer_flag
  FROM
    tc-da-1.adwentureworks_db.customer AS customer
  JOIN
    tc-da-1.adwentureworks_db.individual AS individual
  ON
    customer.CustomerID = individual.CustomerID
  JOIN
    tc-da-1.adwentureworks_db.contact AS contact
  ON
    individual.ContactID = contact.ContactID
  JOIN
    latest_address
  ON
    customer.CustomerID = latest_address.CustomerID
  JOIN
    tc-da-1.adwentureworks_db.address AS address
  ON
    latest_address.latest_address_id = address.AddressID
  JOIN
    tc-da-1.adwentureworks_db.salesorderheader AS sales
  ON
    customer.CustomerID = sales.CustomerID
  JOIN
    tc-da-1.adwentureworks_db.stateprovince AS state
  ON
    address.StateProvinceID = state.StateProvinceID
  JOIN
    tc-da-1.adwentureworks_db.countryregion AS country
  ON
    state.CountryRegionCode = country.CountryRegionCode
  CROSS JOIN
    cur_date
  WHERE
    CustomerType = 'I'
  GROUP BY
    customer.CustomerID,
    FirstName,
    LastName,
    Title,
    EmailAddress,
    Phone,
    AccountNumber,
    CustomerType,
    City,
    AddressLine1,
    AddressLine2,
    State,
    Country,
    cur_date)
  -- end of CTE customer_overview
  -- querying CTE to get top 500 rows ordered by CustomerId desc
SELECT
  CustomerID,
  FirstName,
  LastName,
  full_name,
  addressing_title,
  EmailAddress,
  Phone,
  AccountNumber,
  CustomerType,
  City,
  AddressLine1,
  AddressLine2,
  State,
  Country,
  number_orders,
  total_amount,
  date_last_order,
  active_customer_flag
FROM
  customer_overview
ORDER BY
  CustomerID DESC
LIMIT
  500;
```

</details>

- ### **Task 4**

> Extract data on **all active customers from North America**. Only include customers who have either a total order amount of at least 2500 (including tax) or have placed 5 or more orders. In the output, split their address line into two columns. Order the output by country, state, and `date_last_order`.

<details>
  <summary>
    Expand Solution 4
  </summary>

```sql
  -- CTE to encapsulate the main query's logic and simplify the query
WITH
  customer_overview AS (
    -- CTE to find the latest address for each customer with multiple addresses (nested within customer_overview CTE)
  WITH
    latest_address AS (
    SELECT
      customer_address.CustomerID,
      MAX(customer_address.AddressID) AS latest_address_id
    FROM
      tc-da-1.adwentureworks_db.customeraddress AS customer_address
    GROUP BY
      customer_address.CustomerID ),
    -- CTE to find the current date which (the latest order date)
    cur_date AS (
    SELECT
      MAX(sales.OrderDate) AS cur_date
    FROM
      tc-da-1.adwentureworks_db.salesorderheader AS sales )
    -- main query
  SELECT
    customer.CustomerID,
    contact.FirstName,
    contact.LastName,
    CONCAT(FirstName, ' ', LastName) AS full_name,
  IF
    (Title IS NULL, CONCAT('Dear', ' ', LastName), CONCAT(Title, ' ', LastName)) AS addressing_title,
    contact.EmailAddress,
    contact.Phone,
    customer.AccountNumber,
    customer.CustomerType,
    address.City,
    address.AddressLine1,
    -- created two columns (address_no, address_st) by applying regular expressions
    CAST(REGEXP_EXTRACT(AddressLine1, r'^(\d+)') AS INT64) AS address_no, -- searching for a sequence of one or more digits at the beginning of the AddressLine1 text, the captured digits are then cast to the INT64 data type so that the address_no column contains numeric values only
    REGEXP_REPLACE(AddressLine1, r'^\d+,?\s*', '') AS address_st, -- removing the numeric part and an optional comma and space from the AddressLine1 text
    address.AddressLine2,
    state.Name AS State,
    country.Name AS Country,
    sales_territory.Group AS territory,
    COUNT(sales.SalesOrderID) AS number_orders,
    ROUND(SUM(sales.TotalDue), 3) AS total_amount,
    MAX(sales.OrderDate) AS date_last_order,
    cur_date.cur_date,
    TIMESTAMP_DIFF(cur_date.cur_date, MAX(sales.OrderDate), DAY) AS cur_vs_last,
    CASE
      WHEN TIMESTAMP_DIFF(cur_date.cur_date, MAX(sales.OrderDate), DAY) <= 365 THEN 'active'
    ELSE
    'inactive'
  END
    AS active_customer_flag
  FROM
    tc-da-1.adwentureworks_db.customer AS customer
  JOIN
    tc-da-1.adwentureworks_db.individual AS individual
  ON
    customer.CustomerID = individual.CustomerID
  JOIN
    tc-da-1.adwentureworks_db.contact AS contact
  ON
    individual.ContactID = contact.ContactID
  JOIN
    latest_address
  ON
    customer.CustomerID = latest_address.CustomerID
  JOIN
    tc-da-1.adwentureworks_db.address AS address
  ON
    latest_address.latest_address_id = address.AddressID
  JOIN
    tc-da-1.adwentureworks_db.salesorderheader AS sales
  ON
    customer.CustomerID = sales.CustomerID
  JOIN
    tc-da-1.adwentureworks_db.salesterritory AS sales_territory
  ON
    sales.TerritoryID = sales_territory.TerritoryID
  JOIN
    tc-da-1.adwentureworks_db.stateprovince AS state
  ON
    address.StateProvinceID = state.StateProvinceID
  JOIN
    tc-da-1.adwentureworks_db.countryregion AS country
  ON
    state.CountryRegionCode = country.CountryRegionCode
  CROSS JOIN
    cur_date
  WHERE
    CustomerType = 'I'
  GROUP BY
    customer.CustomerID,
    FirstName,
    LastName,
    Title,
    EmailAddress,
    Phone,
    AccountNumber,
    CustomerType,
    City,
    AddressLine1,
    AddressLine2,
    State,
    Country,
    territory,
    cur_date)
  -- end of CTE customer_overview
  -- querying CTE to get all active customers from North America (who have either ordered no less than 2500 in total amount (with Tax) or ordered 5 + times)
SELECT
  CustomerID,
  FirstName,
  LastName,
  full_name,
  addressing_title,
  EmailAddress,
  Phone,
  AccountNumber,
  CustomerType,
  City,
  AddressLine1,
  address_no,
  address_st,
  AddressLine2,
  State,
  Country,
  number_orders,
  total_amount,
  date_last_order,
  active_customer_flag
FROM
  customer_overview
WHERE
  active_customer_flag = 'active'
  AND territory = 'North America'
  AND (total_amount >= 2500
    OR number_orders >= 5)
ORDER BY
  Country,
  State,
  date_last_order DESC;
```

</details>

- ### **Task 5**

> Create a query to calculate **monthly sales numbers in each country and region**. The query should include the number of orders, customers, and salespersons in each month, along with the total amount earned (including tax). Sales data for all types of customers should be included.

<details>
  <summary>
    Expand Solution 5
  </summary>

```sql
SELECT
  LAST_DAY(DATE_TRUNC(DATE(OrderDate), MONTH)) AS order_month, -- to convert OrderDate from a timestamp to a date, trancate the date to the first day of the month and find the last day of the same month
  CountryRegionCode,
  territory.Name AS Region,
  COUNT(DISTINCT SalesOrderID) AS number_orders,
  COUNT(DISTINCT CustomerID) AS number_customers,
  COUNT(DISTINCT SalespersonID) AS no_salesPersons,
  CAST(SUM(TotalDue) AS INT64) AS Total_w_tax -- to remove decimal places and display the number as an integer
FROM
  tc-da-1.adwentureworks_db.salesorderheader AS sales
JOIN
  tc-da-1.adwentureworks_db.salesterritory AS territory
ON
  sales.TerritoryID = territory.TerritoryID
GROUP BY
  order_month,
  CountryRegionCode,
  Name
ORDER BY
  CountryRegionCode DESC;
```

</details>

- ### **Task 6**

> Enhance the query with the **cumulative sum of the total amount earned (including tax) per country and region** using a CTE or subquery.

<details>
  <summary>
    Expand Solution 6
  </summary>

```sql
  -- CTE to calculate the monthly sales data by CountryRegionCode and Region
WITH
  monthly_sales AS (
  SELECT
    LAST_DAY(DATE_TRUNC(DATE(OrderDate), MONTH)) AS order_month,
    CountryRegionCode,
    territory.Name AS Region,
    COUNT(DISTINCT SalesOrderID) AS number_orders,
    COUNT(DISTINCT CustomerID) AS number_customers,
    COUNT(DISTINCT SalespersonID) AS no_salesPersons,
    CAST(SUM(TotalDue) AS INT64) AS Total_w_tax
  FROM
    tc-da-1.adwentureworks_db.salesorderheader AS sales
  JOIN
    tc-da-1.adwentureworks_db.salesterritory AS territory
  ON
    sales.TerritoryID = territory.TerritoryID
  GROUP BY
    order_month,
    CountryRegionCode,
    Name
  ORDER BY
    CountryRegionCode DESC  )
  -- main query
SELECT
  order_month,
  CountryRegionCode,
  Region,
  number_orders,
  number_customers,
  no_salesPersons,
  Total_w_tax,
  SUM(Total_w_tax) OVER (PARTITION BY CountryRegionCode, Region ORDER BY order_month) AS cumulative_sum -- window function to calculate the cumulative sum of the total amount with tax per country and region
FROM
  monthly_sales
ORDER BY
  CountryRegionCode;
```

</details>

- ### **Task 7**

> Add a **‘sales_rank’** column that ranks rows from best to worst for each country based on the total amount earned (including tax) each month.

<details>
  <summary>
    Expand Solution 7
  </summary>

```sql
  -- CTE to calculate the monthly sales data by CountryRegionCode and Region
WITH
  monthly_sales AS (
  SELECT
    LAST_DAY(DATE_TRUNC(DATE(OrderDate), MONTH)) AS order_month,
    CountryRegionCode,
    territory.Name AS Region,
    COUNT(DISTINCT SalesOrderID) AS number_orders,
    COUNT(DISTINCT CustomerID) AS number_customers,
    COUNT(DISTINCT SalespersonID) AS no_salesPersons,
    CAST(SUM(TotalDue) AS INT64) AS Total_w_tax
  FROM
    tc-da-1.adwentureworks_db.salesorderheader AS sales
  JOIN
    tc-da-1.adwentureworks_db.salesterritory AS territory
  ON
    sales.TerritoryID = territory.TerritoryID
  GROUP BY
    order_month,
    CountryRegionCode,
    Name
  ORDER BY
    CountryRegionCode DESC )
  -- main query
SELECT
  order_month,
  CountryRegionCode,
  Region,
  number_orders,
  number_customers,
  no_salesPersons,
  Total_w_tax,
  RANK() OVER(PARTITION BY CountryRegionCode ORDER BY Total_w_tax DESC) AS country_sales_rank, -- window function to rank the rows from best to worst for each country based on total amount with tax earned each month
  SUM(Total_w_tax) OVER (PARTITION BY CountryRegionCode, Region ORDER BY order_month) AS cumulative_sum
FROM
  monthly_sales
/*WHERE
  CountryRegionCode = 'FR'*/ -- filter for France to check if the query matches the hint provided
ORDER BY
  CountryRegionCode,
  country_sales_rank;
```

</details>

- ### **Task 8**

> Add taxes at the country level. Since tax rates can vary within a country based on the province, include a column for ‘**mean_tax_rate**’ to represent the average tax rate in a country. Also, since not all regions have tax data, include **‘perc_provinces_w_tax’** to show the percentage of provinces with available tax rates for each country.

<details>
  <summary>
    Expand Solution 8
  </summary>

```sql
  -- CTE to find monthly sales
WITH
  monthly_sales AS (
  SELECT
    LAST_DAY(DATE_TRUNC(DATE(OrderDate), MONTH)) AS order_month,
    CountryRegionCode,
    territory.Name AS Region,
    COUNT(DISTINCT SalesOrderID) AS number_orders,
    COUNT(DISTINCT CustomerID) AS number_customers,
    COUNT(DISTINCT SalespersonID) AS no_salesPersons,
    CAST(SUM(TotalDue) AS INT64) AS Total_w_tax
  FROM
    tc-da-1.adwentureworks_db.salesorderheader AS sales
  JOIN
    tc-da-1.adwentureworks_db.salesterritory AS territory
  ON
    sales.TerritoryID = territory.TerritoryID
  GROUP BY
    order_month,
    CountryRegionCode,
    Name
  ORDER BY
    CountryRegionCode DESC ),
  -- CTE to find country tax info
  country_tax_info AS (
  SELECT
    CountryRegionCode,
    COUNT(DISTINCT state_province.StateProvinceID) AS total_provinces,
    COUNT(DISTINCT
      CASE
        WHEN tax_rate.StateProvinceID IS NOT NULL THEN state_province.StateProvinceID
    END
      ) AS provinces_w_tax, -- number of provinces with available tax rates for each country
    ROUND(AVG(max_tax_rate), 1) AS mean_tax_rate -- average taxes on a country level
  FROM
    tc-da-1.adwentureworks_db.stateprovince AS state_province
  LEFT JOIN (
      -- subquery to find higher tax rate if a state has multiple tax rates
    SELECT
      StateProvinceID,
      MAX(TaxRate) AS max_tax_rate
    FROM
      tc-da-1.adwentureworks_db.salestaxrate
    GROUP BY
      StateProvinceID ) AS tax_rate
  ON
    tax_rate.StateProvinceID = state_province.StateProvinceID
  GROUP BY
    CountryRegionCode )
  -- main query
SELECT
  order_month,
  monthly_sales.CountryRegionCode,
  Region,
  number_orders,
  number_customers,
  no_salesPersons,
  Total_w_tax,
  RANK() OVER(PARTITION BY monthly_sales.CountryRegionCode ORDER BY Total_w_tax DESC) AS country_sales_rank,
  SUM(Total_w_tax) OVER (PARTITION BY monthly_sales.CountryRegionCode, Region ORDER BY order_month) AS cumulative_sum,
  mean_tax_rate,
  ROUND(
  IF
    (total_provinces = 0, 0, CAST(provinces_w_tax AS FLOAT64) / total_provinces), 2 ) AS perc_provinces_w_tax -- the percentage of provinces with available tax rates for each country
FROM
  monthly_sales
JOIN
  country_tax_info
ON
  monthly_sales.CountryRegionCode = country_tax_info.CountryRegionCode 
/*WHERE
  monthly_sales.CountryRegionCode = 'US'*/ -- filter for the US to check if the query matches the hint provided
ORDER BY
  CountryRegionCode,
  country_sales_rank;
```

</details>
