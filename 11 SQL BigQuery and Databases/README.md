## Objectives

Learn the fundamentals of SQL and its use databases, followed by an exploration of advanced SQL functionalities (subqueries, CTEs, window functions), and the basic concepts of data cleaning and validation. SQL is an essential skill for data analysts and serves as a great introduction to databases, data storage, and data retrieval intricacies.

## Data Source
- BigQuery `adwentureworks_db`
- [AdventureWorks_2005.pdf](https://drive.google.com/file/d/1-Qsnn3bg0_PYgY5kKJOUDG8xdKLvOLPK/view?usp=sharing)

## Tasks with Solutions

- ### **Task 1**

Create a detailed **overview of all individual customers**. Copy only the top 200 rows from your query, ordered by total amount (including tax). Some customers have multiple addresses; to avoid duplicate data, select their latest available address by choosing the maximum `AddressId`.

- ### **Solution 1**
---
  
```
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
---

- ### **Task 2**

Retrieve data for the **top 200 customers with the highest total amount (including tax) who have not placed an order in the last 365 days**. Note that the database is outdated, so define the current date by finding the latest order date in the orders table.

- ### **Solution 2**

- ### **Task 3**

Create a **new column in the view that marks customers as active or inactive based on whether they have placed an order in the last 365 days**. Copy only the top 500 rows from your query, ordered by `CustomerId` in descending order.

- ### **Solution 3**

- ### **Task 4**

Extract data on **all active customers from North America**. Only include customers who have either a total order amount of at least 2500 (including tax) or have placed 5 or more orders. In the output, split their address line into two columns. Order the output by country, state, and `date_last_order`.

- ### **Solution 4**

- ### **Task 5**

Create a query to calculate **monthly sales numbers in each country and region**. The query should include the number of orders, customers, and salespersons in each month, along with the total amount earned (including tax). Sales data for all types of customers should be included.

- ### **Solution 5**

- ### **Task 6**

Enhance the query with the **cumulative sum of the total amount earned (including tax) per country and region** using a CTE or subquery.

- ### **Solution 6**

- ### **Task 7**

Add a **‘sales_rank’** column that ranks rows from best to worst for each country based on the total amount earned (including tax) each month.

- ### **Solution 7**

- ### **Task 8**

Add taxes at the country level. Since tax rates can vary within a country based on the province, include a column for ‘**mean_tax_rate**’ to represent the average tax rate in a country. Also, since not all regions have tax data, include **‘perc_provinces_w_tax’** to show the percentage of provinces with available tax rates for each country.

- ### **Solution 8**
