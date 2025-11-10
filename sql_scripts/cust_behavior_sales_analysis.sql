/* Top performing products and categories */

SELECT *
FROM retail_sales;

-- Top 5 most popular categories (by quantities sold)
SELECT
    category,
    SUM(quantity) AS total_no_of_quantities_sold
FROM
    retail_sales
GROUP BY
    category
ORDER BY
    total_no_of_quantities_sold DESC
LIMIT
    5;

-- Top 10 items by revenue
SELECT
    item,
    SUM(total_spent) AS "Total Sales"
FROM
    retail_sales
GROUP BY
    item
ORDER BY
    "Total Sales" DESC
LIMIT 10;

/* Customer Analysis */

-- Top 10 customers
SELECT
    customer_id,
    sum(total_spent) AS "Total Spending"
FROM
    retail_sales
GROUP BY
    customer_id
HAVING
    sum(total_spent) IS NOT NULL
ORDER BY
    "Total Spending" DESC
LIMIT 10;

-- Average Spending per Customer
SELECT
    avg("Total Spending") AS average_spending_per_cutomer
FROM(
    SELECT
    customer_id,
    sum(total_spent) AS "Total Spending"
FROM
    retail_sales
GROUP BY
    customer_id
HAVING
    sum(total_spent) IS NOT NULL
) AS cutomer_spending;

/* Monthly Sales Trend */

SELECT
    transaction_date,
    total_spent
FROM
    retail_sales
WHERE
    total_spent IS NOT NULL;

-- Monthly Sales Trend
SELECT
    TO_CHAR(transaction_date, 'YYYY-MM') AS "Sale Month",
    TO_CHAR(transaction_date, 'Month') AS "Month Name",
    SUM(total_spent) AS "Monthly Revenue"
FROM
    retail_sales
WHERE
    total_spent IS NOT NULL
    AND
    TO_CHAR(transaction_date, 'YYYY-MM') LIKE '%2022%'
GROUP BY
    "Sale Month",
    "Month Name"
ORDER BY
    "Sale Month" ASC;

-- Sales by day of the week
SELECT
    EXTRACT(DOW FROM transaction_date) AS day_of_week_number,
    TO_CHAR(transaction_date, 'Day') AS day_of_week_name,
    sum(total_spent)
FROM
    retail_sales
WHERE
    total_spent IS NOT NULL
GROUP BY
    day_of_week_number,
    day_of_week_name
ORDER BY
    day_of_week_number;

/* Location & Payment Analysis */

-- Average Transaction Value by Location
SELECT
    location_ AS "Location",
    avg(total_spent) AS "Average Transaction"
FROM
    retail_sales
WHERE
    total_spent IS NOT NULL
GROUP BY
    "Location";

-- Total Sales by Payment Method
SELECT
    payment_method,
    sum(total_spent) AS "Total Sales"
FROM
    retail_sales
WHERE
    total_spent IS NOT NULL
GROUP BY
    payment_method
ORDER BY
    "Total Sales" DESC;

/* Discount Effectiveness Analysis */
SELECT
    CASE
        WHEN discount_applied = TRUE THEN 'Discount Applied'
        ELSE 'No Discount Applied'
    END AS discount_status,
    count(*) AS "Total Transactions",
    sum(total_spent) AS "Total Revenue"
FROM
    retail_sales
WHERE
    discount_applied IS NOT NULL
    AND
    total_spent IS NOT NULL
GROUP BY
    discount_status;

/* Granular Sales Trend (Day of Week vs Location) */
SELECT
    EXTRACT(DOW from transaction_date) AS day_of_week_number,
    TO_CHAR(transaction_date, 'Day') AS day_of_week_name,
    sum(CASE
            WHEN location_ = 'In-store' THEN total_spent
            ELSE 0
        END) AS in_store_revenue,
    sum(CASE
            WHEN location_ = 'Online' THEN total_spent
            ELSE 0
        END) AS online_revenue,
    sum(total_spent) AS total_daily_revenue
FROM
    retail_sales
GROUP BY
    day_of_week_number,
    day_of_week_name
ORDER BY
    day_of_week_number;

/*Top Selling products by Location*/
WITH ItemRevenueByLocation AS (
SELECT
    location_,
    item,
    SUM(total_spent) AS item_revenue_by_location,
    -- Rank the items within each location partition
    ROW_NUMBER() OVER (PARTITION BY location_ ORDER BY SUM(total_spent) DESC) AS rank_by_revenue
FROM
    retail_sales
GROUP BY
    location_,
    item
)
    -- Select only the top 3 items for each location
SELECT
    location_,
    item,
    item_revenue_by_location
FROM
    ItemRevenueByLocation
WHERE
    rank_by_revenue <= 3
ORDER BY
    item_revenue_by_location DESC;