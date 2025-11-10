/*Sales Forecasting: Monthly Revenue Time Series*/

-- This query aggregates daily transactions into total monthly revenue,
-- which serves as the input data for the Prophet forecasting model.

SELECT
    transaction_date,
    total_spent
FROM
    retail_sales;

SELECT
    date_trunc('month',transaction_date) AS ds,
    sum(total_spent) AS y
FROM
    retail_sales
GROUP BY
    ds
ORDER BY
    ds;