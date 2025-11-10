CREATE TABLE retail_sales (
    Transaction_ID VARCHAR(100),
    Customer_ID VARCHAR(100),
    Category VARCHAR(100),
    Item VARCHAR(100),
    Price_Per_Unit DECIMAL(10,2),
    Quantity NUMERIC,
    Total_Spent DECIMAL(10,2),
    Payment_Method VARCHAR(50),
    Location_ VARCHAR(100),
    Transaction_Date DATE,
    Discount_Applied BOOLEAN
);

-- Importing the .csv file
COPY retail_sales
FROM 'C:\Postgres Imports\retail_stores_data_clean.csv'
WITH (FORMAT csv, HEADER true);

SELECT *
FROM retail_sales