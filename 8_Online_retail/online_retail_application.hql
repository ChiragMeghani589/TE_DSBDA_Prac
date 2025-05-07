
-- Step i: Create and Load Table with Online Retail Data in Hive
CREATE DATABASE IF NOT EXISTS onlineretail_db;
USE onlineretail_db;

CREATE TABLE IF NOT EXISTS online_retail (
    InvoiceNo STRING,
    StockCode STRING,
    Description STRING,
    Quantity INT,
    InvoiceDate STRING,
    UnitPrice FLOAT,
    CustomerID STRING,
    Country STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Load data (adjust path)
-- LOAD DATA LOCAL INPATH '/path/to/Online_Retail.csv' INTO TABLE online_retail;

-- Step j: Create Index on Online Retail Table in Hive
CREATE INDEX idx_customer ON TABLE online_retail (CustomerID)
AS 'COMPACT' WITH DEFERRED REBUILD;

ALTER INDEX idx_customer ON online_retail REBUILD;

-- Step k: Find Total, Average Sales in Hive
SELECT 
    SUM(UnitPrice * Quantity) AS total_sales,
    AVG(UnitPrice * Quantity) AS average_sales
FROM online_retail;

-- Step l: Find Order Details with Maximum Cost
SELECT InvoiceNo, SUM(UnitPrice * Quantity) AS order_total
FROM online_retail
GROUP BY InvoiceNo
ORDER BY order_total DESC
LIMIT 1;

-- Step m: Find Customer Details with Maximum Order Total
SELECT CustomerID, SUM(UnitPrice * Quantity) AS customer_total
FROM online_retail
GROUP BY CustomerID
ORDER BY customer_total DESC
LIMIT 1;

-- Step n: Find the Country with Maximum and Minimum Sale
-- Maximum sale
SELECT Country, SUM(UnitPrice * Quantity) AS country_total
FROM online_retail
GROUP BY Country
ORDER BY country_total DESC
LIMIT 1;

-- Minimum sale (excluding zero sales)
SELECT Country, SUM(UnitPrice * Quantity) AS country_total
FROM online_retail
GROUP BY Country
HAVING country_total > 0
ORDER BY country_total ASC
LIMIT 1;

-- Step o: Create External Hive Table to Connect to HBase
CREATE EXTERNAL TABLE online_retail_hbase (
    key STRING,
    InvoiceNo STRING,
    StockCode STRING,
    Description STRING,
    Quantity INT,
    InvoiceDate STRING,
    UnitPrice FLOAT,
    CustomerID STRING,
    Country STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,cf:InvoiceNo,cf:StockCode,cf:Description,cf:Quantity,cf:InvoiceDate,cf:UnitPrice,cf:CustomerID,cf:Country"
)
TBLPROPERTIES ("hbase.table.name" = "online_retail_hbase");

-- Step p: Display Records of OnlineRetail Table in HBase
SELECT * FROM online_retail_hbase LIMIT 20;
