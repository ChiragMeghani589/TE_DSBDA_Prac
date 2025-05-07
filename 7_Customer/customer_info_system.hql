
-- a. Create tables in Hive
CREATE DATABASE IF NOT EXISTS customer_db;
USE customer_db;

CREATE TABLE customer_info (
    cust_id STRING,
    cust_name STRING,
    order_id STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE order_info (
    order_id STRING,
    item_id STRING,
    quantity INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE item_info (
    item_id STRING,
    item_name STRING,
    item_price DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- b. Load data from local
LOAD DATA LOCAL INPATH '/path/to/customer_info.csv' INTO TABLE customer_info;
LOAD DATA LOCAL INPATH '/path/to/order_info.csv' INTO TABLE order_info;
LOAD DATA LOCAL INPATH '/path/to/item_info.csv' INTO TABLE item_info;

-- c. Perform Join
SELECT c.cust_id, c.cust_name, o.order_id, i.item_name, i.item_price, o.quantity
FROM customer_info c
JOIN order_info o ON c.order_id = o.order_id
JOIN item_info i ON o.item_id = i.item_id;

-- d. Create Index
CREATE INDEX idx_cust_id ON TABLE customer_info (cust_id)
AS 'COMPACT' WITH DEFERRED REBUILD;

-- e. Find total, average sales
SELECT SUM(o.quantity * i.item_price) AS total_sales, AVG(o.quantity * i.item_price) AS avg_sales
FROM order_info o
JOIN item_info i ON o.item_id = i.item_id;

-- f. Find order details with max cost
SELECT o.order_id, SUM(o.quantity * i.item_price) AS order_total
FROM order_info o
JOIN item_info i ON o.item_id = i.item_id
GROUP BY o.order_id
ORDER BY order_total DESC
LIMIT 1;

-- g. Create external Hive table linked to HBase
CREATE EXTERNAL TABLE hbase_customer_info(
    cust_id STRING,
    cust_name STRING,
    order_id STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,cf1:cust_name,cf1:order_id')
TBLPROPERTIES ('hbase.table.name' = 'hbase_customer_info');

-- h. Display records from HBase
SELECT * FROM hbase_customer_info;
