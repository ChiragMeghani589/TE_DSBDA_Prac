
-- HBase table creation
!hbase shell <<EOF
create 'flight_info', 'flight', 'schedule', 'delay'
EOF

-- Hive external table for HBase
CREATE EXTERNAL TABLE flight_hive (
  key STRING,
  flight_number STRING,
  origin STRING,
  destination STRING,
  departure_time STRING,
  arrival_time STRING,
  departure_delay INT,
  arrival_delay INT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,flight:flight_number,flight:origin,flight:destination,schedule:departure_time,schedule:arrival_time,delay:departure_delay,delay:arrival_delay")
TBLPROPERTIES ("hbase.table.name" = "flight_info");

-- Calculate total departure delay
SELECT SUM(departure_delay) AS total_departure_delay FROM flight_hive;

-- Calculate average departure delay
SELECT AVG(departure_delay) AS average_departure_delay FROM flight_hive;

-- Create index on flight_number
CREATE INDEX flight_number_index ON TABLE flight_hive (flight_number) AS 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX flight_number_index ON flight_hive REBUILD;
