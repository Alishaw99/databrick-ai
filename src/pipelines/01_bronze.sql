USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:bronze_schema);

CREATE OR REFRESH STREAMING TABLE products
TBLPROPERTIES ('delta.enableRowTracking'='true', 'delta.enableChangeDataFeed'='true') AS
SELECT *, _metadata.file_path AS source_file, current_timestamp() AS ingested_at
FROM STREAM read_files(:source_root || '/products', format => 'parquet');

CREATE OR REFRESH STREAMING TABLE customers
TBLPROPERTIES ('delta.enableRowTracking'='true', 'delta.enableChangeDataFeed'='true') AS
SELECT *, _metadata.file_path AS source_file, current_timestamp() AS ingested_at
FROM STREAM read_files(:source_root || '/customers', format => 'parquet');

CREATE OR REFRESH STREAMING TABLE orders
TBLPROPERTIES ('delta.enableRowTracking'='true', 'delta.enableChangeDataFeed'='true') AS
SELECT *, _metadata.file_path AS source_file, current_timestamp() AS ingested_at
FROM STREAM read_files(:source_root || '/orders', format => 'parquet');

CREATE OR REFRESH STREAMING TABLE payments
TBLPROPERTIES ('delta.enableRowTracking'='true', 'delta.enableChangeDataFeed'='true') AS
SELECT *, _metadata.file_path AS source_file, current_timestamp() AS ingested_at
FROM STREAM read_files(:source_root || '/payments', format => 'parquet');
