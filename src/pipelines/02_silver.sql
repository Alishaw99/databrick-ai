USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:bronze_schema);
CREATE TEMPORARY VIEW raw_products AS SELECT * FROM products;
CREATE TEMPORARY VIEW raw_orders AS SELECT * FROM orders;
CREATE TEMPORARY VIEW raw_payments AS SELECT * FROM payments;
CREATE TEMPORARY VIEW raw_customer_inserts AS SELECT DISTINCT customer_id FROM customers WHERE operation='I';
CREATE TEMPORARY VIEW raw_customer_stream AS SELECT * FROM STREAM(customers);
USE SCHEMA IDENTIFIER(:silver_schema);

CREATE OR REFRESH MATERIALIZED VIEW products (
 CONSTRAINT product_key EXPECT(product_id IS NOT NULL) ON VIOLATION DROP ROW,
 CONSTRAINT positive_price EXPECT(unit_price > 0) ON VIOLATION DROP ROW
) TBLPROPERTIES ('delta.enableRowTracking'='true') AS
SELECT product_id, category, CAST(unit_price AS DECIMAL(18,2)) AS unit_price,
       source_file, ingested_at
FROM raw_products
QUALIFY row_number() OVER(PARTITION BY product_id ORDER BY ingested_at, source_file)=1;

CREATE OR REFRESH STREAMING TABLE customer_events (
 CONSTRAINT customer_key EXPECT(customer_id IS NOT NULL AND event_id IS NOT NULL) ON VIOLATION DROP ROW,
 CONSTRAINT customer_operation EXPECT(operation IN ('I','U','D')) ON VIOLATION DROP ROW,
 CONSTRAINT customer_time EXPECT(effective_ts IS NOT NULL AND sequence IS NOT NULL) ON VIOLATION DROP ROW
) TBLPROPERTIES ('delta.enableRowTracking'='true') AS
SELECT event_id, customer_id, name, region, tier, operation,
 CAST(effective_ts AS TIMESTAMP) AS effective_ts, CAST(sequence AS INT) AS sequence,
 source_file, ingested_at
FROM STREAM(raw_customer_stream);

CREATE OR REFRESH STREAMING TABLE customer_history
TBLPROPERTIES ('delta.enableRowTracking'='true');
CREATE FLOW customer_history_cdc AS AUTO CDC INTO customer_history
FROM STREAM(customer_events)
KEYS(customer_id)
APPLY AS DELETE WHEN operation='D'
SEQUENCE BY STRUCT(effective_ts, sequence)
COLUMNS * EXCEPT(event_id, operation, source_file, ingested_at)
STORED AS SCD TYPE 2;

CREATE OR REFRESH MATERIALIZED VIEW orders (
 CONSTRAINT required_keys EXPECT(order_id IS NOT NULL AND line_key IS NOT NULL) ON VIOLATION DROP ROW,
 CONSTRAINT known_customer EXPECT(customer_id IS NOT NULL AND customer_exists) ON VIOLATION DROP ROW,
 CONSTRAINT known_product EXPECT(product_exists) ON VIOLATION DROP ROW,
 CONSTRAINT positive_amount EXPECT(line_amount > 0 AND quantity > 0 AND unit_price > 0) ON VIOLATION DROP ROW,
 CONSTRAINT valid_order_time EXPECT(order_ts IS NOT NULL AND order_ts <= current_timestamp()) ON VIOLATION DROP ROW,
 CONSTRAINT amount_matches_quantity EXPECT(line_amount=CAST(quantity*unit_price AS DECIMAL(18,2)))
) TBLPROPERTIES ('delta.enableRowTracking'='true') AS
WITH dedup AS (
 SELECT * FROM raw_orders
 QUALIFY row_number() OVER(PARTITION BY line_key ORDER BY ingested_at, source_file)=1
), customer_keys AS (
 SELECT customer_id FROM raw_customer_inserts
)
SELECT o.order_id, o.line_key, o.line_number, o.customer_id, o.product_id,
 CAST(o.order_ts AS TIMESTAMP) AS order_ts, o.channel, CAST(o.quantity AS INT) AS quantity,
 CAST(o.unit_price AS DECIMAL(18,2)) AS unit_price,
 CAST(o.line_amount AS DECIMAL(18,2)) AS line_amount,
 CAST(o.ingestion_date AS DATE) AS ingestion_date, o.source_file, o.ingested_at,
 c.customer_id IS NOT NULL AS customer_exists, p.product_id IS NOT NULL AS product_exists
FROM dedup o LEFT JOIN customer_keys c ON o.customer_id=c.customer_id
LEFT JOIN products p ON o.product_id=p.product_id;

CREATE OR REFRESH MATERIALIZED VIEW payments (
 CONSTRAINT payment_key EXPECT(payment_event_id IS NOT NULL AND order_id IS NOT NULL) ON VIOLATION DROP ROW,
 CONSTRAINT known_status EXPECT(status IN ('succeeded','failed','refunded')) ON VIOLATION DROP ROW,
 CONSTRAINT positive_payment EXPECT(amount > 0 AND attempt_number > 0) ON VIOLATION DROP ROW,
 CONSTRAINT valid_latency EXPECT(latency_ms >= 0)
) TBLPROPERTIES ('delta.enableRowTracking'='true') AS
SELECT payment_event_id, order_id, gateway, CAST(attempt_number AS INT) AS attempt_number,
 event_type, status, CAST(event_ts AS TIMESTAMP) AS event_ts,
 CAST(amount AS DECIMAL(18,2)) AS amount, CAST(latency_ms AS BIGINT) AS latency_ms,
 source_file, ingested_at
FROM raw_payments
QUALIFY row_number() OVER(PARTITION BY payment_event_id ORDER BY ingested_at, source_file)=1;

-- Dataset-level bronze-to-silver check, inside this pipeline (no check job).
-- The expectation fails this update, but independent flows may already commit.
CREATE OR REFRESH PRIVATE MATERIALIZED VIEW orders_retention_check (
 CONSTRAINT acceptable_order_loss EXPECT(
   bronze_orders > 0 AND silver_orders >= bronze_orders*(1-CAST(:max_order_loss_rate AS DOUBLE))
 ) ON VIOLATION FAIL UPDATE
) AS
SELECT b.bronze_orders, s.silver_orders,
       1-s.silver_orders/CAST(b.bronze_orders AS DOUBLE) AS order_loss_rate
FROM (SELECT count(DISTINCT order_id) AS bronze_orders FROM raw_orders) b
CROSS JOIN (SELECT count(DISTINCT order_id) AS silver_orders FROM orders) s;
