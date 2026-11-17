USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:bronze_schema);
CREATE TEMPORARY VIEW payment_unknown_orders AS
SELECT DISTINCT order_id FROM payments WHERE status IS NULL OR status NOT IN ('succeeded','failed','refunded');
USE SCHEMA IDENTIFIER(:silver_schema);
CREATE TEMPORARY VIEW clean_order_lines AS SELECT * FROM orders;
CREATE TEMPORARY VIEW clean_payment_events AS SELECT * FROM payments;
CREATE TEMPORARY VIEW customer_versions AS SELECT * FROM customer_history;
USE SCHEMA IDENTIFIER(:gold_schema);

CREATE OR REFRESH MATERIALIZED VIEW order_fact AS
WITH baskets AS (
 SELECT order_id, customer_id, min(order_ts) AS order_ts, min(channel) AS channel,
        sum(line_amount) AS valid_line_value, count(*) AS line_count
 FROM clean_order_lines GROUP BY order_id, customer_id
), outcomes AS (
 SELECT order_id, max(gateway) AS gateway,
 max(CASE WHEN status='succeeded' AND event_type='attempt' THEN 1 ELSE 0 END) AS captured,
 max(CASE WHEN status='succeeded' AND event_type='attempt' THEN amount END) AS captured_amount,
 sum(CASE WHEN status='refunded' THEN amount ELSE 0 END) AS refunded_amount,
 max(attempt_number) AS max_attempt,
 sum(CASE WHEN status='failed' AND event_type='attempt' THEN 1 ELSE 0 END) AS failed_attempts,
 sum(CASE WHEN event_type='attempt' THEN 1 ELSE 0 END) AS attempts,
 max(amount) AS payment_order_value
 FROM clean_payment_events GROUP BY order_id
), unknowns AS (
 SELECT order_id FROM payment_unknown_orders
)
SELECT b.order_id, b.customer_id, b.order_ts, date(b.order_ts) AS order_date,
 date_trunc('hour',b.order_ts) AS order_hour, b.channel, c.name AS customer_name,
 c.region, c.tier AS tier_at_order_time, p.gateway, b.valid_line_value,
 p.payment_order_value AS order_value, b.line_count,
 coalesce(p.attempts,0) AS attempts, coalesce(p.failed_attempts,0) AS failed_attempts,
 coalesce(p.max_attempt,0) AS max_attempt,
 CASE WHEN u.order_id IS NOT NULL OR p.order_id IS NULL THEN 'unknown'
      WHEN p.captured=1 THEN 'captured' ELSE 'failed' END AS payment_outcome,
 CASE WHEN u.order_id IS NULL AND p.captured=1 THEN p.captured_amount ELSE 0 END AS captured_value,
 CASE WHEN u.order_id IS NULL THEN coalesce(p.refunded_amount,0) ELSE 0 END AS refunded_value,
 CASE WHEN u.order_id IS NULL AND p.captured=1 THEN p.captured_amount-coalesce(p.refunded_amount,0) ELSE 0 END AS revenue,
 CASE WHEN u.order_id IS NULL AND p.captured=0 THEN p.payment_order_value ELSE 0 END AS lost_revenue,
 CASE WHEN b.order_ts>=CAST(:incident_start AS TIMESTAMP) AND b.order_ts<CAST(:incident_end AS TIMESTAMP)
           AND p.gateway=:incident_gateway THEN true ELSE false END AS in_incident
FROM baskets b
LEFT JOIN outcomes p ON b.order_id=p.order_id
LEFT JOIN unknowns u ON b.order_id=u.order_id
LEFT JOIN customer_versions c
 ON b.customer_id=c.customer_id AND b.order_ts>=c.__START_AT.effective_ts
 AND (b.order_ts<c.__END_AT.effective_ts OR c.__END_AT IS NULL);

CREATE OR REFRESH MATERIALIZED VIEW daily_sales AS
SELECT order_date, region, channel, gateway, tier_at_order_time,
 count(*) AS orders, count_if(payment_outcome='failed') AS failed_orders,
 count_if(payment_outcome='unknown') AS unknown_orders,
 sum(revenue) AS revenue, sum(lost_revenue) AS lost_revenue,
 sum(captured_value) AS captured_value, sum(refunded_value) AS refunded_value
FROM order_fact GROUP BY order_date, region, channel, gateway, tier_at_order_time;

CREATE OR REFRESH MATERIALIZED VIEW hourly_gateway_health AS
SELECT date_trunc('hour',event_ts) AS event_hour, gateway,
 count(*) AS attempts, count_if(status='failed') AS failed_attempts,
 count_if(status='failed')/CAST(count(*) AS DOUBLE) AS failure_rate,
 avg(latency_ms) AS avg_latency_ms, count_if(attempt_number>1) AS retries
FROM clean_payment_events WHERE event_type='attempt'
GROUP BY date_trunc('hour',event_ts), gateway;

CREATE OR REFRESH MATERIALIZED VIEW customer_outage_impact AS
WITH impact AS (
 SELECT customer_id, max(customer_name) AS customer_name, max(region) AS region,
 count(*) AS incident_orders, count_if(payment_outcome='failed') AS failed_orders,
 sum(lost_revenue) AS lost_revenue, count_if(tier_at_order_time='enterprise' AND payment_outcome='failed')>0 AS affected_enterprise
 FROM order_fact WHERE in_incident GROUP BY customer_id
), downgrade_events AS (
 SELECT customer_id, min(__START_AT.effective_ts) AS downgrade_at
 FROM customer_versions
 WHERE tier<>'enterprise' AND __START_AT.effective_ts>=CAST(:incident_end AS TIMESTAMP)
 AND __START_AT.effective_ts<CAST(:incident_end AS TIMESTAMP)+INTERVAL 14 DAYS
 GROUP BY customer_id
)
SELECT i.*, CASE WHEN i.affected_enterprise THEN d.downgrade_at END AS downgrade_at,
       i.affected_enterprise AND d.downgrade_at IS NOT NULL AS downgraded_within_14_days
FROM impact i LEFT JOIN downgrade_events d ON i.customer_id=d.customer_id;

CREATE OR REFRESH MATERIALIZED VIEW recovery_trend AS
SELECT event_hour, gateway, attempts, failed_attempts, failure_rate, avg_latency_ms, retries,
 CASE WHEN event_hour<CAST(:incident_start AS TIMESTAMP) THEN 'before'
      WHEN event_hour<CAST(:incident_end AS TIMESTAMP) THEN 'incident' ELSE 'after' END AS incident_phase
FROM hourly_gateway_health;
