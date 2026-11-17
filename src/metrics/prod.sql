CREATE OR REPLACE VIEW IDENTIFIER(:metric_view)
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
source: rafa.payments_gold_prod.order_fact
comment: Governed payment settlement, failure and incident metrics. Hour is order hour in UTC. Lost revenue is terminal failed order value, not causal incremental loss.
joins:
  - name: dim_impact
    source: rafa.payments_gold_prod.customer_outage_impact
    'on': source.customer_id = dim_impact.customer_id
dimensions:
  - name: Date
    expr: source.order_date
  - name: Hour
    expr: source.order_hour
  - name: Region
    expr: source.region
  - name: Channel
    expr: source.channel
  - name: Gateway
    expr: source.gateway
  - name: Tier
    expr: source.tier_at_order_time
  - name: Customer ID
    expr: source.customer_id
  - name: Customer Name
    expr: source.customer_name
  - name: Affected Customer ID
    expr: CASE WHEN source.in_incident AND source.payment_outcome='failed' AND source.tier_at_order_time='enterprise' THEN source.customer_id END
  - name: Downgraded
    expr: coalesce(dim_impact.downgraded_within_14_days, false)
measures:
  - name: Revenue
    expr: SUM(source.revenue)
    comment: Captured settlement amounts less completed refunds. Orders with unknown outcomes contribute zero.
  - name: Lost Revenue
    expr: SUM(source.lost_revenue)
    comment: Value of ultimately failed orders. Successful retries are not losses.
  - name: Orders
    expr: COUNT(*)
  - name: Failed Orders
    expr: COUNT(*) FILTER (WHERE source.payment_outcome='failed')
  - name: Payment Attempts
    expr: SUM(source.attempts)
  - name: Failed Attempts
    expr: SUM(source.failed_attempts)
  - name: Payment Failure Rate
    expr: SUM(source.failed_attempts) / NULLIF(SUM(source.attempts), 0)
    comment: Failed known-status attempts divided by known-status attempts, grouped by order time.
  - name: Affected Enterprise Customers
    expr: COUNT(DISTINCT CASE WHEN source.in_incident AND source.payment_outcome='failed' AND source.tier_at_order_time='enterprise' THEN source.customer_id END)
$$;
