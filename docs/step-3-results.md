# Step 3 — medallion pipeline completed

Completed on November 16, 2026 (America/Los_Angeles). Pipeline:
[payments_lakehouse](https://dbc-cbef9784-5349.cloud.databricks.com/pipelines/b6118dc8-faaa-4c10-89e5-0d58e6b7a068).

Successful incremental updates: `8f00d4d0-3df1-480d-aff6-ad82950d239f` and
`0d04c163-8229-4fda-840a-4d7139b6236c`. Both reached COMPLETED. No full refresh,
resource deletion or raw source overwrite was performed. Only dev was deployed;
both bundle targets pass strict validation.

## Implementation

- `rafa.dev_tariq_faisal1999_payments_bronze`: four streaming tables use Auto Loader to incrementally ingest only the published source folders. Every row retains `source_file` and `ingested_at`. Pipeline-managed checkpoints track ingestion.
- `rafa.dev_tariq_faisal1999_payments_silver`: products, orders and payments are typed, deduplicated materialized views. Expectations drop invalid records; soft checks warn. Customer events are validated in a streaming table and applied through Auto CDC as SCD Type 2, ordered by `(effective_ts, sequence)` with explicit delete handling.
- A private silver `orders_retention_check` materialized view compares distinct bronze and silver orders. Its FAIL UPDATE expectation permits at most 1% loss. Current loss is **0.299145%**: 329 of 109,980 orders. Duplicate line removal is measured separately. This is part of the pipeline, not a separate check job.
- `rafa.dev_tariq_faisal1999_payments_gold`: `order_fact`, `daily_sales`, `hourly_gateway_health`, `customer_outage_impact`, and `recovery_trend`. Facts join customer history using inclusive start / exclusive end timestamps. Payments are aggregated before joining order baskets, preventing line-item multiplication of settlement amounts.

All transformations are SQL. Environment schema names, source paths, incident settings and the retention threshold use named `:param` markers. Catalog/schema context uses `USE ... IDENTIFIER(:param)`; temporary views give the graph static cross-layer references. See the official [pipeline parameter documentation](https://docs.databricks.com/aws/en/ldp/parameters).

Bronze streaming tables support exactly-once ingestion of immutable source files. Silver/gold materialized views refresh from the current upstream state; serverless can maintain eligible queries incrementally or automatically recompute views. No streaming-state reset is requested.

## Results

| Layer/table | Rows |
|---|---:|
| Bronze products | 200 |
| Bronze customer events | 10,405 |
| Bronze order lines | 165,493 |
| Bronze payments | 114,565 |
| Silver products | 200 |
| Silver customer events | 10,405 |
| Silver customer history | 10,305 |
| Silver order lines | 163,749 |
| Silver payments | 114,455 |
| Gold order fact | 109,651 |
| Gold daily sales | 6,219 |
| Gold hourly gateway health | 4,375 |
| Gold customer outage impact | 287 |
| Gold recovery trend | 4,375 |

Event-log evidence confirms **973 invalid lines dropped**, **110 unknown-status payment events dropped**, and the order-retention expectation passed. Orders also remove **771 exact duplicate line records** before expectations. Source files remain untouched.

All 14 integration checks passed:

- Bronze counts reconcile to raw files and all rows retain provenance.
- Silver keys are unique and drops reconcile to the measured raw defect cohorts.
- Gold has exactly one row per surviving order, with historical customer tier and region resolved.
- Customer SCD2 history has no overlapping versions and 9,900 current customers after 100 deletes.
- Captured payment amounts minus refunds reconcile exactly to order-fact and daily-sales revenue: **$12,656,691.82**.
- Unknown outcomes contribute no lost revenue.
- Gateway A outage failure rate is **60.1367%**.
- **18 affected enterprise customers**, with **5 downgrades** within 14 days.

Revenue deliberately uses authoritative payment settlement amounts, while `valid_line_value` retains the cleaned basket amount for audit. Step 1's valid-line-value estimate was $12,608,680.48; the difference is $48,011.34 because dropping a defective line does not reduce the amount actually recorded as captured/refunded for an otherwise surviving order. Entirely invalid orders are absent from the fact. Lost revenue is terminal failed payment order value, not a causal baseline-adjusted estimate. Orders containing unknown payment statuses remain classified as unknown rather than failed.

## Operational limits

A failing aggregate retention expectation fails its flow and the overall update. Other flows in a triggered pipeline may already have committed; this single-pipeline design does not provide an atomic rollback or publication barrier for every dataset. Consumers should use only successful update results. No separate pipeline/job gate was added because the project requests one pipeline and quality enforcement within it.

## Evidence and UI checklist

- [Integration verification](../reports/step-3-verification.json), [pipeline expectation metrics](../reports/step-3-expectations.json).
- Jobs & Pipelines → `[dev tariq_faisal1999] payments_lakehouse` → select a COMPLETED update and inspect the graph.
- Inspect silver `orders` data-quality metrics: 478 missing customers, 344 non-positive amounts and 151 future timestamps; inspect silver `payments`: 110 invalid statuses.
- Inspect `orders_retention_check`: 1 passed aggregate record, 0 failed.
- Catalog Explorer → `rafa` → the three `dev_tariq_faisal1999_payments_*` layer schemas → gold `order_fact` and summaries.

Step 3 is complete. Stop here. The manually run orchestration job, failure email and extra-day demonstration belong to Step 4; no job or schedule was created here.
