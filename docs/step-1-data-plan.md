# Step 1: setup and proposed data plan

Status: plan approved and Step 1 completed on 2026-11-16. See step-1-results.md and ../reports/historical-v1.json for measured results.

## Confirmed configuration

- Workspace: https://dbc-cbef9784-5349.cloud.databricks.com
- CLI profile: Faisal (OAuth)
- Catalog: rafa (existing catalog created by the user)
- Base schema setting: payments_raw. Databricks development mode prefixes managed schemas with the developer identity.
- Actual development schema: dev_tariq_faisal1999_payments_raw
- Managed volume: /Volumes/rafa/dev_tariq_faisal1999_payments_raw/raw_data
- SQL warehouse: Serverless Starter Warehouse (d781c076806d9c09)
- Failure notification recipient: tariq.faisal1999@outlook.com
- One bundle, dev and prod targets; validate both, deploy only dev.
- Production schema is isolated as payments_raw_prod; no production resources deployed.

## Proposed historical dataset

All times UTC. Generate 61 days, 2026-09-01 through 2026-10-31.
Reserve 2026-11-01 for the later incremental-load demonstration.

| Source | Proposed scale | Grain and important fields |
| --- | --- | --- |
| Products | 200 | Product ID, category, unit price; small, skewed catalog |
| Customers | 10,000 initial customers plus change events | Event ID, customer ID, operation I/U/D, effective timestamp, sequence, region, tier |
| Orders | About 120,000 distinct orders, 180,000 line items | Order ID, line ID, customer/product IDs, timestamp, channel, quantity, unit price, line amount |
| Payments | About 140,000–170,000 events | Payment event ID, order ID, gateway, attempt number, event type, status, timestamp, amount, latency; refunds reference successful payments |

Counts are targets; final counts and measured story metrics will be recorded after generation.

## Business story and distributions

- Three gateways with approximate traffic shares 70%, 20%, 10%.
- On 2026-10-10 from 10:00 through 13:59 UTC, the main gateway's attempt failure rate rises from about 2% to about 60%, with increased latency and retries. Other gateways remain near baseline; recovery returns the main gateway to baseline.
- Enterprise represents about 5% of customers and about 50% of pre-outage order value. Skew purchase frequency and basket amounts, retaining long-tailed spending within each tier.
- Four regions and three channels have unequal shares. Include daytime peaks, weekday/weekend differences, and a gradual volume trend.
- About 25% of enterprise customers with terminally failed outage orders downgrade 1–14 days afterward. Retain changes for historical tier-at-order-time joins. Include unrelated tier changes and a small number of deletes for CDC coverage; no orders reference a customer after their deletion.
- Baseline attempt failures also occur outside the incident, so the anomaly is measurable against a realistic baseline.

## Payment and metric semantics

- Every order contains one or more line items; payment attempts are at order grain, never repeated per line item.
- An attempt failure is distinct from an ultimately failed order: successful retries recover revenue.
- Revenue is successful captured order value minus completed refunds, counted once per order.
- Lost revenue is order value with no successful capture after all attempts settle. Report incident-window lost revenue separately; do not claim this equals causal incremental loss without a baseline adjustment.
- Affected enterprise customers are distinct customers whose tier was enterprise at order time and whose outage-window order ultimately failed.
- Downgrades are determined from subsequent customer change events, not the current customer tier.

## Deliberate quality defects

Inject known, non-overlapping cohorts into order line records: approximately 0.3% missing/unknown customer IDs, 0.2% non-positive amounts, 0.1% future timestamps, plus 0.5% exact duplicate line records. Inject unknown statuses into approximately 0.1% of payment events as a separately identified cohort. Keep a generation manifest with exact injected counts outside the source folders. Preserve defects for later pipeline expectations; do not clean raw files.

## Files, repeatability, and incremental mode

- Write multi-part Parquet files into source directories, partitioned by ingestion date. Customer effective timestamps remain separate from ingestion dates.
- Use serverless Spark via Databricks Connect, with local Python dependencies in the project .venv. Use a small deterministic local Faker name vocabulary and native Spark expressions for all source records. The workspace's remote Python UDF container failed to start, so no remote Python UDFs are used. Relationships and distributions use deterministic ID/date-based hashes.
- Generate parent files before child sources, reading parent Parquet files for valid foreign keys. Raw sources remain files, not tables.
- Historical generation uses a fixed batch ID and deterministic event IDs. Record completed staging paths and published file counts per source in a batch manifest; files are partitioned by ingestion day. Re-running a completed batch skips it; unfinished sources use new staging attempt directories. Resume publication by copying only missing immutable files. Never overwrite published files or append duplicate source events on a rerun.
- Incremental mode generates only the explicitly requested new day, with new event IDs, customer changes, orders and related payment events. It reads the existing customer history to preserve continuity.
- Keep ingestion source directories separate from manifests and validation reports. Validate business metrics against valid, deduplicated records while also reporting raw counts and injected defects.

## Acceptance checks after generation

1. Record exact source counts, file counts, date ranges and injected defect counts.
2. Confirm gateway traffic shares and normal-versus-outage failure rates, retries and latency.
3. Confirm enterprise customer share and pre-outage order-value share.
4. Reconcile captured amounts, refunds, recovered retries and terminal failed-order value without line-item fan-out.
5. Confirm affected enterprise customers and downgrades within 14 days, plus visible recovery.
6. Confirm rerunning a completed day adds no files or events; reserve the extra-day run for Step 4.

Stop after Step 1. Step 2 profiling begins only when the user says continue.
