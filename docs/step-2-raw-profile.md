# Step 2 — raw data profile

Profiled 2026-11-17T00:21:36.373389+00:00 using profile `Faisal`, Databricks Connect and serverless Spark.
Sources are immutable historical-v1 Parquet files; all profiling is read-only. No quality monitor or check job was created.

## Volume and source summary

Volume: `/Volumes/rafa/dev_tariq_faisal1999_payments_raw/raw_data`

| Source | Rows | Files | Bytes | Duplicate keys |
|---|---:|---:|---:|---:|
| products | 200 | 1 | 4,444 | 0 |
| customers | 10,405 | 7 | 186,948 | 0 |
| orders | 165,493 | 61 | 4,337,815 | 771 |
| payments | 114,565 | 61 | 3,027,521 | 0 |

## Nulls, keys and date ranges

- **products** — nulls: none in any column; exact duplicate rows: 0. ingestion_date: 2026-09-01 through 2026-09-01
- **customers** — nulls: none in any column; exact duplicate rows: 0. effective_ts: 2026-09-01 00:00:00 through 2026-10-18 00:00:00; ingestion_date: 2026-09-01 through 2026-10-18
- **orders** — nulls: customer_id: 478; exact duplicate rows: 771. order_ts: 2026-09-01 00:01:02 through 2099-01-01 00:00:00; ingestion_date: 2026-09-01 through 2026-10-31
- **payments** — nulls: none in any column; exact duplicate rows: 0. event_ts: 2026-09-01 00:01:02 through 2026-11-01 00:08:31; ingestion_date: 2026-09-01 through 2026-10-31

Order timestamps include deliberate 2099 values. Use ingestion dates to describe historical file coverage; flag future event timestamps in silver. Payments can settle just after midnight on November 1 for October 31 orders; this does not mean the reserved November 1 incremental batch has been generated.

## Value distributions

- **products.category**: consumer = 150, business = 50
- **customers.operation**: I = 10,000, U = 305, D = 100
- **customers.tier**: standard = 6,600, pro = 3,305, enterprise = 500
- **customers.region**: North America = 4,720, Europe = 2,875, Asia Pacific = 1,984, Latin America = 826
- **orders.channel**: web = 96,019, mobile = 52,883, partner = 16,591
- **payments.gateway**: GatewayA = 79,967, GatewayB = 23,185, GatewayC = 11,413
- **payments.event_type**: attempt = 111,857, refund = 2,708
- **payments.status**: succeeded = 109,165, refunded = 2,706, failed = 2,584, UNKNOWN = 110
- **payments.attempt_number**: 1 = 112,649, 2 = 1,916

Customer tier/region distributions above count CDC events, not unique customers or historical order cohorts.

| Numeric field | Min | Median | P95 | Max |
|---|---:|---:|---:|---:|
| products.product_idx | 0 | 99 | 189 | 199 |
| products.unit_price | 15.08 | 50.21 | 233.48 | 257.29 |
| customers.sequence | 1 | 1 | 1 | 2 |
| orders.line_number | 1 | 1 | 2 | 2 |
| orders.quantity | 1 | 1 | 2 | 2 |
| orders.unit_price | 15.08 | 47.57 | 218.01 | 257.29 |
| orders.line_amount | -1.0 | 52.87 | 245.39 | 514.58 |
| payments.attempt_number | 1 | 1 | 1 | 2 |
| payments.amount | 15.08 | 81.58 | 394.84 | 1029.16 |
| payments.latency_ms | 0 | 443 | 767 | 19957 |

## Outage visibility

Rates below exclude UNKNOWN statuses and use payment attempts, including retries, rather than counting refund events as attempts.

| Gateway | Period | Attempts | Failure rate | Mean latency ms | Retries |
|---|---|---:|---:|---:|---:|
| GatewayA | baseline | 49296 | 2.17% | 449.9 | 810 |
| GatewayA | outage | 439 | 60.14% | 12331.2 | 146 |
| GatewayA | recovery | 28267 | 2.09% | 450.5 | 429 |
| GatewayB | baseline | 14284 | 2.00% | 448.3 | 223 |
| GatewayB | outage | 74 | 0.00% | 466.3 | 0 |
| GatewayB | recovery | 8243 | 1.86% | 449.3 | 107 |
| GatewayC | baseline | 7222 | 2.10% | 449.4 | 114 |
| GatewayC | outage | 41 | 2.44% | 435.2 | 1 |
| GatewayC | recovery | 3883 | 1.70% | 455.5 | 47 |

The incident window is October 10, 10:00–13:59 UTC. Adjacent-day hourly evidence is in `reports/step-2-hourly-gateways.csv`; daily source volumes and every column's exact null/cardinality and numerical summaries are in `reports/step-2-profile.json`.

## Defects and readiness for Step 3

| Raw defect | Count |
|---|---:|
| missing_customer_lines | 478 |
| nonpositive_lines | 344 |
| future_lines | 151 |
| duplicate_lines | 771 |
| unknown_payment_statuses | 110 |

Order defect counts use unique line keys; duplicate excess is reported separately. Defects are retained in raw files.

Proposed silver handling for the next step:

- Deduplicate order line keys before joining payments, keeping payment outcomes at order grain.
- Enforce customer presence, positive amounts and valid event timestamps with bronze-to-silver expectations. Monitor the deduplicated-row loss rate separately from duplicate removal.
- Treat UNKNOWN payment statuses as invalid and preserve their count; an unknown outcome must not be silently reclassified as a terminal failure.
- Apply customer I/U/D events as SCD Type 2 ordered by effective timestamp and sequence; tier at order time must come from that history.
- Preserve source file path and ingestion timestamp during bronze ingestion. Read only `sources/<source>/`; never ingest `_staging` or `_control`.

## Verification

Checks: {"all_injected_defect_counts_match": true, "all_source_counts_match_step1": true, "baseline_near_2_percent": true, "outage_near_60_percent": true, "recovery_near_2_percent": true}.
All source row counts reconcile to Step 1. The main-gateway outage, recovery and all injected defect classes are visible. No source files were modified.

## UI checklist

- Catalog Explorer → rafa → dev_tariq_faisal1999_payments_raw → raw_data.
- Open `_control/reports/step-2-profile.json` for machine-readable metrics.
- Open the bundle's `docs/step-2-raw-profile.md` and `reports/step-2-hourly-gateways.csv` in Workspace.

Step 2 complete. Stop here until the user says continue.
