# Step 4 — orchestration and incremental load

Manual job: [payments_incremental](https://dbc-cbef9784-5349.cloud.databricks.com/jobs/158198177201822).
Successful [run 217226658742723](https://dbc-cbef9784-5349.cloud.databricks.com/?o=7474652258155511#job/158198177201822/run/217226658742723); pipeline update `cc017f98-06e4-451e-ba8f-cdb80ae4d93c`.

One bundle defines the job and pipeline. Both dev and prod targets validate; only dev is deployed.
The single pipeline task sets `full_refresh: false`, runs on serverless in STANDARD mode, and has concurrency 1. There are no schedules, continuous triggers or file triggers.
Failure notifications are configured for tariq.faisal1999@outlook.com. Configuration was verified; no failure was induced and no test email was sent.

## New-day results

The November 1, 2026 source batch adds three Parquet files. Historical files were not modified or re-ingested.

| Bronze source | Before rows | Added rows | After rows | Added files |
|---|---:|---:|---:|---:|
| products | 200 | 0 | 200 | 0 |
| customers | 10,405 | 10 | 10,415 | 1 |
| orders | 165,493 | 2,238 | 167,731 | 1 |
| payments | 114,565 | 1,563 | 116,128 | 1 |

Gold now contains **111,149 orders**, including **1,498 November 1 orders**. November 1 net settlement revenue is **$168380.48**.
Historical gold order count is unchanged. Each fact order remains unique, every new order has its historical customer tier, and daily sales reconciles to the new-day fact count.

## Incremental evidence

The before snapshot records all 130 historical bronze file paths, row counts and first/last ingestion timestamps. The after comparison matches every historical entry exactly, and its three new file paths belong only to `batch=day-2026-11-01-v1`. New bronze row deltas match the source batch counts, and their ingestion timestamps fall within the manual job run.
Repeating the extra-day generator returned `SKIP day-2026-11-01-v1: complete, no files or events added`.

**24 checks passed.** Evidence: [verification](../reports/step-4-verification.json), [original before snapshot](../reports/step-4-before.json).
The verifier runs locally against serverless Spark for this handoff; no separate quality-check job was created. Pipeline expectations remain the enforcement mechanism.

## Databricks UI checklist

- Jobs & Pipelines → `[dev tariq_faisal1999] payments_incremental` → latest run → SUCCESS.
- Job settings → Notifications → failure email; Triggers → no trigger.
- Task settings → existing pipeline, full refresh disabled.
- Pipeline update → COMPLETED. Bronze file provenance includes the new day batch; historical ingestion timestamps are unchanged.
- Catalog Explorer → rafa → dev_tariq_faisal1999_payments_gold → order_fact / daily_sales → order_date 2026-11-01.

Step 4 complete. Stop here; the metric view and dashboard belong to Step 5.
