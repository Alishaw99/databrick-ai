# Step 1 completed — 2026-11-16

Workspace access verified with OAuth profile `Faisal`. The single `payments_lakehouse` bundle manages the development landing schema and managed volume. Both dev and prod targets pass strict validation; only dev is deployed. Production remains undeployed.

## Published raw files

Landing volume: `/Volumes/rafa/dev_tariq_faisal1999_payments_raw/raw_data`.

| Source | Rows | Parquet files |
| --- | ---: | ---: |
| Products | 200 | 1 |
| Customer change feed | 10,405 | 7 |
| Order lines, including injected duplicates | 165,493 | 61 |
| Payment events | 114,565 | 61 |
| Total files | | 130 |

There are **109,980 distinct orders** over **2026-09-01–2026-10-31**, with 10,000 initial customers, 305 update events and 100 delete events. The catalog is a small single Parquet file; orders and payments have daily multi-file source folders. The extra day, 2026-11-01, has not been generated.

Realized counts are below the approximate planning targets of 120K orders / 180K lines / 140K–170K payments. The daily/weekly acceptance model produces 109,980 orders; payment events contain real attempts and refunds, so only failed payments generate retry attempts. No artificial events were added to reach a count target.

## Business story checks

- Gateway A carries 76,693 / 109,980 orders: **69.73%**. Gateway B carries 20.27%, Gateway C 9.99% (rounding applies).
- Gateway A attempt failure rate: **2.17% baseline → 60.14% outage → 2.09% recovery**. The outage is October 10, 10:00–13:59 UTC.
- Gateway A mean latency rises from **450 ms** to **12,331 ms**, returning to **451 ms**.
- Enterprise is **5%** of initial customers and **50.18%** of pre-outage order value.
- **18** enterprise customers have ultimately failed main-gateway outage orders; **5** downgrade within 14 days.
- **1,759 orders** across the history are recovered by retries.
- Valid-record captured value: **$12,926,549.86**; refunds: **$317,869.38**; net revenue: **$12,608,680.48**.
- Valid-record terminal failed-order value: **$80,434.05** across the history; **$11,349.19** for main-gateway outage-window orders. This is observed failed-order value, not a causal incremental-loss estimate.

Financial checks deduplicate line keys, exclude invalid line records, aggregate order value before joining payment outcomes, and exclude 110 orders with unknown payment statuses. The raw defects remain untouched; production cleaning and expectations belong to Step 3.

## Quality defects deliberately preserved

| Defect | Measured count |
| --- | ---: |
| Missing customer line records | 478 |
| Non-positive line amounts | 344 |
| Future line timestamps | 151 |
| Exact duplicate line records | 771 |
| Unknown payment statuses | 110 |

Integrity checks found zero unexpected customer references, zero unknown product references and zero orphan payment events. The missing-customer cohort is intentional and reported separately.

## Repeatability and implementation

The repeated historical command returned `SKIP historical-v1: complete, no files or events added`. Completed batches use an immutable publication manifest at `_control/batches/historical-v1.json`. Finished source staging can be reused after interruption; only absent publication files are copied. `_staging` and `_control` must never be ingestion inputs. No resources were deleted, no full refresh was run and no published source files were overwritten.

Local Python 3.12 runs in `.venv`; dependencies are pinned in `requirements.txt` and fully recorded in `requirements.lock.txt`. All record generation and Spark verification execute on Databricks serverless through Connect. The workspace's isolated Python UDF environment failed to start, so the implementation uses a small local Faker vocabulary and native Spark expressions, without remote Python UDFs.

Machine-readable evidence: [historical-v1.json](../reports/historical-v1.json), also copied to `_control/reports/historical-v1.json` in the volume. Generator: [generate_data.py](../src/generate_data.py). Run instructions: [README.md](../README.md).

## Databricks UI checklist

- Catalog Explorer → `rafa` → `dev_tariq_faisal1999_payments_raw` → Volumes → `raw_data`.
- Browse `sources/products`, `sources/customers`, `sources/orders`, `sources/payments` → `batch=historical-v1` → ingestion date folders → Parquet files.
- Open `_control/reports/historical-v1.json` to inspect metrics and `_control/batches/historical-v1.json` to confirm `status: complete`.
- No bronze, silver, gold, pipeline, orchestration job or dashboard is expected yet.

Stopped after Step 1. Step 2 starts only when the user says `continue`.
