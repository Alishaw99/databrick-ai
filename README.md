# Payments Lakehouse

The five project steps are complete. Open the
[published dashboard](https://dbc-cbef9784-5349.cloud.databricks.com/dashboardsv3/01f1e220cca71de4a0f4b9f834558e02/published).
See `docs/step-5-results.md` for the metric view, query validation and publication details.

Project requirements: [PROJECT.md](PROJECT.md). Approved generation design:
[Step 1 data plan](docs/step-1-data-plan.md).

## Workspace and storage

- Profile: `Faisal` (browser OAuth; no credentials in the repository).
- Catalog: `rafa`.
- Development landing volume: `/Volumes/rafa/dev_tariq_faisal1999_payments_raw/raw_data`.
- Source input roots: `<volume>/sources/{products,customers,orders,payments}/`.
- `_staging` holds immutable generation attempts; `_control` holds publication manifests and reports. Neither directory is a pipeline input.
- Published files are multi-part Parquet under `batch=<id>/ingestion_date=<date>/`.
- The catalog was created manually with Default Storage and is not owned by the bundle. The landing schema and volume are bundle-managed.

## Local environment

Use Python 3.12 in the project virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

All Spark operations execute on Databricks serverless through Databricks Connect.
The CLI executable must be on PATH for OAuth authentication from the SDK.

## Generate and verify the approved history

```powershell
.\.venv\Scripts\python.exe -u src/generate_data.py --profile Faisal
```

Successful generation writes `reports/historical-v1.json` locally and a copy to the volume.
A read-only recheck of the published sources is available with `--validate-only`.
A completed batch exits immediately on rerun. An interrupted run reuses finished staging outputs and copies only missing immutable files. Incomplete staging attempts remain outside the ingestion directories. Do not run two generators concurrently.

The publication manifest is marked complete only after verification succeeds. Ingestion must start after publication completes. Data quality defects remain in the source files for pipeline expectations in Step 3.

## Incremental day (Step 4 only)

```powershell
.\.venv\Scripts\python.exe -u src/generate_data.py --profile Faisal --day 2026-11-01
```

This adds a new batch, preserves published history, and reads historical customer changes to maintain the new day's tier state. Repeating it skips a completed batch. No deletions, full refreshes, or overwrites of source files are performed.

## Profile raw sources (Step 2)

```powershell
.\.venv\Scripts\python.exe -u src/profile_raw.py --profile Faisal
```

This profiles the completed historical files on serverless compute. Read
`docs/step-2-raw-profile.md` for the findings, `reports/step-2-profile.json` for
column metrics and daily distributions, and `reports/step-2-hourly-gateways.csv`
for the incident and adjacent-day hourly comparison. It writes reports only,
leaving raw files untouched. No monitoring or quality-check job is created.

## Bundle

Step 3 implements the SQL medallion pipeline in `src/pipelines/`. Read
`docs/step-3-results.md` for table names, semantics, checks and UI verification.
Run an incremental update and verify outputs with:

```powershell
databricks bundle run payments -t dev
.\.venv\Scripts\python.exe -u src/verify_pipeline.py
```

`verify_pipeline.py` checks the historical-only Step 3 baseline. After adding
the extra day, use Step 4's before/after verification instead.

## Manual orchestration (Step 4)

The bundle resource `payments_incremental` runs the existing pipeline without a
full refresh. It has no schedule or file trigger, uses STANDARD serverless mode,
and emails the configured alert address on failure.

```powershell
databricks bundle run payments_incremental -t dev
```

The one-day demonstration uses `generate_data.py --day 2026-11-01`. The original
before-load evidence is preserved in `reports/step-4-before.json`. After the
manual job completes, verify ingestion and gold with:

```powershell
.\.venv\Scripts\python.exe -u src/verify_incremental.py --verify --job-id 158198177201822 --run-id 217226658742723 --update-id cc017f98-06e4-451e-ba8f-cdb80ae4d93c
```

Re-running the generator for an already completed batch skips without adding
files or events. The verifier compares each historical file's row count and
ingestion timestamps, and reconciles the newly added files and rows.

Do not request a full refresh or delete resources without explicit approval.

```powershell
databricks bundle validate --strict -t dev
databricks bundle validate --strict -t prod
databricks bundle deploy -t dev --auto-approve
```

Production validation does not deploy production resources. Stop after each project step and wait for `continue`.
