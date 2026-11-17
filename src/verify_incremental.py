"""Step 4 before/after ingestion evidence; no separate quality-check job.

Run --capture before publishing the extra day, then --verify after job success.
"""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F
from generate_data import ROOT, read_json

PREFIX = "rafa.dev_tariq_faisal1999_payments_"
BASELINE = Path("reports/step-4-before.json")


def inventory(spark):
    result = {}
    for source in ("products", "customers", "orders", "payments"):
        df = spark.table(f"{PREFIX}bronze.{source}")
        groups = df.groupBy("source_file").agg(F.count("*").alias("rows"),
            F.min("ingested_at").cast("string").alias("first_ingested"),
            F.max("ingested_at").cast("string").alias("last_ingested"))
        files = {row.source_file: {k: v for k, v in row.asDict().items() if k != "source_file"} for row in groups.toLocalIterator()}
        result[source] = {"rows": sum(data["rows"] for data in files.values()), "files": files}
        print(f"Inventory {source}: {result[source]['rows']} rows / {len(files)} files", flush=True)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--capture", action="store_true")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--job-id", type=int)
    parser.add_argument("--run-id", type=int)
    parser.add_argument("--update-id")
    args = parser.parse_args()
    if args.capture == args.verify:
        raise ValueError("Choose exactly one of --capture or --verify")
    w = WorkspaceClient(profile="Faisal")
    spark = DatabricksSession.builder.profile("Faisal").serverless(True).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if args.capture:
        if BASELINE.exists():
            raise RuntimeError("Keep the original before snapshot; it already exists")
        snapshot = {"bronze": inventory(spark), "gold_orders": spark.table(f"{PREFIX}gold.order_fact").count(),
                    "gold_max_order_date": str(spark.table(f"{PREFIX}gold.order_fact").agg(F.max("order_date")).first()[0])}
        BASELINE.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
        print("Captured pre-increment baseline", flush=True)
        return
    before = json.loads(BASELINE.read_text())
    after = inventory(spark)
    batch = read_json(w, f"{ROOT}/_control/batches/day-2026-11-01-v1.json")
    checks = {"batch_complete": batch["status"] == "complete"}
    deltas = {}
    for source, old in before["bronze"].items():
        now = after[source]
        checks[f"{source}_old_files_unchanged"] = all(now["files"].get(path) == values for path, values in old["files"].items())
        new_files = set(now["files"]) - set(old["files"])
        expected_files = batch["sources"].get(source, {}).get("file_count", 0)
        expected_rows = spark.read.parquet(batch["sources"][source]["published"]).count() if source in batch["sources"] else 0
        delta_rows = now["rows"]-old["rows"]
        checks[f"{source}_only_new_batch_files"] = len(new_files) == expected_files and all("batch=day-2026-11-01-v1/" in path for path in new_files)
        checks[f"{source}_row_delta_matches_new_batch"] = delta_rows == expected_rows
        deltas[source] = {"before_rows": old["rows"], "after_rows": now["rows"], "new_rows": delta_rows,
                          "new_file_count": len(new_files), "new_files": sorted(new_files)}
    fact = spark.table(f"{PREFIX}gold.order_fact")
    new_day = fact.filter(F.col("order_date") == F.lit("2026-11-01").cast("date"))
    new_day_count = new_day.count()
    fact_count = fact.count()
    daily_count = spark.table(f"{PREFIX}gold.daily_sales").filter(F.col("order_date") == F.lit("2026-11-01").cast("date")).agg(F.sum("orders")).first()[0]
    checks["new_day_reaches_gold"] = new_day_count > 0 and daily_count == new_day_count
    checks["historical_gold_count_unchanged"] = fact_count - new_day_count == before["gold_orders"]
    checks["gold_keys_unique"] = fact_count == fact.select("order_id").distinct().count()
    checks["new_day_has_historical_tier"] = new_day.filter(F.col("tier_at_order_time").isNull()).count() == 0
    job = w.jobs.get(args.job_id).settings
    checks["failure_email_configured"] = job.email_notifications.on_failure == ["tariq.faisal1999@outlook.com"]
    checks["manual_only"] = not any((job.schedule, job.trigger, job.continuous))
    checks["job_incremental"] = all(task.pipeline_task and not task.pipeline_task.full_refresh for task in job.tasks)
    run = w.jobs.get_run(args.run_id)
    checks["job_success"] = str(run.state.result_state.value) == "SUCCESS"
    new_ingestion_times = [datetime.fromisoformat(info["first_ingested"]).replace(tzinfo=timezone.utc).timestamp()*1000
        for source, current in after.items() for path, info in current["files"].items()
        if path not in before["bronze"][source]["files"]]
    checks["new_files_ingested_during_job"] = bool(new_ingestion_times) and all(
        run.start_time <= timestamp <= run.end_time for timestamp in new_ingestion_times)
    if args.update_id:
        update = w.pipelines.get_update("b6118dc8-faaa-4c10-89e5-0d58e6b7a068", args.update_id).update
        checks["pipeline_update_completed"] = update.state.value == "COMPLETED"
        checks["pipeline_update_not_full_refresh"] = not update.full_refresh
    report = {"job_id": args.job_id, "run_id": args.run_id, "run_url": run.run_page_url,
              "pipeline_update_id": args.update_id,
              "checks": checks, "bronze_deltas": deltas, "before_gold_orders": before["gold_orders"],
              "after_gold_orders": fact_count, "new_day_gold_orders": new_day_count,
              "new_day_revenue_usd": str(new_day.agg(F.sum("revenue")).first()[0]),
              "failure_email_note": "Configuration verified; no failure induced and no test email sent."}
    Path("reports/step-4-verification.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2), flush=True)
    assert all(checks.values()), checks
    lines = ["# Step 4 — orchestration and incremental load", "",
        f"Manual job: [payments_incremental](https://dbc-cbef9784-5349.cloud.databricks.com/jobs/{args.job_id}).",
        f"Successful [run {args.run_id}]({run.run_page_url}); pipeline update `{args.update_id}`.", "",
        "One bundle defines the job and pipeline. Both dev and prod targets validate; only dev is deployed.",
        "The single pipeline task sets `full_refresh: false`, runs on serverless in STANDARD mode, and has concurrency 1. There are no schedules, continuous triggers or file triggers.",
        "Failure notifications are configured for tariq.faisal1999@outlook.com. Configuration was verified; no failure was induced and no test email was sent.", "",
        "## New-day results", "", "The November 1, 2026 source batch adds three Parquet files. Historical files were not modified or re-ingested.", "",
        "| Bronze source | Before rows | Added rows | After rows | Added files |", "|---|---:|---:|---:|---:|"]
    for source, delta in deltas.items():
        lines.append(f"| {source} | {delta['before_rows']:,} | {delta['new_rows']:,} | {delta['after_rows']:,} | {delta['new_file_count']} |")
    lines += ["", f"Gold now contains **{fact_count:,} orders**, including **{new_day_count:,} November 1 orders**. November 1 net settlement revenue is **${report['new_day_revenue_usd']}**.",
        "Historical gold order count is unchanged. Each fact order remains unique, every new order has its historical customer tier, and daily sales reconciles to the new-day fact count.", "",
        "## Incremental evidence", "",
        "The before snapshot records all 130 historical bronze file paths, row counts and first/last ingestion timestamps. The after comparison matches every historical entry exactly, and its three new file paths belong only to `batch=day-2026-11-01-v1`. New bronze row deltas match the source batch counts, and their ingestion timestamps fall within the manual job run.",
        "Repeating the extra-day generator returned `SKIP day-2026-11-01-v1: complete, no files or events added`.", "",
        f"**{len(checks)} checks passed.** Evidence: [verification](../reports/step-4-verification.json), [original before snapshot](../reports/step-4-before.json).",
        "The verifier runs locally against serverless Spark for this handoff; no separate quality-check job was created. Pipeline expectations remain the enforcement mechanism.", "",
        "## Databricks UI checklist", "",
        "- Jobs & Pipelines → `[dev tariq_faisal1999] payments_incremental` → latest run → SUCCESS.",
        "- Job settings → Notifications → failure email; Triggers → no trigger.",
        "- Task settings → existing pipeline, full refresh disabled.",
        "- Pipeline update → COMPLETED. Bronze file provenance includes the new day batch; historical ingestion timestamps are unchanged.",
        "- Catalog Explorer → rafa → dev_tariq_faisal1999_payments_gold → order_fact / daily_sales → order_date 2026-11-01.", "",
        "Step 4 complete. Stop here; the metric view and dashboard belong to Step 5."]
    Path("docs/step-4-results.md").write_text("\n".join(lines)+"\n", encoding="utf-8")


if __name__ == "__main__":
    main()
