"""Step 2: read-only serverless profiling; output JSON, CSV and Markdown.

Run with the project venv: python src/profile_raw.py --profile Faisal
No source writes, monitors, tables, jobs or pipeline changes.
"""
import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F, types as T
from generate_data import ROOT, read_json, write_json


def rows(df):
    return [r.asDict(recursive=True) for r in df.toLocalIterator()]


def category(df, column):
    return rows(df.groupBy(column).count().orderBy(F.desc("count"), column).limit(20))


def profile(name, df):
    print(f"Profiling {name}: schema, nulls, cardinality, ranges and files", flush=True)
    expressions = [F.count("*").alias("row_count")]
    for field in df.schema.fields:
        col = F.col(field.name)
        expressions += [F.sum(col.isNull().cast("long")).alias(field.name + "__nulls"),
                        F.count_distinct(col).alias(field.name + "__distinct")]
        if isinstance(field.dataType, T.NumericType):
            expressions += [F.min(col).alias(field.name + "__min"), F.max(col).alias(field.name + "__max"),
                            F.avg(col).alias(field.name + "__mean"),
                            F.percentile_approx(col, [.5, .95, .99], 10000).alias(field.name + "__quantiles")]
        elif isinstance(field.dataType, (T.DateType, T.TimestampType)):
            expressions += [F.min(col).cast("string").alias(field.name + "__min"),
                            F.max(col).cast("string").alias(field.name + "__max")]
        elif isinstance(field.dataType, T.StringType):
            expressions += [F.sum((F.trim(col) == "").cast("long")).alias(field.name + "__blanks")]
    metrics = df.agg(*expressions).first().asDict()
    columns = {field.name: {"type": field.dataType.simpleString(),
        **{key.split("__", 1)[1]: value for key, value in metrics.items() if key.startswith(field.name + "__")}}
        for field in df.schema.fields}
    # File metadata comes from the actual parquet scan, not a pre-existing manifest.
    inventory = df.select(F.col("_metadata.file_path").alias("file"), F.col("_metadata.file_size").alias("bytes"))
    inventory = inventory.groupBy("file").agg(F.max("bytes").alias("bytes"))
    sizes = inventory.agg(F.count("*").alias("files"), F.sum("bytes").alias("bytes"),
        F.min("bytes").alias("smallest_file_bytes"), F.max("bytes").alias("largest_file_bytes")).first().asDict()
    keys = {"products": ["product_id"], "customers": ["event_id"], "orders": ["line_key"], "payments": ["payment_event_id"]}[name]
    return {"rows": metrics["row_count"], "columns": columns, "storage": sizes,
        "exact_duplicate_rows": metrics["row_count"] - df.dropDuplicates().count(),
        "duplicate_key_excess": metrics["row_count"] - df.select(*keys).distinct().count(),
        "distributions": {}}


def markdown(report):
    result = ["# Step 2 — raw data profile", "", f"Profiled {report['profiled_at_utc']} using profile `Faisal`, Databricks Connect and serverless Spark.",
        "Sources are immutable historical-v1 Parquet files; all profiling is read-only. No quality monitor or check job was created.", "",
        "## Volume and source summary", "", f"Volume: `{ROOT}`", "",
        "| Source | Rows | Files | Bytes | Duplicate keys |", "|---|---:|---:|---:|---:|"]
    for name, source in report["sources"].items():
        result.append(f"| {name} | {source['rows']:,} | {source['storage']['files']} | {source['storage']['bytes']:,} | {source['duplicate_key_excess']} |")
    result += ["", "## Nulls, keys and date ranges", ""]
    for name, source in report["sources"].items():
        nulls = ", ".join(f"{col}: {data['nulls']}" for col, data in source["columns"].items() if data["nulls"])
        dates = "; ".join(f"{col}: {data['min']} through {data['max']}" for col, data in source["columns"].items()
            if data["type"] in ("date", "timestamp"))
        result += [f"- **{name}** — nulls: {nulls or 'none in any column'}; exact duplicate rows: {source['exact_duplicate_rows']}. {dates}"]
    result += ["", "Order timestamps include deliberate 2099 values. Use ingestion dates to describe historical file coverage; flag future event timestamps in silver. Payments can settle just after midnight on November 1 for October 31 orders; this does not mean the reserved November 1 incremental batch has been generated.",
        "", "## Value distributions", ""]
    for name, source in report["sources"].items():
        for column, distribution in source["distributions"].items():
            result.append(f"- **{name}.{column}**: " + ", ".join(f"{row[column]} = {row['count']:,}" for row in distribution))
    result += ["", "Customer tier/region distributions above count CDC events, not unique customers or historical order cohorts.", "",
        "| Numeric field | Min | Median | P95 | Max |", "|---|---:|---:|---:|---:|"]
    for name, source in report["sources"].items():
        for column, data in source["columns"].items():
            if "quantiles" in data:
                result.append(f"| {name}.{column} | {data['min']} | {data['quantiles'][0]} | {data['quantiles'][1]} | {data['max']} |")
    result += ["", "## Outage visibility", "", "Rates below exclude UNKNOWN statuses and use payment attempts, including retries, rather than counting refund events as attempts.", "",
        "| Gateway | Period | Attempts | Failure rate | Mean latency ms | Retries |", "|---|---|---:|---:|---:|---:|"]
    for row in report["gateway_periods"]:
        result.append(f"| {row['gateway']} | {row['period']} | {row['attempts']} | {row['failure_rate']:.2%} | {row['mean_latency_ms']:.1f} | {row['retries']} |")
    result += ["", "The incident window is October 10, 10:00–13:59 UTC. Adjacent-day hourly evidence is in `reports/step-2-hourly-gateways.csv`; daily source volumes and every column's exact null/cardinality and numerical summaries are in `reports/step-2-profile.json`.", "",
        "## Defects and readiness for Step 3", "", "| Raw defect | Count |", "|---|---:|"]
    for key, value in report["quality"].items():
        result.append(f"| {key} | {value:,} |")
    result += ["", "Order defect counts use unique line keys; duplicate excess is reported separately. Defects are retained in raw files.",
        "", "Proposed silver handling for the next step:", "",
        "- Deduplicate order line keys before joining payments, keeping payment outcomes at order grain.",
        "- Enforce customer presence, positive amounts and valid event timestamps with bronze-to-silver expectations. Monitor the deduplicated-row loss rate separately from duplicate removal.",
        "- Treat UNKNOWN payment statuses as invalid and preserve their count; an unknown outcome must not be silently reclassified as a terminal failure.",
        "- Apply customer I/U/D events as SCD Type 2 ordered by effective timestamp and sequence; tier at order time must come from that history.",
        "- Preserve source file path and ingestion timestamp during bronze ingestion. Read only `sources/<source>/`; never ingest `_staging` or `_control`.", "",
        "## Verification", "", f"Checks: {json.dumps(report['checks'], sort_keys=True)}.",
        "All source row counts reconcile to Step 1. The main-gateway outage, recovery and all injected defect classes are visible. No source files were modified.", "",
        "## UI checklist", "", "- Catalog Explorer → rafa → dev_tariq_faisal1999_payments_raw → raw_data.",
        "- Open `_control/reports/step-2-profile.json` for machine-readable metrics.",
        "- Open the bundle's `docs/step-2-raw-profile.md` and `reports/step-2-hourly-gateways.csv` in Workspace.",
        "", "Step 2 complete. Stop here until the user says continue."]
    return "\n".join(result) + "\n"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="Faisal")
    args = parser.parse_args()
    w = WorkspaceClient(profile=args.profile)
    state = read_json(w, f"{ROOT}/_control/batches/historical-v1.json")
    if not state or state.get("status") != "complete":
        raise RuntimeError("Historical batch must be complete")
    spark = DatabricksSession.builder.profile(args.profile).serverless(True).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    dfs = {name: spark.read.parquet(info["published"]) for name, info in state["sources"].items()}
    report = {"profiled_at_utc": datetime.now(timezone.utc).isoformat(), "sources": {}, "paths": {
        name: info["published"] for name, info in state["sources"].items()}}
    categoricals = {"products": ["category"], "customers": ["operation", "tier", "region"],
        "orders": ["channel"], "payments": ["gateway", "event_type", "status", "attempt_number"]}
    for name, df in dfs.items():
        source = profile(name, df)
        source["distributions"] = {col: category(df, col) for col in categoricals[name]}
        source["daily_volumes"] = rows(df.groupBy("ingestion_date").count().orderBy("ingestion_date"))
        report["sources"][name] = source
        Path("reports").mkdir(exist_ok=True)
        Path("reports/step-2-profile.inprogress.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        print(f"Finished {name}: {source['rows']:,} rows, {source['storage']['files']} files", flush=True)
    lines = dfs["orders"].dropDuplicates(["line_key"])
    report["quality"] = lines.agg(F.sum(F.col("customer_id").isNull().cast("int")).alias("missing_customer_lines"),
        F.sum((F.col("line_amount") <= 0).cast("int")).alias("nonpositive_lines"),
        F.sum((F.col("order_ts") > F.current_timestamp()).cast("int")).alias("future_lines")).first().asDict()
    report["quality"]["duplicate_lines"] = report["sources"]["orders"]["duplicate_key_excess"]
    payments = dfs["payments"]
    report["quality"]["unknown_payment_statuses"] = payments.filter(~F.col("status").isin("failed", "succeeded", "refunded")).count()
    print("Profiling gateway incident and hourly recovery", flush=True)
    attempts = payments.filter((F.col("event_type") == "attempt") & F.col("status").isin("succeeded", "failed"))
    attempts = attempts.withColumn("period", F.when((F.to_date("event_ts") == F.lit("2026-10-10").cast("date")) & F.hour("event_ts").between(10, 13), "outage")
        .when(F.col("event_ts") >= F.lit("2026-10-10 14:00:00").cast("timestamp"), "recovery").otherwise("baseline"))
    def health(df, columns):
        return df.groupBy(*columns).agg(F.count("*").alias("attempts"),
            F.avg((F.col("status") == "failed").cast("double")).alias("failure_rate"),
            F.avg("latency_ms").alias("mean_latency_ms"),
            F.sum((F.col("attempt_number") > 1).cast("long")).alias("retries")).orderBy(*columns)
    report["gateway_periods"] = rows(health(attempts, ["gateway", "period"]))
    hourly = health(attempts.filter(F.to_date("event_ts").between("2026-10-09", "2026-10-11"))
        .withColumn("hour_utc", F.date_trunc("hour", "event_ts")), ["gateway", "hour_utc"])
    # Spark Connect materializes timestamp objects in the local Python timezone.
    # Cast on the server so the exported label stays UTC as declared.
    hourly_rows = rows(hourly.withColumn("hour_utc", F.col("hour_utc").cast("string")))
    with Path("reports/step-2-hourly-gateways.csv").open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(hourly_rows[0]))
        writer.writeheader()
        writer.writerows(hourly_rows)
    expected = json.loads(Path("reports/historical-v1.json").read_text(encoding="utf-8"))
    primary = {row["period"]: row for row in report["gateway_periods"] if row["gateway"] == "GatewayA"}
    report["checks"] = {"all_source_counts_match_step1": all(report["sources"][name]["rows"] == count for name, count in expected["counts"].items()),
        "all_injected_defect_counts_match": report["quality"] == expected["quality"],
        "baseline_near_2_percent": .01 < primary["baseline"]["failure_rate"] < .035,
        "outage_near_60_percent": .5 < primary["outage"]["failure_rate"] < .7,
        "recovery_near_2_percent": .01 < primary["recovery"]["failure_rate"] < .035}
    assert all(report["checks"].values()), report["checks"]
    # Normalize dates once for identical local and remote JSON.
    report = json.loads(json.dumps(report, default=str))
    Path("reports/step-2-profile.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    Path("docs/step-2-raw-profile.md").write_text(markdown(report), encoding="utf-8")
    write_json(w, f"{ROOT}/_control/reports/step-2-profile.json", report)
    print(json.dumps({"checks": report["checks"], "quality": report["quality"], "sources": {
        name: {"rows": value["rows"], "storage": value["storage"]} for name, value in report["sources"].items()}}, indent=2), flush=True)
    print("Step 2 COMPLETE", flush=True)


if __name__ == "__main__":
    main()
