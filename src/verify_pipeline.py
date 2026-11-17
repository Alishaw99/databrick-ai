"""Read-only Step 3 integration verification on serverless Spark."""
import json
from pathlib import Path
from decimal import Decimal
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F, Window

PREFIX = "rafa.dev_tariq_faisal1999_payments_"
PIPELINE_ID = "b6118dc8-faaa-4c10-89e5-0d58e6b7a068"


def main():
    spark = DatabricksSession.builder.profile("Faisal").serverless(True).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    raw = json.loads(Path("reports/historical-v1.json").read_text())
    counts = {}
    for layer, names in {"bronze": ["products", "customers", "orders", "payments"],
                         "silver": ["products", "customer_events", "customer_history", "orders", "payments"],
                         "gold": ["order_fact", "daily_sales", "hourly_gateway_health", "customer_outage_impact", "recovery_trend"]}.items():
        for name in names:
            counts[f"{layer}.{name}"] = spark.table(f"{PREFIX}{layer}.{name}").count()
            print(f"{layer}.{name}: {counts[f'{layer}.{name}']}", flush=True)
    table = lambda layer, name: spark.table(f"{PREFIX}{layer}.{name}")
    checks = {}
    checks["bronze_matches_raw"] = all(counts[f"bronze.{name}"] == count for name, count in raw["counts"].items())
    checks["bronze_provenance"] = all(table("bronze", name).filter(F.col("source_file").isNull() | F.col("ingested_at").isNull()
        | F.col("source_file").contains("/_staging/")).count() == 0 for name in raw["counts"])
    checks["silver_line_count"] = counts["silver.orders"] == raw["counts"]["orders"] - sum(raw["quality"][key]
        for key in ("duplicate_lines", "missing_customer_lines", "nonpositive_lines", "future_lines"))
    checks["silver_payment_count"] = counts["silver.payments"] == raw["counts"]["payments"]-raw["quality"]["unknown_payment_statuses"]
    silver = table("silver", "orders")
    fact = table("gold", "order_fact")
    bronze_orders = table("bronze", "orders").select("order_id").distinct().count()
    silver_orders = silver.select("order_id").distinct().count()
    loss = 1-silver_orders/bronze_orders
    checks["order_loss_below_1_percent"] = loss <= .01
    checks["silver_keys_unique"] = counts["silver.orders"] == silver.select("line_key").distinct().count()
    checks["fact_one_row_per_order"] = counts["gold.order_fact"] == silver_orders == fact.select("order_id").distinct().count()
    checks["fact_historical_tier_resolved"] = fact.filter(F.col("tier_at_order_time").isNull() | F.col("region").isNull()).count() == 0
    history = table("silver", "customer_history")
    checks["customer_current_count"] = history.filter(F.col("__END_AT").isNull()).count() == 9900
    window = Window.partitionBy("customer_id").orderBy("__START_AT")
    overlaps = history.withColumn("next_start", F.lead("__START_AT").over(window)).filter(
        F.col("next_start").isNotNull() & (F.col("__END_AT").isNull() | (F.col("__END_AT") > F.col("next_start"))))
    checks["customer_history_no_overlap"] = overlaps.count() == 0
    # Independent reconciliation: payment captures/refunds over orders surviving silver.
    payments = table("bronze", "payments")
    unknowns = payments.filter(F.col("status").isNull() | ~F.col("status").isin("succeeded", "failed", "refunded")).select("order_id").distinct()
    amounts = payments.join(unknowns, "order_id", "left_anti").join(silver.select("order_id").distinct(), "order_id")
    expected = amounts.agg(F.sum(F.when(F.col("status") == "succeeded", F.col("amount").cast("decimal(18,2)"))
        .when(F.col("status") == "refunded", -F.col("amount").cast("decimal(18,2)"))
        .otherwise(Decimal("0.00"))).alias("revenue")).first().revenue
    actual = fact.agg(F.sum("revenue")).first()[0]
    daily = table("gold", "daily_sales").agg(F.sum("revenue")).first()[0]
    checks["settlement_revenue_reconciles"] = expected == actual == daily
    checks["unknown_outcomes_not_lost_revenue"] = fact.filter((F.col("payment_outcome") == "unknown") & (F.col("lost_revenue") != 0)).count() == 0
    incident = table("gold", "hourly_gateway_health").filter((F.col("gateway") == "GatewayA")
        & (F.col("event_hour") >= F.lit("2026-10-10 10:00:00").cast("timestamp"))
        & (F.col("event_hour") < F.lit("2026-10-10 14:00:00").cast("timestamp")))
    rate = incident.agg((F.sum("failed_attempts")/F.sum("attempts")).alias("rate")).first().rate
    checks["outage_visible_in_gold"] = .5 < rate < .7
    impact = table("gold", "customer_outage_impact")
    affected = impact.filter("affected_enterprise").count()
    downgraded = impact.filter("downgraded_within_14_days").count()
    checks["enterprise_impact_matches_raw_story"] = affected == 18 and downgraded == 5
    report = {"pipeline_id": PIPELINE_ID, "counts": counts, "checks": checks, "distinct_bronze_orders": bronze_orders,
              "distinct_silver_orders": silver_orders, "order_loss_rate": loss, "net_revenue_usd": str(actual),
              "gateway_a_outage_failure_rate": rate, "affected_enterprise_customers": affected,
              "downgraded_enterprise_customers": downgraded,
              "revenue_definition": "Captured payment amount less successful refunds; omit unknown outcome orders. Partial invalid lines do not reduce the independently recorded settlement amount."}
    Path("reports/step-3-verification.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2), flush=True)
    assert all(checks.values()), checks


if __name__ == "__main__":
    main()
