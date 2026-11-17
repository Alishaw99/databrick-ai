# Step 5 — governed metrics and published dashboard

Completed November 16, 2026 (America/Los_Angeles).

**[Open the published dashboard](https://dbc-cbef9784-5349.cloud.databricks.com/dashboardsv3/01f1e220cca71de4a0f4b9f834558e02/published)**

## Governed semantic layer

Metric view: `rafa.dev_tariq_faisal1999_payments_gold.payments_metrics`.

- Measures: Revenue, Lost Revenue, Orders, Failed Orders, Payment Failure Rate and Affected Enterprise Customers, with atomic Payment Attempts and Failed Attempts available for safe ratio rollups.
- Dimensions: Date, Hour, Region, Channel, Gateway and Tier (at order time), plus customer detail and incident/downgrade dimensions.
- Source is the gold order fact. The customer impact join is one row per customer. Affected customer counts use distinct customer IDs and re-aggregate at query time.
- The source-controlled SQL is applied by the manual bundle job `deploy_metrics`. The canonical template generates complete dev/prod definitions. Only dev was deployed and run; no schedule is attached.

## Dashboard

The bundle-managed dashboard contains six KPI cards, hourly gateway failure rates, lost revenue by region and historical tier, a daily recovery trend and an impacted enterprise-customer table sorted by lost revenue. Gateway A is red, with a vertical annotation at the incident start. Chart time is explicitly **order time in UTC**; this is the metric-view grain, not a separate event-time failure metric.

The Date, Region and Gateway filters bind both datasets. The date default is October 9–11, 2026. Clearing the date filter restores September 1–November 1. The second dataset contains affected enterprise outage orders for customer detail; it shares the same dimensions and filter bindings.

The dashboard queries the metric view at reusable dimensional grain. Additive measures roll up with sums, the failure rate divides aggregate failed attempts by aggregate attempts, and affected customers use distinct IDs rather than summing subgroup counts. KPI rollups reconcile exactly to the governed measures.

## Validation

All **21 dataset/widget/filter-semantic SQL queries** executed successfully through the CLI on Serverless Starter Warehouse (`d781c076806d9c09`) before dashboard deployment. Queries and results are saved under `reports/dashboard_sql/`, `reports/step-5-query-results.json`, and `reports/step-5-query-checks.json`.

All-history metric totals:

| Metric | Value |
|---|---:|
| Net settlement revenue | $12,825,072.30 |
| Terminal failed order value | $81,966.93 |
| Orders | 111,149 |
| Ultimately failed orders | 717 |
| Payment attempt failure rate | 2.30684% |
| Affected enterprise customers | 18 |

The outage cohort remains traceable to 18 affected enterprise customers, of whom 5 subsequently downgraded. The incident is October 10, 10:00–13:59 UTC. Unknown outcomes are excluded from revenue and lost revenue; successful retries are not losses. Lost revenue is observed terminal failed order value, not a causal counterfactual estimate. Default-filter KPI values cover the incident comparison window, so they differ from the all-history totals above.

Both bundle targets pass strict validation. Only dev was deployed. The dashboard is ACTIVE, its published revision was confirmed through `lakeview get-published`, and its saved datasets resolve to the dev gold schema and confirmed warehouse. Publication does not embed credentials or expand sharing permissions; existing workspace/data permissions apply.

## Reproduction

1. Run `python src/build_dashboard.py` in the project virtual environment to render metric definitions and dashboard JSON.
2. Deploy dev, then run `databricks bundle run deploy_metrics -t dev` to apply the metric view.
3. Run `python src/test_dashboard.py` to execute all dashboard SQL and reconciliation checks before deploying a changed dashboard.
4. Deploy dev and publish the dashboard revision using `databricks lakeview publish <dashboard-id> --warehouse-id d781c076806d9c09 --profile Faisal`.

## UI checklist

### Rendering repair

The initial SQL checks passed but missed a Lakeview calculated-field namespace error: `BAD_REQUEST: Circular reference detected in calculated field: revenue`. The calculated measure `Revenue` collided case-insensitively with its input alias `revenue`, causing all six KPI cards to fail before normal query execution. Dashboard input aliases now use the `metric_` prefix, distinct from calculated measure names. Metric-view definitions and financial values are unchanged.

After republishing, the published dashboard was inspected directly in Chrome: all six KPI cards, hourly chart, region/tier bars, recovery chart and impacted customer table rendered successfully. Default October 9–11 values displayed $569.41K revenue, $14.77K lost revenue, 4.95K orders, 139 failed orders, 7.02% attempt failure rate and 18 affected enterprise customers.

All 21 SQL checks and KPI reconciliation assertions passed again. `tests/test_dashboard_fields.py` adds three regression checks: reject the original case-insensitive self-reference, reject indirect cycles, and accept the repaired dashboard. All three passed.

- Open the published dashboard link above. Initial filters focus on October 9–11.
- Inspect the hourly chart for Gateway A's failure spike and recovery; hover points for exact values.
- Change Region and Gateway to check impact slices. Clear Date to see the full history.
- Inspect impacted enterprise customers and the Downgraded column.
- In Catalog Explorer, open the gold `payments_metrics` metric view and inspect its measure definitions.

The five project steps are complete. Raw files, pipeline, manual incremental job, semantic layer and published dashboard are retained in the workspace and source-controlled bundle. Production remains undeployed.
