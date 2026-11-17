"""Execute every dataset and widget query through the Databricks CLI before deploy."""
import json
import os
from pathlib import Path
import re
import subprocess

CLI = str(Path(os.environ["LOCALAPPDATA"])/"Databricks/CLI/databricks.exe")
GOLD = "rafa.dev_tariq_faisal1999_payments_gold"


def assert_calculated_fields_acyclic(dataset):
    calculated={col["displayName"].casefold():col["expression"] for col in dataset.get("columns",[])}
    def visit(name, stack):
        assert name not in stack, f"Circular dashboard calculated field: {name}"
        for reference in re.findall(r"`([^`]+)`", calculated[name]):
            if reference.casefold() in calculated:
                visit(reference.casefold(),stack+[name])
    for name in calculated:
        visit(name,[])


def main():
    dashboard=json.loads(Path("src/dashboards/payments.lvdash.json").read_text())
    ds={d["name"]:d for d in dashboard["datasets"]}
    # Lakeview resolves calculated field names case-insensitively. SQL-only
    # tests miss cycles introduced by this extra semantic namespace.
    for dataset in ds.values():
        assert_calculated_fields_acyclic(dataset)
    sqls={}
    for name,d in ds.items():
        base="".join(d["queryLines"]).replace("FROM payments_metrics",f"FROM {GOLD}.payments_metrics")
        sqls[f"dataset_{name}"]=f"SELECT * FROM ({base}) LIMIT 5"
    for page in dashboard["pages"]:
        for item in page["layout"]:
            widget=item["widget"]
            for query in widget.get("queries",[]):
                q=query["query"]; dataset=ds[q["datasetName"]]
                measures={m["displayName"]:m["expression"] for m in dataset["columns"]}
                def resolve(expr):
                    return re.sub(r"MEASURE\(`([^`]+)`\)",lambda m:measures[m.group(1)],expr)
                fields=[f"{resolve(f['expression'])} AS `{f['name']}`" for f in q["fields"]]
                base="".join(dataset["queryLines"]).replace("FROM payments_metrics",f"FROM {GOLD}.payments_metrics")
                sql=f"WITH dataset AS ({base}) SELECT "+", ".join(fields)+" FROM dataset GROUP BY ALL"
                if q.get("orders"):
                    sql+=" ORDER BY "+", ".join(resolve(order["expression"])+" "+order["direction"] for order in q["orders"])
                sql+=" LIMIT 10000"
                sqls[f"{widget['name']}_{query['name']}"]=sql
    sqls["metric_totals"]=f"SELECT MEASURE(Revenue) AS revenue, MEASURE(`Lost Revenue`) AS lost_revenue, MEASURE(Orders) AS orders, MEASURE(`Failed Orders`) AS failed_orders, MEASURE(`Payment Failure Rate`) AS failure_rate, MEASURE(`Affected Enterprise Customers`) AS affected FROM {GOLD}.payments_metrics"
    sqls["filter_semantics"]=f"SELECT `Region`, MEASURE(Orders) AS orders FROM {GOLD}.payments_metrics WHERE `Date`=DATE '2026-10-10' AND `Gateway`='GatewayA' GROUP BY ALL"
    out=Path("reports/dashboard_sql");out.mkdir(exist_ok=True)
    paths=[]
    for name,sql in sqls.items():
        path=out/f"{name}.sql";path.write_text(sql,encoding="utf-8");paths.append(str(path))
    command=[CLI,"experimental","aitools","tools","query","--warehouse","d781c076806d9c09","-p","Faisal","-o","json"]
    for path in paths: command.extend(["--file",path])
    completed=subprocess.run(command,text=True,encoding="utf-8",capture_output=True)
    Path("reports/step-5-query-results.json").write_text(completed.stdout,encoding="utf-8")
    if completed.stderr: print(completed.stderr)
    results=json.loads(completed.stdout)
    summary=[{"name":name,"state":result.get("state"),"rows":len(result.get("rows") or []),"error":result.get("error")} for name,result in zip(sqls,results)]
    Path("reports/step-5-query-checks.json").write_text(json.dumps(summary,indent=2),encoding="utf-8")
    print(json.dumps(summary,indent=2),flush=True)
    assert completed.returncode==0 and len(results)==len(sqls), "A dashboard query failed"
    assert all(item["state"]=="SUCCEEDED" for item in summary),summary
    by_name={name:result for name,result in zip(sqls,results)}
    totals=by_name["metric_totals"]["rows"][0]
    assert [by_name[f"kpi_{i}_main_query"]["rows"][0][0] for i in range(6)] == totals, "Dashboard KPI rollups must match governed metrics"
    assert totals[2]=="111149" and totals[5]=="18", totals
    print(f"PASS: {len(results)} dataset/widget/semantic queries via CLI",flush=True)

if __name__=="__main__":main()
