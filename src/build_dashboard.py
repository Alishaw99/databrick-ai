"""Render source-controlled metric SQL and Lakeview JSON; no remote writes."""
import json
from pathlib import Path

GOLD = {"dev": "rafa.dev_tariq_faisal1999_payments_gold", "prod": "rafa.payments_gold_prod"}

def main():
    template = Path("src/metrics/payments.template.sql").read_text()
    for target, schema in GOLD.items():
        Path(f"src/metrics/{target}.sql").write_text(template.replace("__GOLD__", schema), encoding="utf-8")
    columns = ["Date", "Hour", "Region", "Channel", "Gateway", "Tier", "Customer ID", "Customer Name", "Affected Customer ID", "Downgraded"]
    measures = {"Revenue": "metric_revenue", "Lost Revenue": "metric_lost_revenue", "Orders": "metric_orders", "Failed Orders": "metric_failed_orders", "Payment Attempts": "metric_attempts", "Failed Attempts": "metric_failed_attempts"}
    select = [f"`{col}` AS `{col.lower().replace(' ', '_')}`" for col in columns]
    select += [f"MEASURE(`{name}`) AS {alias}" for name, alias in measures.items()]
    base = "SELECT " + ", ".join(select) + "\nFROM payments_metrics\n"
    queries = {"payments": base+"GROUP BY ALL ORDER BY hour, gateway",
               "enterprise": base+"WHERE `Affected Customer ID` IS NOT NULL\nGROUP BY ALL ORDER BY metric_lost_revenue DESC"}
    declared = [{"displayName": name, "expression": f"SUM(`{alias}`)"} for name, alias in measures.items()]
    declared += [{"displayName": "Payment Failure Rate", "expression": "SUM(`metric_failed_attempts`) / NULLIF(SUM(`metric_attempts`),0)"},
                 {"displayName": "Affected Enterprise Customers", "expression": "COUNT(DISTINCT `affected_customer_id`)"}]
    datasets = [{"name": name, "displayName": name.title(), "queryLines": [query], "columns": declared} for name, query in queries.items()]
    currency = {"type":"number-currency", "currencyCode":"USD", "abbreviation":"compact", "decimalPlaces":{"type":"max","places":2}}
    percent = {"type":"number-percent", "decimalPlaces":{"type":"max","places":2}}
    layout = []
    def add(widget, x,y,w,h):
        layout.append({"widget":widget,"position":{"x":x,"y":y,"width":w,"height":h}})
    def text(name, value, y):
        add({"name":name,"multilineTextboxSpec":{"lines":[value]}},0,y,12,1)
    def widget(name, title, dataset, fields, kind, encodings, x,y,w,h, orders=None):
        query={"datasetName":dataset,"fields":[{"name":n,"expression":e} for n,e in fields],"disaggregated":False}
        if orders: query["orders"] = orders
        spec={"version":3 if kind in ("line","bar") else 2,"widgetType":kind,
              "encodings":encodings,"frame":{"title":title,"showTitle":True}}
        add({"name":name,"queries":[{"name":"main_query","query":query}],"spec":spec},x,y,w,h)
    text("title","# Payments outage — revenue and customer impact",0)
    text("subtitle","October 10 · 10:00–14:00 UTC · Gateway A | Baseline ≈2%, outage ≈60%, then recovery. All times are order time (UTC).",1)
    for index,(field,title,kind) in enumerate([("date","Date range","filter-date-range-picker"),("region","Region","filter-multi-select"),("gateway","Gateway","filter-multi-select")]):
        qs=[]; fs=[]
        for ds in queries:
            qname=f"{ds}_{field}"
            qs.append({"name":qname,"query":{"datasetName":ds,"fields":[{"name":field,"expression":f"`{field}`"}],"disaggregated":False}})
            fs.append({"fieldName":field,"displayName":title,"queryName":qname})
        spec={"version":2,"widgetType":kind,"encodings":{"fields":fs},"frame":{"title":title,"showTitle":True}}
        if field=="date":
            spec["selection"]={"defaultSelection":{"range":{"dataType":"DATE","min":{"value":"2026-10-09"},"max":{"value":"2026-10-11"}}}}
        add({"name":f"filter_{field}","queries":qs,"spec":spec},index*4,2,4,2)
    cards=[("Revenue","Net settlement revenue",currency),("Lost Revenue","Terminal failed order value",currency),("Orders","Orders",None),
           ("Failed Orders","Ultimately failed orders",None),("Payment Failure Rate","Payment attempt failure rate",percent),("Affected Enterprise Customers","Affected enterprise customers",None)]
    for i,(measure,title,fmt) in enumerate(cards):
        field_name=f"measure({measure})"
        value={"fieldName":field_name,"displayName":title,"rowNumber":0}
        if fmt: value["format"]=fmt
        widget(f"kpi_{i}",title,"payments",[(field_name,f"MEASURE(`{measure}`)")],"counter",{"value":value},(i%3)*4,4+(i//3)*3,4,3)
    text("incident_heading","## Gateway incident and recovery",10)
    widget("incident","Hourly failure rate by gateway — order time UTC","payments",[("hour","`hour`"),("gateway","`gateway`"),("failure_rate","MEASURE(`Payment Failure Rate`)")],"line",
           {"x":{"fieldName":"hour","scale":{"type":"temporal"}},"y":{"fieldName":"failure_rate","scale":{"type":"quantitative"},"format":percent},"color":{"fieldName":"gateway","scale":{"type":"categorical"}}},0,11,12,6,
           [{"direction":"ASC","expression":"`hour`"}])
    widget("lost_region","Lost revenue by region","payments",[("region","`region`"),("lost","MEASURE(`Lost Revenue`)")],"bar",
           {"x":{"fieldName":"region","scale":{"type":"categorical"}},"y":{"fieldName":"lost","scale":{"type":"quantitative"},"format":currency}},0,17,6,5)
    widget("lost_tier","Lost revenue by tier at order time","payments",[("tier","`tier`"),("lost","MEASURE(`Lost Revenue`)")],"bar",
           {"x":{"fieldName":"tier","scale":{"type":"categorical"}},"y":{"fieldName":"lost","scale":{"type":"quantitative"},"format":currency}},6,17,6,5)
    widget("recovery","Daily failure-rate recovery by gateway","payments",[("date","`date`"),("gateway","`gateway`"),("failure_rate","MEASURE(`Payment Failure Rate`)")],"line",
           {"x":{"fieldName":"date","scale":{"type":"temporal"}},"y":{"fieldName":"failure_rate","scale":{"type":"quantitative"},"format":percent},"color":{"fieldName":"gateway","scale":{"type":"categorical"}}},0,22,12,5,
           [{"direction":"ASC","expression":"`date`"}])
    fields=[("customer_id","`customer_id`"),("customer_name","`customer_name`"),("region","`region`"),("lost","MEASURE(`Lost Revenue`)"),("failed","MEASURE(`Failed Orders`)"),("downgraded","`downgraded`")]
    widget("top_customers","Top impacted enterprise customers — outage failures","enterprise",fields,"table",
           {"columns":[{"fieldName":n,"displayName":n.replace('_',' ').title(),**({"format":currency} if n=="lost" else {})} for n,_ in fields]},0,27,12,7,
           [{"direction":"DESC","expression":"MEASURE(`Lost Revenue`)"}])
    text("definitions","Revenue = captures minus refunds; lost revenue = terminal failed orders (not causal incremental loss). Unknown outcomes are excluded from revenue/loss. Filter dates to Oct 10 for the incident; 5 affected enterprise customers downgraded afterward.",34)
    for item in layout:
        spec=item["widget"].get("spec",{})
        if spec.get("widgetType")=="line":
            spec["encodings"]["color"]["scale"]["mappings"]=[{"value":gateway,"color":color} for gateway,color in [("GatewayA","#C83E3A"),("GatewayB","#2272B4"),("GatewayC","#178577")]]
            spec["annotations"]=[{"type":"vertical-line","encodings":{"x":{"dataValue":"2026-10-10T10:00:00.000","dataType":"DATETIME"},"label":{"value":"Gateway A outage"}}}]
    dashboard={"datasets":datasets,"pages":[{"name":"overview","displayName":"Payment outage","pageType":"PAGE_TYPE_CANVAS","layoutVersion":"GRID_V1","layout":layout}]}
    Path("src/dashboards").mkdir(exist_ok=True)
    Path("src/dashboards/payments.lvdash.json").write_text(json.dumps(dashboard,indent=2),encoding="utf-8")
    Path("src/dashboard_queries").mkdir(exist_ok=True)
    for name,q in queries.items():
        Path(f"src/dashboard_queries/{name}.sql").write_text(q,encoding="utf-8")
    print("Rendered metric SQL for dev/prod and dashboard datasets/widgets")

if __name__ == "__main__": main()
