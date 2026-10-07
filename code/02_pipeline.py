"""Bronze -> Silver -> Gold batch pipeline for inbound-logistics visibility (Namanve case).
Run:  python code/02_pipeline.py     Outputs: lake/*.parquet and metrics.json"""
import pandas as pd, numpy as np, json, time, hashlib, duckdb, datetime as dt
T0 = time.perf_counter(); M = {"stages": {}, "dq": {}, "rejects": {}}
def stage(name):
    class S:
        def __enter__(s): s.t = time.perf_counter()
        def __exit__(s, *a): M["stages"][name] = round(time.perf_counter() - s.t, 3)
    return S()
RUN_TS = dt.datetime(2026, 10, 4, 6, 0, 0).isoformat()

# ============ BRONZE: ingest as-received, add lineage columns ============
with stage("bronze_ingest"):
    erp  = pd.read_csv("raw/erp_purchase_orders.csv", dtype=str)
    gate = pd.read_csv("raw/gate_log.csv", dtype=str)
    rain = pd.read_csv("raw/rainfall_daily.csv")
    ev   = pd.read_csv("raw/road_events.csv")
    sup  = pd.read_csv("raw/suppliers.csv")
    disp = pd.DataFrame(json.load(open("raw/transporter_dispatch.json")))
    for name, df, src in [("erp", erp, "erp_purchase_orders.csv"), ("gate", gate, "gate_log.csv"),
                          ("rain", rain, "rainfall_daily.csv"), ("events", ev, "road_events.csv"),
                          ("dispatch", disp, "transporter_dispatch.json"), ("suppliers", sup, "suppliers.csv")]:
        d = df.copy(); d["_source"] = src; d["_ingested_at"] = RUN_TS
        d.to_parquet(f"lake/bronze/{name}.parquet", index=False)
    M["bronze_rows"] = {"erp": len(erp), "gate": len(gate), "rain": len(rain), "events": len(ev), "dispatch": len(disp), "suppliers": len(sup)}

# ---- DQ profile of RAW gate log (before) ----
g_iso = pd.to_datetime(gate.gate_arrival_ts, format="%Y-%m-%d %H:%M:%S", errors="coerce")
raw_complete = gate.gate_arrival_ts.notna().mean()
raw_valid_fmt = g_iso.notna().sum() / max(gate.gate_arrival_ts.notna().sum(), 1)
raw_unique = 1 - gate.duplicated("po_number").mean()
sup_ids = set(sup.supplier_id)
raw_consistent = erp.supplier_id.isin(sup_ids).mean()
M["dq"]["before"] = {"completeness": raw_complete, "format_validity": raw_valid_fmt, "uniqueness": raw_unique, "supplier_consistency": raw_consistent}

# ============ SILVER: clean, conform, validate, quarantine ============
with stage("silver_clean"):
    quarantine = []
    # --- ERP
    e = erp.drop_duplicates("po_number").copy()
    e["supplier_id"] = e["supplier_id"].str.upper().str.strip()
    e["planned_arrival_ts"] = pd.to_datetime(e["planned_arrival_ts"], errors="coerce")
    e["load_tonnes"] = pd.to_numeric(e["load_tonnes"], errors="coerce")
    bad_e = e[~e.supplier_id.isin(sup_ids) | e.planned_arrival_ts.isna()]
    for r in bad_e.itertuples(): quarantine.append((r.po_number, "ERP_INVALID_SUPPLIER_OR_PLAN"))
    e = e.drop(bad_e.index)
    # --- Gate
    gd = gate.drop_duplicates("po_number", keep="first").copy()
    M["rejects"]["gate_duplicates_removed"] = int(len(gate) - len(gd))
    a = pd.to_datetime(gd.gate_arrival_ts, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    alt = pd.to_datetime(gd.gate_arrival_ts, format="%d/%m/%Y %H:%M", errors="coerce")
    M["rejects"]["gate_dates_reformatted"] = int((a.isna() & alt.notna()).sum())
    gd["gate_arrival_ts"] = a.fillna(alt)
    gd["offload_complete_ts"] = pd.to_datetime(gd.offload_complete_ts, errors="coerce")
    # --- Dispatch
    dd = disp.drop_duplicates("po").rename(columns={"po": "po_number"})
    dd["dispatch_ts"] = pd.to_datetime(dd.dispatched_at, errors="coerce")
    # --- Join
    s = e.merge(gd[["po_number", "truck_id", "gate_arrival_ts", "offload_complete_ts"]], on="po_number", how="left") \
         .merge(dd[["po_number", "dispatch_ts"]], on="po_number", how="left") \
         .merge(sup[["supplier_id", "zone"]], on="supplier_id", how="left")
    std = {"Kampala Industrial Area": 1.5, "Mukono": 1.0, "Entebbe": 2.0, "Jinja": 2.5, "Mbale": 5.5, "Mbarara": 6.5}
    s["std_lead_h"] = s.zone.map(std)
    s["dispatch_imputed"] = s.dispatch_ts.isna()
    s.loc[s.dispatch_imputed, "dispatch_ts"] = s.planned_arrival_ts - pd.to_timedelta(s.std_lead_h, unit="h")
    # Rules
    r_missing = s.gate_arrival_ts.isna()
    r_impossible = (~r_missing) & (s.gate_arrival_ts < s.dispatch_ts)
    for r in s[r_missing].itertuples(): quarantine.append((r.po_number, "GATE_ARRIVAL_MISSING"))
    for r in s[r_impossible].itertuples(): quarantine.append((r.po_number, "ARRIVAL_BEFORE_DISPATCH"))
    M["rejects"]["gate_arrival_missing"] = int(r_missing.sum()); M["rejects"]["arrival_before_dispatch"] = int(r_impossible.sum())
    M["rejects"]["dispatch_imputed"] = int(s.dispatch_imputed.sum())
    M["rejects"]["erp_invalid"] = int(len(bad_e))
    silver = s[~r_missing & ~r_impossible].copy()
    q = pd.DataFrame(quarantine, columns=["po_number", "reject_reason"]); q["_run_ts"] = RUN_TS
    q.to_parquet("lake/silver/quarantine.parquet", index=False)
    silver["_run_ts"] = RUN_TS
    silver.to_parquet("lake/silver/deliveries.parquet", index=False)
    # Rainfall
    r = rain.copy(); r["date"] = pd.to_datetime(r["date"])
    r["rain_imputed"] = r.rainfall_mm.isna()
    month_med = r.groupby(r.date.dt.month).rainfall_mm.transform("median")
    r["rainfall_mm"] = r.rainfall_mm.fillna(month_med)
    M["rejects"]["rain_days_imputed"] = int(r.rain_imputed.sum())
    r.to_parquet("lake/silver/rainfall.parquet", index=False)
    # Road events -> daily flags
    ev2 = ev.copy(); ev2["date"] = pd.to_datetime(ev2["date"])
    ev_d = ev2.pivot_table(index="date", columns="event_type", values="severity", aggfunc="max").reindex(columns=["FLOODING", "ROADWORKS", "ACCIDENT"]).notna().astype(int)
    ev_d.columns = [c.lower() + "_flag" for c in ev_d.columns]; ev_d = ev_d.reset_index()
    ev_d.to_parquet("lake/silver/road_events_daily.parquet", index=False)
    M["silver_rows"] = {"deliveries": len(silver), "quarantined": len(q), "rainfall": len(r), "events_daily": len(ev_d)}

# ---- DQ profile AFTER (silver) ----
M["dq"]["after"] = {"completeness": float(silver.gate_arrival_ts.notna().mean()), "format_validity": 1.0,
                    "uniqueness": float(1 - silver.duplicated("po_number").mean()),
                    "supplier_consistency": float(silver.supplier_id.isin(sup_ids).mean())}

# ============ GOLD: analytics-ready fact + KPI tables (SQL on DuckDB) ============
with stage("gold_build"):
    con = duckdb.connect()
    con.execute("CREATE VIEW d AS SELECT * FROM 'lake/silver/deliveries.parquet'")
    con.execute("CREATE VIEW r AS SELECT * FROM 'lake/silver/rainfall.parquet'")
    con.execute("CREATE VIEW v AS SELECT * FROM 'lake/silver/road_events_daily.parquet'")
    fact = con.execute("""
      WITH rr AS (
        SELECT date, rainfall_mm,
               SUM(rainfall_mm) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rain_3d_mm
        FROM r)
      SELECT d.po_number, d.supplier_id, d.zone, d.material, d.load_tonnes, d.dispatch_ts, d.planned_arrival_ts, d.gate_arrival_ts,
             d.std_lead_h, date_diff('minute', d.planned_arrival_ts, d.gate_arrival_ts) AS delay_min,
             CASE WHEN date_diff('minute', d.planned_arrival_ts, d.gate_arrival_ts) <= 30 THEN 1 ELSE 0 END AS on_time,
             rr.rainfall_mm AS rain_mm_day, rr.rain_3d_mm,
             COALESCE(v.flooding_flag,0) AS flooding_flag, COALESCE(v.roadworks_flag,0) AS roadworks_flag, COALESCE(v.accident_flag,0) AS accident_flag,
             hour(d.dispatch_ts) AS dispatch_hour, dayofweek(d.dispatch_ts) AS dow, month(d.dispatch_ts) AS month
      FROM d LEFT JOIN rr ON rr.date = CAST(d.dispatch_ts AS DATE)
             LEFT JOIN v ON v.date = CAST(d.dispatch_ts AS DATE)
      ORDER BY d.dispatch_ts""").df()
    fact.to_parquet("lake/gold/delivery_fact.parquet", index=False)
    kpi = con.execute("""
      SELECT strftime(CAST(dispatch_ts AS DATE), '%Y-%m') AS month, COUNT(*) AS deliveries,
             ROUND(100*AVG(CASE WHEN date_diff('minute', planned_arrival_ts, gate_arrival_ts) <= 30 THEN 1 ELSE 0 END),1) AS on_time_pct,
             ROUND(AVG(date_diff('minute', planned_arrival_ts, gate_arrival_ts)),1) AS mean_delay_min
      FROM d GROUP BY 1 ORDER BY 1""").df()
    kpi.to_parquet("lake/gold/monthly_kpi.parquet", index=False)
    M["gold_rows"] = {"delivery_fact": len(fact), "monthly_kpi": len(kpi)}

# ============ TESTS: reconciliation, idempotency ============
distinct_pos = erp.po_number.nunique()
M["tests"] = {"reconciliation_ok": bool(len(silver) + len(q[q.reject_reason != "ERP_INVALID_SUPPLIER_OR_PLAN"]) + len(bad_e) == distinct_pos),
              "fact_unique_po": bool(fact.po_number.is_unique),
              "no_negative_dwell": bool(((fact.gate_arrival_ts - fact.dispatch_ts).dt.total_seconds() >= 0).all())}
h1 = hashlib.md5(pd.util.hash_pandas_object(fact, index=False).values.tobytes()).hexdigest()
M["gold_hash_run1"] = h1
M["total_runtime_s"] = round(time.perf_counter() - T0, 3)
json.dump(M, open("metrics.json", "w"), indent=1, default=float)
print(json.dumps(M, indent=1, default=float))
