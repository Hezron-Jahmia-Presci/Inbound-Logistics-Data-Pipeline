"""Synthetic source-data generator for the Namanve inbound-logistics capstone.
All data are SIMULATED under documented assumptions (see report, Section 3.4).
Ground-truth effects are planted so the pipeline's analytics can be validated."""
import numpy as np, pandas as pd, json
rng = np.random.default_rng(2026)

START, END = "2024-01-01", "2025-12-31"
days = pd.date_range(START, END, freq="D")

# ---- Rainfall (mm/day): bimodal climatology (Mar-May, Sep-Nov wet seasons) ----
p_rain = {1:.20,2:.25,3:.50,4:.65,5:.55,6:.25,7:.20,8:.30,9:.45,10:.60,11:.60,12:.35}
mean_amt = {1:6,2:7,3:12,4:15,5:13,6:7,7:6,8:8,9:11,10:14,11:14,12:9}
rain = []
for d in days:
    m = d.month
    rain.append(rng.gamma(1.1, 0.75*mean_amt[m]) if rng.random() < p_rain[m] else 0.0)
rain = np.round(np.array(rain), 1)
rain_s = pd.Series(rain, index=days)
rain_df = pd.DataFrame({"date": days, "rainfall_mm": rain, "station": "NAMANVE-GRID"})
rain_df.loc[rng.choice(len(rain_df), 18, replace=False), "rainfall_mm"] = np.nan   # sensor/API gaps
rain_df["date"] = rain_df["date"].dt.strftime("%Y-%m-%d")
rain_df.to_csv("raw/rainfall_daily.csv", index=False)

# ---- Road events (manual log: flooding, roadworks, accident) ----
ev = []
for d in days:
    r = rain_s[d]
    if r > 30 and rng.random() < 0.5: ev.append((d, "FLOODING", int(rng.integers(2,4))))
    if rng.random() < 0.035: ev.append((d, "ROADWORKS", 1))
    if rng.random() < 0.02: ev.append((d, "ACCIDENT", int(rng.integers(1,3))))
ev_df = pd.DataFrame(ev, columns=["date","event_type","severity"])
ev_df["date_s"] = ev_df["date"].dt.strftime("%Y-%m-%d")
ev_df[["date_s","event_type","severity"]].rename(columns={"date_s":"date"}).to_csv("raw/road_events.csv", index=False)

# ---- Suppliers & routes ----
routes = {"Kampala Industrial Area": (1.5, 1.4), "Mukono": (1.0, 1.0), "Entebbe": (2.0, 1.6),
          "Jinja": (2.5, 1.3), "Mbale": (5.5, 1.8), "Mbarara": (6.5, 2.0)}
zones = list(routes)
mats = ["Packaging", "Resin/Chemicals", "Steel coil", "Agro raw material", "Spare parts"]
sup = pd.DataFrame({"supplier_id":[f"S{i:02d}" for i in range(1,13)],
                    "zone":[zones[i%6] for i in range(12)],
                    "material":[mats[i%5] for i in range(12)]})
sup["supplier_name"] = "Supplier " + sup["supplier_id"]
sup.to_csv("raw/suppliers.csv", index=False)

# ---- Deliveries ----
ev_by_day = {k: g for k, g in ev_df.groupby("date")}
rows = []; n_per_day = 6.0
dow_w = {0:1.15,1:1.1,2:1.1,3:1.05,4:1.0,5:.55,6:.1}
po = 100000
for d in days:
    k = rng.poisson(n_per_day*dow_w[d.dayofweek])
    for _ in range(k):
        po += 1
        s = sup.iloc[int(rng.integers(0,12))]
        std_h, sens = routes[s.zone]
        disp_h = rng.uniform(5.0, 16.0)
        dispatch = d + pd.Timedelta(hours=disp_h)
        planned = dispatch + pd.Timedelta(hours=std_h)
        r0 = rain_s[d]
        r3 = rain_s[max(d - pd.Timedelta(days=2), days[0]):d].sum()
        ev_min = 0.0
        if d in ev_by_day:
            for e in ev_by_day[d].itertuples():
                ev_min += {"FLOODING":120*e.severity/2.5,"ROADWORKS":45,"ACCIDENT":60}[e.event_type]
        peak = 20 if (7 <= disp_h <= 9 or disp_h >= 16) else 0
        peak *= 1 if s.zone in ("Kampala Industrial Area","Mukono") else 0.3
        delay = (rng.normal(8, 14) + sens*r0 + 0.25*sens*(r3-r0) + ev_min + peak + rng.exponential(10))
        arrival = planned + pd.Timedelta(minutes=float(delay))
        offload = arrival + pd.Timedelta(minutes=float(rng.normal(55,15)))
        rows.append(dict(po_number=f"PO{po}", supplier_id=s.supplier_id, zone=s.zone, material=s.material,
                         truck_id=f"UA{int(rng.integers(100,999))}{'ABCDEFGH'[int(rng.integers(0,8))]}",
                         load_tonnes=round(float(rng.uniform(5,28)),1), dispatch_ts=dispatch,
                         planned_arrival_ts=planned, gate_arrival_ts=arrival, offload_complete_ts=offload))
D = pd.DataFrame(rows)
print("deliveries generated:", len(D))

# ---- Source 1: ERP purchase orders (CSV) ----
erp = D[["po_number","supplier_id","material","load_tonnes","planned_arrival_ts"]].copy()
erp["planned_arrival_ts"] = erp["planned_arrival_ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
lc = rng.random(len(erp)) < 0.02
erp.loc[lc, "supplier_id"] = erp.loc[lc, "supplier_id"].str.lower()          # 2% case errors
erp.to_csv("raw/erp_purchase_orders.csv", index=False)

# ---- Source 2: Gate/weighbridge log (CSV, dirty) ----
gate = D[["po_number","truck_id","gate_arrival_ts","offload_complete_ts"]].copy()
n = len(gate)
gate["gate_arrival_ts"] = gate["gate_arrival_ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
gate["offload_complete_ts"] = gate["offload_complete_ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
fmt = rng.choice(n, int(.015*n), replace=False)
gate.loc[gate.index[fmt],"gate_arrival_ts"] = pd.to_datetime(gate.loc[gate.index[fmt],"gate_arrival_ts"]).dt.strftime("%d/%m/%Y %H:%M")
bad = rng.choice(n, int(.01*n), replace=False)
gate.loc[gate.index[bad],"gate_arrival_ts"] = (D.loc[D.index[bad],"dispatch_ts"] - pd.Timedelta(hours=3)).dt.strftime("%Y-%m-%d %H:%M:%S")
miss = rng.choice(n, int(.02*n), replace=False); gate.loc[gate.index[miss],"gate_arrival_ts"] = np.nan
dup = gate.sample(int(.03*n), random_state=1)
gate = pd.concat([gate, dup]).sample(frac=1, random_state=3).reset_index(drop=True)
gate.to_csv("raw/gate_log.csv", index=False)

# ---- Source 3: transporter dispatch feed (JSON) ----
disp = [dict(po=r.po_number, dispatched_at=r.dispatch_ts.strftime("%Y-%m-%dT%H:%M:%S"),
             origin=r.zone, truck=r.truck_id) for r in D.itertuples()]
miss_d = set(rng.choice(len(disp), int(.015*len(disp)), replace=False).tolist())
disp = [x for i,x in enumerate(disp) if i not in miss_d]
json.dump(disp, open("raw/transporter_dispatch.json","w"))
print("raw files written")
