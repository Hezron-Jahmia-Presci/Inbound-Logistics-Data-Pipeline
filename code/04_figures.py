import json, pandas as pd, numpy as np, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Rectangle
plt.rcParams["font.family"] = "Liberation Serif"
NAVY, BLUE, TEAL, GOLD, PUR, RED, GREY = "#2E5E8B", "#3F7CAC", "#3C8E7C", "#C98A2A", "#8A4F9E", "#B23F3F", "#5A5A5A"
R = json.load(open("results.json")); M = json.load(open("metrics.json"))
f = pd.read_parquet("lake/gold/delivery_fact.parquet"); rain = pd.read_parquet("lake/silver/rainfall.parquet")

def box(ax, x, y, w, h, fc, title, sub=None, tc="white", fs=9):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.15,rounding_size=1.0", fc=fc, ec="black", lw=0.7))
    if sub:
        ax.text(x+w/2, y+h/2+h*0.2, title, ha="center", va="center", color=tc, fontsize=fs, fontweight="bold")
        ax.text(x+w/2, y+h/2-h*0.2, sub, ha="center", va="center", color=tc, fontsize=fs-1.5)
    else:
        ax.text(x+w/2, y+h/2, title, ha="center", va="center", color=tc, fontsize=fs, fontweight="bold")
def arr(ax, x1, y1, x2, y2, c="black"):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1), arrowprops=dict(arrowstyle="-|>", lw=1.2, color=c))

# ---- Fig 1 conceptual framework ----
fig = plt.figure(figsize=(6.4, 2.9), dpi=220); ax = fig.add_axes([0,0,1,1]); ax.set_xlim(0,100); ax.set_ylim(0,45); ax.axis("off")
ax.add_patch(FancyBboxPatch((1,3),42,39,boxstyle="round,pad=0.2,rounding_size=1.2",fc="#EAF1F8",ec=NAVY,lw=1.2))
ax.text(22,39,"INDEPENDENT VARIABLE", ha="center", fontsize=8.5, fontweight="bold", color=NAVY)
ax.text(22,35.5,"Integrated logistics data pipeline", ha="center", fontsize=9.5, fontweight="bold")
for i,t in enumerate(["Data integration (ERP, gate, dispatch)","Data quality (completeness, validity)","Timeliness / visibility (batch refresh)","Weather & road-event enrichment"]):
    box(ax, 4, 24.5-i*7.6, 36, 6, BLUE, t, fs=8.2)
ax.add_patch(FancyBboxPatch((57,3),42,39,boxstyle="round,pad=0.2,rounding_size=1.2",fc="#EAF4F0",ec=TEAL,lw=1.2))
ax.text(78,39,"DEPENDENT VARIABLE", ha="center", fontsize=8.5, fontweight="bold", color=TEAL)
ax.text(78,35.5,"Inbound delivery reliability", ha="center", fontsize=9.5, fontweight="bold")
for i,t in enumerate(["On-time delivery rate (≤ 30 min late)","Mean delivery delay (minutes)","ETA accuracy (mean absolute error)"]):
    box(ax, 60, 24.5-i*7.6, 36, 6, TEAL, t, fs=8.2)
arr(ax,43.5,22.5,56.5,22.5); ax.text(50,25.5,"H1, H2", ha="center", fontsize=9, fontweight="bold", color=RED)
fig.savefig("figs/fig_framework.png", facecolor="white"); plt.close(fig)

# ---- Fig 2 pipeline architecture ----
fig = plt.figure(figsize=(6.4, 3.4), dpi=220); ax = fig.add_axes([0,0,1,1]); ax.set_xlim(0,100); ax.set_ylim(0,58); ax.axis("off")
srcs = [("ERP purchase orders","CSV"),("Gate / weighbridge log","CSV"),("Transporter dispatch feed","JSON"),("Rainfall (daily)","CSV / API"),("Road-event log","CSV (manual)")]
for i,(a,b) in enumerate(srcs): box(ax, 1, 44-i*10, 20, 8, GREY, a, b, fs=7.6)
ax.text(11,55.6,"SOURCES",ha="center",fontsize=8,fontweight="bold",color=GREY)
box(ax, 28, 19, 15, 20, "#8A5A2F", "BRONZE", "raw, as received\n+ lineage columns", fs=8.5)
box(ax, 49, 19, 15, 20, "#7F8FA3", "SILVER", "clean, conform,\nvalidate, quarantine", fs=8.5)
box(ax, 70, 19, 14, 20, "#C9A227", "GOLD", "delivery_fact,\nmonthly KPIs", tc="black", fs=8.5)
box(ax, 88, 33, 11.5, 9, PUR, "Dashboard", "BI / Excel", fs=7.6)
box(ax, 88, 17, 11.5, 9, RED, "ETA model", "delay risk", fs=7.6)
for i in range(5): arr(ax,21.3,48-i*10,27.6,32 if i<2 else (29 if i==2 else 26))
arr(ax,43.3,29,48.6,29); arr(ax,64.3,29,69.6,29); arr(ax,84.3,32,87.7,36); arr(ax,84.3,26,87.7,22)
ax.text(63.7,14.6,"Orchestration: daily batch job (Python / Apache Airflow DAG)  |  Storage: Parquet  |  SQL: DuckDB",ha="center",fontsize=7.4,style="italic")
ax.add_patch(Rectangle((28,4),71.5,5.2,fc="#DCCFAE",ec="none")); ax.text(63.7,6.6,"Cross-cutting: data-quality rules • lineage • run metrics • access control • tests",ha="center",va="center",fontsize=7.8,fontweight="bold")
fig.savefig("figs/fig_pipeline.png", facecolor="white"); plt.close(fig)

# ---- Fig 3 data model (star schema) ----
fig = plt.figure(figsize=(6.4, 3.0), dpi=220); ax = fig.add_axes([0,0,1,1]); ax.set_xlim(0,100); ax.set_ylim(0,48); ax.axis("off")
ax.add_patch(FancyBboxPatch((33,5),34,38,boxstyle="round,pad=0.2,rounding_size=1",fc=NAVY,ec="black",lw=0.8))
ax.text(50,40,"delivery_fact (Gold)",ha="center",color="white",fontsize=9.5,fontweight="bold")
for i,t in enumerate(["po_number (PK)","supplier_id (FK)","dispatch_ts, planned_arrival_ts","gate_arrival_ts","delay_min, on_time","rain_mm_day, rain_3d_mm","flooding / roadworks / accident flags","dispatch_hour, dow, month"]):
    ax.text(35,35.5-i*3.7,t,color="white",fontsize=7.6)
box(ax,2,28,24,13,TEAL,"dim_supplier","supplier_id, zone, material",fs=8.5)
box(ax,2,5,24,13,GOLD,"silver.rainfall","date, rainfall_mm, imputed flag",tc="black",fs=8.5)
box(ax,74,28,24,13,PUR,"silver.road_events_daily","date, 3 event flags",fs=8.2)
box(ax,74,5,24,13,GREY,"silver.quarantine","po_number, reject_reason",fs=8.5)
arr(ax,26.3,34,32.7,34); arr(ax,26.3,11,32.7,15); arr(ax,73.7,34,67.3,34); arr(ax,73.7,11,67.3,15,c=GREY)
fig.savefig("figs/fig_datamodel.png", facecolor="white"); plt.close(fig)

# ---- Fig 4 monthly on-time vs rainfall ----
kpi = pd.read_parquet("lake/gold/monthly_kpi.parquet")
rain["ym"] = rain.date.dt.strftime("%Y-%m"); mr = rain.groupby("ym").rainfall_mm.sum()
fig, ax = plt.subplots(figsize=(6.4, 2.9), dpi=220)
x = np.arange(len(kpi)); ax.bar(x, mr.reindex(kpi.month).values, color="#9DB7D1", label="Monthly rainfall (mm)")
ax.set_ylabel("Rainfall (mm)", fontsize=8.5); ax.set_xticks(x[::2]); ax.set_xticklabels(kpi.month[::2], rotation=45, ha="right", fontsize=7.5)
ax2 = ax.twinx(); ax2.plot(x, kpi.on_time_pct, color=RED, marker="o", ms=3.5, lw=1.8, label="On-time deliveries (%)")
ax2.set_ylabel("On-time (%)", fontsize=8.5, color=RED); ax2.set_ylim(0, 100)
h1,l1 = ax.get_legend_handles_labels(); h2,l2 = ax2.get_legend_handles_labels(); ax.legend(h1+h2, l1+l2, fontsize=7.5, loc="upper right", ncol=2, frameon=False)
ax.set_ylim(0, mr.max()*1.35); ax.tick_params(labelsize=7.5); ax2.tick_params(labelsize=7.5)
for s in ("top",): ax.spines[s].set_visible(False); ax2.spines[s].set_visible(False)
fig.tight_layout(); fig.savefig("figs/fig_monthly.png", facecolor="white"); plt.close(fig)

# ---- Fig 5 two-panel: delay by rain band + model vs baseline ----
fig, (a, b) = plt.subplots(1, 2, figsize=(6.4, 2.8), dpi=220, gridspec_kw={"width_ratios":[1.25,1]})
bands = [r["band"].replace(" (", "\n(") for r in R["bands"]]; md = [r["mean_delay"] for r in R["bands"]]; ot = [r["on_time_pct"] for r in R["bands"]]
bars = a.bar(bands, md, color=[ "#9DB7D1", BLUE, GOLD, RED ], ec="black", lw=0.5)
for bb, v, o in zip(bars, md, ot): a.text(bb.get_x()+bb.get_width()/2, v+3, f"{v:.0f} min\n{o:.0f}% on time", ha="center", fontsize=7)
a.set_ylabel("Mean delay (min)", fontsize=8.5); a.set_ylim(0, max(md)*1.3); a.tick_params(labelsize=7); a.set_title("(a) Delay by rainfall on dispatch day", fontsize=8.5, fontweight="bold")
vals = [R["metrics_base"]["MAE"], R["metrics_model"]["MAE"]]
bb = b.bar(["Baseline\n(supplier average)", "Pipeline\nETA model"], vals, color=[GREY, TEAL], ec="black", lw=0.5, width=0.55)
for r_, v in zip(bb, vals): b.text(r_.get_x()+r_.get_width()/2, v+0.5, f"{v:.1f} min", ha="center", fontsize=8, fontweight="bold")
b.set_ylim(0, 24); b.set_ylabel("ETA error: MAE (min)", fontsize=8.5); b.tick_params(labelsize=7.5); b.set_title("(b) ETA error, test set", fontsize=8.5, fontweight="bold")
for ax_ in (a, b): ax_.spines["top"].set_visible(False); ax_.spines["right"].set_visible(False)
fig.tight_layout(); fig.savefig("figs/fig_results.png", facecolor="white"); plt.close(fig)

# ---- Fig 6 Gantt (appendix) ----
tasks = [("Discovery, access, ethics approval",0,2,NAVY),("Bronze ingestion + lineage",2,4,BLUE),("Silver cleaning + DQ rules",4,7,TEAL),("Gold tables + ETA model",7,9,GOLD),("Dashboard + user training",8,11,PUR),("Pilot run + validation",10,12,RED),("Risk, change & stakeholder updates",0,12,GREY)]
fig, ax = plt.subplots(figsize=(6.4, 2.8), dpi=220)
for i,(n,s,e,c) in enumerate(tasks):
    y = len(tasks)-1-i; ax.barh(y, e-s, left=s, height=0.55, color=c, ec="black", lw=0.5)
ax.set_yticks(range(len(tasks))); ax.set_yticklabels([t[0] for t in reversed(tasks)], fontsize=8)
ax.set_xticks(range(0,13)); ax.set_xticklabels([f"W{w}" if w else "" for w in range(13)], fontsize=7.5); ax.set_xlim(0,12.2)
for g in (2,7,12): ax.axvline(g, color=RED, ls="--", lw=0.8)
ax.set_xlabel("Project week (proposed 12-week pilot)", fontsize=8.5)
for s_ in ("top","right"): ax.spines[s_].set_visible(False)
ax.grid(axis="x", alpha=0.25); fig.tight_layout(); fig.savefig("figs/fig_gantt.png", facecolor="white"); plt.close(fig)

# ---- Fig 7 dashboard mock-up (appendix) ----
fig = plt.figure(figsize=(6.4, 3.4), dpi=220, facecolor="#F4F6F8")
gs = fig.add_gridspec(2, 4, height_ratios=[0.55, 1.6], hspace=0.45, wspace=0.5)
tiles = [("Deliveries", f"{R['n']:,}"), ("On-time rate", f"{R['on_time_pct']}%"), ("Mean delay", f"{R['mean_delay']} min"), ("90th pct delay", f"{R['p90_delay']} min")]
for i,(t,v) in enumerate(tiles):
    a = fig.add_subplot(gs[0,i]); a.axis("off"); a.add_patch(FancyBboxPatch((0,0),1,1,boxstyle="round,pad=0.02",fc="white",ec="#CCD3DA",transform=a.transAxes))
    a.text(0.5,0.68,v,ha="center",va="center",fontsize=13,fontweight="bold",color=NAVY,transform=a.transAxes); a.text(0.5,0.22,t,ha="center",va="center",fontsize=8,color=GREY,transform=a.transAxes)
a1 = fig.add_subplot(gs[1,0:2]); a1.plot(kpi.month, kpi.on_time_pct, color=RED, marker="o", ms=3); a1.set_title("On-time % by month", fontsize=8.5, fontweight="bold"); a1.set_xticks(range(0,24,4)); a1.set_xticklabels(kpi.month[::4], fontsize=6.5, rotation=30); a1.tick_params(labelsize=7)
z = pd.DataFrame(R["zones"]).sort_values("mean_delay"); a2 = fig.add_subplot(gs[1,2:]); a2.barh(z.zone, z.mean_delay, color=BLUE); a2.set_title("Mean delay by supplier zone (min)", fontsize=8.5, fontweight="bold"); a2.tick_params(labelsize=7)
for a_ in (a1,a2): a_.spines["top"].set_visible(False); a_.spines["right"].set_visible(False); a_.set_facecolor("white")
fig.savefig("figs/fig_dashboard.png", facecolor="#F4F6F8", bbox_inches="tight"); plt.close(fig)

# sizes
from PIL import Image
import glob, os
json.dump({os.path.basename(p)[:-4]: Image.open(p).size for p in glob.glob("figs/*.png")}, open("sizes.json","w")); print(open("sizes.json").read())

# ================= ADDITIONAL FIGURES =================
from matplotlib.patches import Polygon
# ---- Fig 1.1 problem tree ----
fig = plt.figure(figsize=(6.4, 3.5), dpi=220); ax = fig.add_axes([0,0,1,1]); ax.set_xlim(0,100); ax.set_ylim(0,56); ax.axis("off")
ax.text(18,54,"CAUSES",ha="center",fontsize=9,fontweight="bold",color=GREY); ax.text(50,54,"CORE PROBLEM",ha="center",fontsize=9,fontweight="bold",color=RED); ax.text(83,54,"EFFECTS",ha="center",fontsize=9,fontweight="bold",color=GREY)
causes = ["Heavy rain and flooding","Road works, accidents, congestion","Data scattered: ERP, gate, dispatch, weather","Fixed standard lead times in planning","Manual, late reconciliation"]
for i,t in enumerate(causes):
    y = 43-i*9.6; hi = i in (2,3)
    ax.add_patch(FancyBboxPatch((1,y),34,7.2,boxstyle="round,pad=0.15,rounding_size=0.9",fc="#EAF4F0" if hi else "#EEEEEE",ec=TEAL if hi else GREY,lw=1.6 if hi else 0.8))
    ax.text(18,y+3.6,t,ha="center",va="center",fontsize=7.8,fontweight="bold" if hi else "normal")
    arr(ax,35.6,y+3.6,41.2,26)
ax.add_patch(FancyBboxPatch((41.5,17),17,18,boxstyle="round,pad=0.2,rounding_size=1.2",fc=RED,ec="black",lw=0.8))
ax.text(50,26,"Unpredictable\ninbound delivery\ntimes at Namanve\nplants",ha="center",va="center",color="white",fontsize=8.6,fontweight="bold")
effects = ["Idle production lines","High buffer stock and storage cost","Overtime at receiving bay","Weak supplier accountability","Higher logistics cost"]
for i,t in enumerate(effects):
    y = 43-i*9.6
    ax.add_patch(FancyBboxPatch((65,y),34,7.2,boxstyle="round,pad=0.15,rounding_size=0.9",fc="#F6E3E3",ec=RED,lw=0.8)); ax.text(82,y+3.6,t,ha="center",va="center",fontsize=7.8)
    arr(ax,58.8,26,64.4,y+3.6)
ax.add_patch(Rectangle((1,0.5),34,3.6,fc="#EAF4F0",ec=TEAL,lw=1.2)); ax.text(18,2.3,"Green = causes addressed by the data pipeline",ha="center",va="center",fontsize=7.3,color=TEAL,fontweight="bold")
fig.savefig("figs/fig_problem.png", facecolor="white"); plt.close(fig)

# ---- Fig 3.1 as-is vs to-be ----
fig = plt.figure(figsize=(6.4, 3.0), dpi=220); ax = fig.add_axes([0,0,1,1]); ax.set_xlim(0,100); ax.set_ylim(0,50); ax.axis("off")
asis = ["ERP plan with\nfixed lead time","Phone calls\nfrom transporters","Paper / Excel\ngate log","Manual weekly\nreconciliation","Delay found\nafter the fact"]
tobe = ["Five sources\nland daily","Automated\nBronze-Silver-Gold\nwith validation","Rain and\nroad-event\nenrichment","Dashboard and\ndelay-risk ETA","Planner\nreschedules early"]
ax.text(1,47.5,"AS-IS (manual)",fontsize=9,fontweight="bold",color=GREY); ax.text(1,23.5,"TO-BE (data pipeline)",fontsize=9,fontweight="bold",color=TEAL)
for i,(a,b) in enumerate(zip(asis,tobe)):
    x = 1+i*19.6
    ax.add_patch(FancyBboxPatch((x,30),16,12,boxstyle="round,pad=0.15,rounding_size=0.9",fc="#E6E6E6",ec=GREY,lw=0.9)); ax.text(x+8,36,a,ha="center",va="center",fontsize=7.2)
    ax.add_patch(FancyBboxPatch((x,6),16,12,boxstyle="round,pad=0.15,rounding_size=0.9",fc="#DDEFE9",ec=TEAL,lw=0.9)); ax.text(x+8,12,b,ha="center",va="center",fontsize=7.0)
    if i<4: arr(ax,x+16.6,36,x+19.3,36); arr(ax,x+16.6,12,x+19.3,12,c=TEAL)
fig.savefig("figs/fig_asis.png", facecolor="white"); plt.close(fig)

# ---- Fig 4.3 record flow + feature importance ----
fig, (a, b) = plt.subplots(1, 2, figsize=(6.4, 2.7), dpi=220, gridspec_kw={"width_ratios":[1.15,1]})
labels = ["Raw gate-log\nrows", "After removing\nduplicates", "After validation\n(Silver)"]
vals = [M["bronze_rows"]["gate"], M["bronze_rows"]["gate"]-M["rejects"]["gate_duplicates_removed"], M["silver_rows"]["deliveries"]]
bars = a.bar(labels, vals, color=["#8A5A2F", "#7F8FA3", "#C9A227"], ec="black", lw=0.5, width=0.6)
for r_, v in zip(bars, vals): a.text(r_.get_x()+r_.get_width()/2, v+25, f"{v:,}", ha="center", fontsize=8, fontweight="bold")
a.text(0.5, 4130, f"-{M['rejects']['gate_duplicates_removed']}\nduplicates", ha="center", fontsize=7.2, color=RED)
a.text(1.5, 4130, f"-{M['silver_rows']['quarantined']} quarantined\n({M['rejects']['gate_arrival_missing']} missing,\n{M['rejects']['arrival_before_dispatch']} impossible)", ha="center", fontsize=7.0, color=RED)
a.set_ylim(3000, 4450); a.set_ylabel("Records", fontsize=8.5); a.tick_params(labelsize=7); a.set_title("(a) Record flow through the pipeline", fontsize=8.5, fontweight="bold")
imp = R["top_features"]; nm = {"rain_mm_day":"Rain on dispatch day","accident_flag":"Accident logged","rain_3d_mm":"3-day rainfall","roadworks_flag":"Road works logged","dispatch_hour":"Dispatch hour","std_lead_h":"Standard lead time"}
items = list(imp.items())[::-1]
b.barh([nm[k] for k,_ in items], [v for _,v in items], color=TEAL, ec="black", lw=0.5)
b.set_xlabel("Increase in MAE when shuffled (min)", fontsize=7.8); b.tick_params(labelsize=7.3); b.set_title("(b) Feature importance, ETA model", fontsize=8.5, fontweight="bold")
for ax_ in (a, b): ax_.spines["top"].set_visible(False); ax_.spines["right"].set_visible(False)
fig.tight_layout(); fig.savefig("figs/fig_funnel.png", facecolor="white"); plt.close(fig)

# ---- Fig C1 silver validation flowchart ----
fig = plt.figure(figsize=(6.4, 5.4), dpi=220); ax = fig.add_axes([0,0,1,1]); ax.set_xlim(0,100); ax.set_ylim(0,100); ax.axis("off")
def rb(x,y,w,h,t,fc="#DCE6F0",ec=NAVY,fs=8): ax.add_patch(FancyBboxPatch((x,y),w,h,boxstyle="round,pad=0.15,rounding_size=0.8",fc=fc,ec=ec,lw=0.9)); ax.text(x+w/2,y+h/2,t,ha="center",va="center",fontsize=fs)
def dia(cx,cy,w,h,t): ax.add_patch(Polygon([(cx,cy+h/2),(cx+w/2,cy),(cx,cy-h/2),(cx-w/2,cy)],fc="#FBEFD0",ec=GOLD,lw=1)); ax.text(cx,cy,t,ha="center",va="center",fontsize=7.8)
steps = ["1. Raw ERP, gate log and dispatch feed (Bronze)","2. Remove duplicate PO records","3. Parse timestamps (ISO or dd/mm/yyyy)","4. Standardise supplier codes","5. Join sources; impute missing dispatch from plan"]
ys = [92, 80.5, 69, 57.5, 46]
for k,(t,y) in enumerate(zip(steps,ys)):
    rb(10,y,48,7,t)
    if k < 4: arr(ax,34,y,34,ys[k+1]+7)
arr(ax,34,46,34,40.2)
dia(34,34,40,12,"Gate arrival present?")
arr(ax,54,34,64,34); ax.text(57,36.2,"No",fontsize=8,color=RED,fontweight="bold")
rb(64,30.5,35,7,"Quarantine: GATE_ARRIVAL_MISSING",fc="#F6E3E3",ec=RED,fs=6.8)
arr(ax,34,28,34,25.6); ax.text(35.4,26.4,"Yes",fontsize=8,color=TEAL,fontweight="bold")
dia(34,19.5,40,12,"Arrival at or after dispatch?")
arr(ax,54,19.5,64,19.5); ax.text(57,21.7,"No",fontsize=8,color=RED,fontweight="bold")
rb(64,16,35,7,"Quarantine: ARRIVAL_BEFORE_DISPATCH",fc="#F6E3E3",ec=RED,fs=6.6)
arr(ax,34,13.5,34,9.2); ax.text(35.4,11,"Yes",fontsize=8,color=TEAL,fontweight="bold")
rb(10,2,48,7,"Silver deliveries (valid, unique, conformed)",fc="#E6D9A8",ec=GOLD,fs=8)
fig.savefig("figs/fig_flow.png", facecolor="white"); plt.close(fig)
from PIL import Image
import glob, os
json.dump({os.path.basename(p)[:-4]: Image.open(p).size for p in glob.glob("figs/*.png")}, open("sizes.json","w"))
print("added figures")
