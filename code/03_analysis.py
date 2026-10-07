"""Analytics on the Gold layer: KPIs, rainfall-delay association (H2), ETA model vs baseline (H1)."""
import pandas as pd, numpy as np, json
from scipy import stats
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
f = pd.read_parquet("lake/gold/delivery_fact.parquet")
f["dispatch_date"] = f.dispatch_ts.dt.normalize()
R = {}
R["n"] = len(f); R["on_time_pct"] = round(100*f.on_time.mean(),1); R["mean_delay"] = round(f.delay_min.mean(),1)
R["median_delay"] = round(f.delay_min.median(),1); R["p90_delay"] = round(f.delay_min.quantile(.9),1)
# rainfall bands
bands = pd.cut(f.rain_mm_day, [-0.1,0.0,10,25,1000], labels=["No rain","Light (0-10 mm)","Moderate (10-25 mm)","Heavy (>25 mm)"])
bt = f.groupby(bands, observed=True).agg(n=("delay_min","size"), mean_delay=("delay_min","mean"), on_time_pct=("on_time", lambda s: 100*s.mean())).round(1)
R["bands"] = bt.reset_index().rename(columns={"rain_mm_day":"band"}).to_dict("records")
# H2: Spearman rain vs delay
rho, p = stats.spearmanr(f.rain_mm_day, f.delay_min); R["spearman_rain_delay"] = [round(rho,3), float(p)]
rho3, p3 = stats.spearmanr(f.rain_3d_mm, f.delay_min); R["spearman_rain3d_delay"] = [round(rho3,3), float(p3)]
# Kruskal across bands
groups = [g.delay_min.values for _, g in f.groupby(bands, observed=True)]
kw = stats.kruskal(*groups); R["kruskal"] = [round(float(kw.statistic),1), float(kw.pvalue)]
# flood effect
fl = f[f.flooding_flag==1].delay_min; nf = f[f.flooding_flag==0].delay_min
R["flood_days_deliveries"] = int(len(fl)); R["flood_mean_delay"] = round(fl.mean(),1); R["noflood_mean_delay"] = round(nf.mean(),1)
mw = stats.mannwhitneyu(fl, nf); R["flood_mw_p"] = float(mw.pvalue)
# zone summary
zs = f.groupby("zone").agg(n=("delay_min","size"), mean_delay=("delay_min","mean"), on_time_pct=("on_time", lambda s: 100*s.mean())).round(1).sort_values("mean_delay")
R["zones"] = zs.reset_index().to_dict("records")
# season (wet vs dry)
wet = f.month.isin([3,4,5,9,10,11])
R["wet_on_time"] = round(100*f[wet].on_time.mean(),1); R["dry_on_time"] = round(100*f[~wet].on_time.mean(),1)
R["wet_delay"] = round(f[wet].delay_min.mean(),1); R["dry_delay"] = round(f[~wet].delay_min.mean(),1)
# ---------- H1: ETA model vs baseline (temporal split) ----------
cut = pd.Timestamp("2025-07-01")
tr, te = f[f.dispatch_ts < cut].copy(), f[f.dispatch_ts >= cut].copy()
R["train_n"], R["test_n"] = len(tr), len(te)
# Baseline = planner's current practice: ERP planned time + supplier's historical average delay (train only)
sup_mean = tr.groupby("supplier_id").delay_min.mean()
te["pred_base"] = te.supplier_id.map(sup_mean).fillna(tr.delay_min.mean())
Z = pd.get_dummies(f.zone, prefix="z")
feat = ["rain_mm_day","rain_3d_mm","flooding_flag","roadworks_flag","accident_flag","dispatch_hour","dow","month","std_lead_h","load_tonnes"]
X = pd.concat([f[feat], Z], axis=1).astype(float)
Xtr, Xte = X.loc[tr.index], X.loc[te.index]
m = HistGradientBoostingRegressor(max_iter=250, learning_rate=0.06, max_depth=4, random_state=7).fit(Xtr, tr.delay_min)
te["pred_model"] = m.predict(Xte)
def met(y, yh): return dict(MAE=round(mean_absolute_error(y,yh),2), RMSE=round(float(np.sqrt(mean_squared_error(y,yh))),2), R2=round(r2_score(y,yh),3))
R["metrics_base"] = met(te.delay_min, te.pred_base); R["metrics_model"] = met(te.delay_min, te.pred_model)
R["metrics_planned_only"] = met(te.delay_min, np.zeros(len(te)))
eb, em = (te.delay_min-te.pred_base).abs(), (te.delay_min-te.pred_model).abs()
w = stats.wilcoxon(eb, em); R["wilcoxon_p"] = float(w.pvalue); R["wilcoxon_stat"] = float(w.statistic)
R["mae_reduction_pct"] = round(100*(eb.mean()-em.mean())/eb.mean(),1)
tt = stats.ttest_rel(eb, em); R["paired_t"] = [round(float(tt.statistic),2), float(tt.pvalue)]
# share of test deliveries ETA within +/-30 min
R["within30_base"] = round(100*(eb<=30).mean(),1); R["within30_model"] = round(100*(em<=30).mean(),1)
# permutation importance (quick)
from sklearn.inspection import permutation_importance
pi = permutation_importance(m, Xte, te.delay_min, n_repeats=5, random_state=1, scoring="neg_mean_absolute_error")
imp = pd.Series(pi.importances_mean, index=X.columns).sort_values(ascending=False).head(6).round(2)
R["top_features"] = imp.to_dict()
te.to_parquet("lake/gold/eta_test_predictions.parquet", index=False)
json.dump(R, open("results.json","w"), indent=1, default=float)
print(json.dumps(R, indent=1, default=float))
