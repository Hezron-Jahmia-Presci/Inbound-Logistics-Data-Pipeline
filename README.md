Namanve inbound-logistics data pipeline: project files (supporting material, NOT for Vclass submission)
Run order (Python 3, pandas, numpy, scipy, scikit-learn, duckdb, pyarrow, matplotlib):
  python code/01_generate_data.py   -> raw/*.csv, raw/transporter_dispatch.json (SIMULATED data, seed 2026)
  python code/02_pipeline.py        -> lake/bronze, lake/silver, lake/gold + metrics.json
  python code/03_analysis.py        -> results.json (hypothesis tests, ETA model)
  python code/04_figures.py         -> figs/*.png
All data are synthetic; effects are planted so results validate the pipeline, not real-world effect sizes.


# Designing a Batch Data Pipeline for Inbound Delivery Reliability at Namanve Industrial and Business Park

## ACRONYMS

| Acronym | Meaning |
|---|---|
| API | Application Programming Interface |
| BI | Business Intelligence |
| CSV | Comma-Separated Values |
| DAG | Directed Acyclic Graph |
| DV | Dependent Variable |
| ERP | Enterprise Resource Planning |
| ETA | Estimated Time of Arrival |
| IoT | Internet of Things |
| IV | Independent Variable |
| JSON | JavaScript Object Notation |
| KPI | Key Performance Indicator |
| MAE | Mean Absolute Error |
| MoFPED | Ministry of Finance, Planning and Economic Development |
| PO | Purchase Order |
| RMSE | Root Mean Square Error |
| SQL | Structured Query Language |
| UIA | Uganda Investment Authority |
| UNMA | Uganda National Meteorological Authority |

## ABSTRACT

Manufacturers at Namanve Industrial and Business Park depend on timely inbound deliveries, yet ERP plans, gate logs, transporter updates and weather or road conditions sit in separate places, so planners cannot anticipate a wet day or a flooded road. This capstone designs, implements and evaluates a batch data pipeline that integrates five sources into bronze, silver and gold layers using Python, DuckDB and Parquet, with an Apache Airflow schedule designed for production.

Using a design science approach and a simulated dataset of 3,739 deliveries for an anonymised plant, the pipeline reached 100% on four data-quality dimensions in under a second and quarantined 110 invalid records. Rainfall was significantly associated with delay (Spearman ρ = 0.361), and a weather-aware ETA model cut mean absolute error by 22.2% against the supplier-average baseline. Because the effects were planted in the simulation, the results validate the pipeline, not real-world effect sizes. A 12-week pilot at a real plant is recommended.

**Keywords:** data pipeline; inbound logistics; delivery reliability; Namanve; data engineering; Uganda.

---

## CHAPTER ONE: INTRODUCTION

### 1.0 Introduction

This chapter introduces the study. It presents the background (historical, theoretical, conceptual and contextual perspectives), the problem statement, the main and specific objectives together with the research questions and hypotheses, the scope, and the significance of the study.

### 1.1 Background

The background is organised into historical, theoretical, conceptual and contextual perspectives.

#### 1.1.1 Historical Perspective

Manufacturers have long protected production from late deliveries by holding safety stock, a costly buffer against uncertainty. Research later turned to information: Barratt and Oke (2007) showed that supply chain visibility rests on the information-sharing resources a firm builds, not only on its physical assets. Weather is a recognised source of transport uncertainty, and Koetse and Rietveld (2009) reviewed empirical findings showing that it affects transport performance. In Uganda, earlier work examined delivery cycle time among food processors in Kampala (Lulagala, 2016) and found that road quality was significantly related to supply chain performance at a sugar factory (r = .601, p < .05) (Nkumba University, 2025). These studies report associations. The gap addressed here is that, to the author's knowledge, none builds an engineered data pipeline that joins a plant's own ERP and gate records with rainfall and road-event data to measure and predict delivery delay.

#### 1.1.2 Theoretical Perspective

Two theories guide the study. Information Processing Theory (Galbraith, 1973; Tushman & Nadler, 1978) holds that organisations facing uncertainty must raise their capacity to process information. The DeLone and McLean (2003) information systems success model links the quality of a system and its information to net benefits for the organisation. Both are developed in Section 2.1.

#### 1.1.3 Conceptual Perspective

The independent variable (IV) is an integrated logistics data pipeline, conceptualised through data integration, data quality, timeliness, and weather and road-event enrichment. The dependent variable (DV) is inbound delivery reliability, conceptualised through on-time delivery rate, mean delay and ETA (estimated time of arrival) accuracy (Figure 2.1).

#### 1.1.4 Contextual Perspective

Namanve, the Kampala Industrial and Business Park (KIBP), is Uganda's largest industrial park and lies along the Kampala–Jinja highway (The Observer, n.d.). It now hosts about 480 factories, nearly 280 of them operating, and its infrastructure project is 80% complete, with substantial completion expected by the end of 2026 (Pulse Uganda, 2026); the Ministry of Finance has extended the completion date to 30 December 2026 and asked that the Standard Gauge Railway, Bukasa port and Kampala–Jinja expressway works be sequenced with it (Ministry of Finance, Planning and Economic Development [MoFPED], n.d.). Roads and drainage have improved, but for years heavy rain turned park roads muddy, trucks struggled to reach factories and flooding cut off parts of the park (Daily Monitor, n.d.-c), and drivers said the earlier roads made deliveries difficult to predict (Pulse Uganda, 2026). The UIA lists reliable power, industrial water, waste management and access roads among the park's challenges (Uganda Investment Authority [UIA], n.d.). Nationally, a Private Sector Foundation Uganda study found manufacturers operating at 54.4% of capacity (Pulse Uganda, n.d.), transport and vehicle expenses account for nearly 17.5% of manufacturing costs, and digital adoption in logistics is relatively low (Daily Monitor, n.d.-b). Uganda has two rainy seasons, March–May and September–December (Daily Monitor, n.d.-a), and Kampala floods often paralyse transport (Daily Monitor, n.d.-d), so the approach routes to Namanve stay exposed to weather even as the park's own roads improve.

### 1.2 Problem Statement

Reliable inbound delivery lets a manufacturing plant run production lines continuously and schedule receiving labour efficiently; this is the ideal situation. In practice, plants at Namanve face unpredictable deliveries because of rain, flooding, congestion and road works on the routes that feed the park (Daily Monitor, n.d.-c, n.d.-d; Pulse Uganda, 2026). Planned arrival times sit in the ERP, actual arrivals are recorded in separate gate logs, dispatch updates come from transporters, and weather and road conditions are linked to none of them. Planners therefore rely on a fixed lead time per supplier that cannot anticipate a wet day or a flooded road. The consequences are idle production lines, costly buffer stock, overtime for receiving crews and weak supplier accountability, adding to the logistics cost burden manufacturers already report (Daily Monitor, n.d.-b). One way to alleviate the problem is to isolate its causes and make them visible to planners. Among several causes, weather and road events are measurable and external, so a pipeline that integrates them with plant records (IV) should improve delivery reliability (DV). To the author's knowledge no earlier Ugandan study has engineered and evaluated such a pipeline, which justifies this project. Figure 1.1 summarises the causes and effects.

**Figure 1.1**
*Problem Tree: Causes and Effects of Unpredictable Inbound Deliveries*

![Figure 1.1 – Problem Tree](figure-1-1.png)

*Source: constructed by the author from the evidence in Section 1.1.4.*

### 1.3 Main Objective

To design, implement and evaluate a data pipeline that integrates ERP, gate, transporter, rainfall and road-event data to improve inbound delivery visibility and reliability for a manufacturing plant at Namanve Industrial and Business Park.

### 1.4 Specific Objectives

i. To study and analyse the current inbound-logistics data flows of the plant and identify the requirements of an integrated pipeline.
ii. To design and implement a bronze–silver–gold batch pipeline that cleans, conforms and enriches the data.
iii. To test and validate the pipeline for data quality and analytical value, including the accuracy of weather-aware arrival-time predictions.

**Research questions.**
- **RQ1:** What data sources and data-quality problems characterise inbound logistics at the plant?
- **RQ2:** How can a pipeline integrate them into one reliable dataset?
- **RQ3:** To what extent are rainfall and road events associated with delivery delay?
- **RQ4:** Does a weather-aware ETA built on the pipeline predict arrival more accurately than current practice?

**Hypotheses.**
- **H1:** A weather- and road-event-aware ETA model built on the pipeline has a significantly lower mean absolute error than the baseline supplier-average lead time.
- **H2:** Rainfall on the dispatch day is significantly and positively associated with delivery delay.

### 1.5 Scope

**Content scope:** inbound road deliveries of raw materials and spare parts; daily batch ingestion of five sources; analysis of delay and ETA accuracy. Outbound distribution, customs clearance and real-time GPS streaming are excluded.

**Geographical scope:** six supplier zones (Kampala Industrial Area, Mukono, Entebbe, Jinja, Mbale and Mbarara) delivering to one anonymised plant, "Plant X", at Namanve.

**Sample scope:** 24 months (1 January 2024 to 31 December 2025) and 3,739 purchase-order deliveries.

### 1.6 Significance

Academically, the study links data engineering with logistics theory in a Ugandan setting and offers reproducible code and a synthetic dataset for teaching. Practically, plant planners gain weather-aware arrival estimates and one trusted version of delivery data, and suppliers and transporters gain objective on-time measures. For policy, it shows the value of shared road and flood data in industrial parks, in line with Uganda's digital transformation roadmap (UNDP Uganda, 2022).

---

## CHAPTER TWO: LITERATURE REVIEW

### 2.0 Introduction

This chapter contains the theoretical review, the conceptual framework and the review of related literature, in that order.

### 2.1 Theoretical Review

**Information Processing Theory.** Galbraith (1973) proposed that the more uncertain an organisation's tasks, the more information it must process during execution; Tushman and Nadler (1978) extended this into the principle that performance is best when an organisation's information-processing capacity matches its information-processing requirements. *Link to the study:* rain, flooding and road works create uncertainty (a processing requirement), while data scattered across ERP, gate and transporter systems gives the plant little capacity to process it. The pipeline raises that capacity, so the IV is expected to improve the DV.

**DeLone and McLean information systems success model.** DeLone and McLean (2003) argue that system quality, information quality and service quality shape use and user satisfaction, which produce net benefits. *Link to the study:* the data-quality metrics in Chapter Four measure information quality, and improved delivery reliability is the net benefit.

### 2.2 Conceptual Framework

Figure 2.1 shows how the IV, the integrated logistics data pipeline, is linked to the DV, inbound delivery reliability. Each IV component has a measurable indicator: integration (share of purchase orders joined across sources), quality (completeness, validity, uniqueness, consistency), timeliness (daily refresh) and enrichment (rainfall and event features attached to each delivery). The DV is measured by on-time rate (arrival within 30 minutes of plan), mean delay in minutes, and ETA mean absolute error (MAE).

**Figure 2.1**
*Conceptual Framework: Data Pipeline and Delivery Reliability*

![Figure 2.1 – Conceptual Framework](figure-2-1.png)

*Source: constructed by the author from Galbraith (1973) and DeLone and McLean (2003).*

### 2.3 Review of Related Literature

Weather affects transport performance in ways that can be measured (Koetse & Rietveld, 2009), and firms with stronger information-sharing resources achieve better supply chain visibility (Barratt & Oke, 2007). In Uganda, road quality is significantly related to supply chain performance (Nkumba University, 2025), and logistics inefficiency is costly: a World Bank report cited in the press puts it at Shs 3 trillion annually (Daily Monitor, n.d.-b). On the engineering side, the lakehouse approach stores raw, curated and trusted data in layers on open formats (Armbrust et al., 2021). For Ugandan plants, IoT-based monitoring faces high sensor cost, legacy-protocol integration and security concerns (Bazigu & Mwebaze, 2025). This project therefore chooses a low-cost, batch pipeline built on records plants already hold. The literature leaves one gap: no engineered, evaluated pipeline joining plant logistics records with weather and road events at a Namanve plant.

---

## CHAPTER THREE: METHODOLOGY

### 3.0 Introduction

This chapter covers the study design, population, sample, data collection methods and instruments, data quality control, collection procedure, analysis, system analysis and design, implementation, testing, ethical considerations, and the project plan.

### 3.1 Study Design

The study is mainly quantitative and follows design science research (Hevner et al., 2004): an artefact, the pipeline, is built and then evaluated against measurable criteria. It is a single-case study of one plant, with a quasi-experimental temporal hold-out comparison, since the model is trained on earlier deliveries and compared with the baseline on later, unseen deliveries. This design fits because the aim is to build and evaluate a working system rather than survey opinions.

### 3.2 Population

The target population is all inbound road deliveries of raw materials and spare parts to Plant X at Namanve from 12 suppliers in six zones between January 2024 and December 2025, covering five material categories. Namanve was chosen because its logistics problems are documented (Section 1.1.4).

### 3.3 Sample Size and Sample Size Determination

No sampling was needed: a pipeline ingests complete records, so all 3,739 purchase orders were processed (a census). After cleaning, 3,629 deliveries remained, split by date into a training set (January 2024–June 2025, n = 2,746) and a test set (July–December 2025, n = 883). A temporal split keeps representativeness across both rainy seasons and prevents future information leaking into training.

### 3.4 Data Collection Methods

The study uses secondary data. Plant records are confidential, so for this class project the data were simulated under documented assumptions, as the coursework permits; they are not real Plant X records. Table 3.1 lists the sources. In a real deployment each source maps to an existing system, and rainfall would come from the Uganda National Meteorological Authority or an open weather service, to be confirmed with the provider.

**Table 3.1**
*Data Sources Ingested by the Pipeline*

| Source | Format | Key fields | Raw records | Refresh |
|---|---|---|---|---|
| ERP purchase orders | CSV | po_number, supplier_id, material, load_tonnes, planned_arrival_ts | 3,739 | Daily |
| Gate / weighbridge log | CSV | po_number, truck_id, gate_arrival_ts, offload_complete_ts | 3,851 | Daily |
| Transporter dispatch feed | JSON | po, dispatched_at, origin, truck | 3,683 | Daily |
| Rainfall (daily) | CSV / API | date, rainfall_mm, station | 731 | Daily |
| Road-event log | CSV (manual) | date, event_type, severity | 41 | As logged |
| Supplier master | CSV | supplier_id, zone, material | 12 | Static |

*Source: simulated data generated by the author (code in Appendix C; assumptions in Appendix A).*

**Simulation assumptions.** Planned arrival equals dispatch time plus a standard lead time of 1.0 to 6.5 hours by zone. Delay combines random noise, a rainfall effect that differs by zone, a smaller effect from the two preceding days of rain, penalties for flooding, road works and accidents, and peak-hour congestion. Rainfall has two wet seasons and totals about 1,250 mm a year. Defects were deliberately injected (duplicates, missing and badly formatted timestamps, impossible times, case errors) so the cleaning stage has real work to do. Because the effects are planted, the tests in Chapter Four show that the pipeline preserves and exposes known signal; they do not estimate real-world effect sizes.

### 3.5 Data Collection Instruments

The instruments are (a) extraction scripts that read the CSV exports and JSON feed, (b) the data dictionary in Appendix B, which defines every field of the Gold table, and (c) for a real pilot, a short structured checklist for logistics staff to confirm that cleaned records match what they observed. The checklist is planned and has not been administered.

### 3.6 Data Quality Control

Validity of the pipeline is controlled through four data-quality dimensions measured before and after cleaning: completeness, format validity, uniqueness and supplier consistency, plus rules such as "arrival cannot precede dispatch". Reliability is controlled through reconciliation (every purchase order ends in Silver or in quarantine), an idempotency test (re-running gives identical output), a fixed random seed (2026), and a temporal train–test split.

### 3.7 Data Collection Procedure

In a real pilot, written permission would be sought from the plant's supply chain manager and management, with a data-sharing agreement covering transporter data; extracts would be anonymised before use. For this project, the generator script was run once with its fixed seed and the files were placed in the raw landing folder.

### 3.8 Data Analysis

Data were prepared by parsing mixed timestamp formats, removing duplicates, standardising supplier codes, imputing missing rainfall with the monthly median, and quarantining records that fail validation. Analysis was automated in Python. Descriptive statistics (delivery counts, on-time rate, mean and 90th-percentile delay) are reported at the univariate level. Bivariate analysis uses Spearman correlation and the Kruskal–Wallis and Mann–Whitney tests, chosen because delays are right-skewed. For H1, a gradient-boosting regressor predicts delay from rainfall, event flags, dispatch hour, weekday, month, zone and load; it is compared with the baseline using MAE, RMSE, R² and paired Wilcoxon and t tests. Significance is set at α = 0.05.

### 3.9 System Analysis and Design

**Current system.** Planners rely on the ERP's planned arrival time; the gate keeps a paper or spreadsheet log; transporters report by phone; no weather or road data are linked; and reconciliation is manual (an assumption for Plant X).

**New system.** Figure 3.1 contrasts the as-is and to-be processes. Process modelling uses the pipeline flow diagram in Figure 3.2, which shows the sequence of data movement from five sources through Bronze, Silver and Gold to the dashboard and ETA model; the Silver validation logic is in Appendix C (Figure C1). Data modelling uses the star schema in Figure 3.3, with one delivery fact table linked to supplier, rainfall, road-event and quarantine tables.

**Figure 3.1**
*As-Is versus To-Be Inbound Logistics Process*

![Figure 3.1 – As-Is vs To-Be](figure-3-1.png)

*Source: constructed by the author; the as-is process is an assumption for Plant X.*

**Figure 3.2**
*Pipeline Architecture (Process Model)*

![Figure 3.2 – Pipeline Architecture](figure-3-2.png)

*Source: designed by the author, applying the layered lakehouse approach of Armbrust et al. (2021).*

**Figure 3.3**
*Data Model for the Gold Layer (Star Schema)*

![Figure 3.3 – Star Schema](figure-3-3.png)

*Source: designed by the author.*

### 3.10 System Implementation

Table 3.2 shows how each tool helps implement the design.

**Table 3.2**
*Implementation Tools and Their Contribution*

| Tool | How it helps in this project |
|---|---|
| Python 3 and pandas (McKinney, 2010) | Parses mixed date formats, removes duplicates and applies validation rules in a few readable lines. |
| DuckDB (Raasveldt & Mühleisen, 2019) | Runs SQL window functions, such as the three-day rainfall total, directly on Parquet files with no database server, which suits a plant without a data team. |
| Parquet files | Compact columnar storage for the Bronze, Silver and Gold layers, so reruns are fast and layers are inspectable. |
| scikit-learn (Pedregosa et al., 2011) and SciPy | Provide the gradient-boosting ETA model, error metrics and non-parametric tests suited to skewed delays. |
| Apache Airflow (Apache Software Foundation, n.d.) | Designed to schedule the daily Bronze → Silver → Gold run with retries and logs (DAG in Appendix C; not executed in this project). |
| Excel / Power BI | Planner-facing dashboard over the Gold tables (mock-up in Appendix E). |
| ClickUp, Microsoft Project, Zotero | Task board, schedule and reference management (Section 3.13, Appendix D). |

*Note: the pipeline, model and statistics were executed in Python; Airflow and the BI dashboard are design deliverables.*

### 3.11 Testing and Validation

Testing used four techniques: (a) rule-level unit checks on cleaning logic, (b) integration tests across stages, namely reconciliation of counts, a unique key in the fact table, and no negative travel times, (c) an idempotency test comparing a hash of the Gold table across two runs, and (d) ground-truth recovery, checking that the pipeline exposes the effects planted in the simulation. User acceptance testing with planners is planned for the pilot.

### 3.12 Ethical Consideration

No real company or personal data were used. The plant is anonymised as "Plant X", suppliers carry codes, and truck identifiers are fictitious. In a pilot, driver-related fields would be excluded or pseudonymised in line with the Data Protection and Privacy Act, 2019 (Republic of Uganda, 2019), and the plant's permission would be obtained first. The report states plainly that results come from simulated data, and all sources are cited.

### 3.13 Project Plan, Risks and Management Tools

A 12-week pilot is planned in four phases with stage gates at weeks 2, 7 and 12 (Figure D1, Appendix D). Microsoft Project holds the schedule, a ClickUp board tracks tasks, and Zotero manages references. The main risks are denied data access, poor source data, scope creep, skills gaps and weather-data availability; each has a mitigation in Table D1.

---

## CHAPTER FOUR: DATA ANALYSIS, PRESENTATION AND INTERPRETATION

### 4.0 Introduction

This chapter reports pipeline execution and data quality (RQ1, RQ2; Figure 4.1), delivery performance, the rainfall–delay relationship (H2, RQ3) and the ETA model comparison (H1, RQ4). All results derive from the simulated dataset.

### 4.1 Pipeline Execution and Data Quality

The pipeline ran end to end in 0.209 seconds on a laptop-class sandbox (Table 4.1). Of 3,739 purchase orders, 3,629 (97.1%) passed all rules and 110 were quarantined: 74 with no gate arrival and 36 with an arrival before dispatch. The cleaning stage also removed 112 duplicate gate records, reformatted 54 dates, imputed 56 missing dispatch times and 18 rainfall days. All integration tests passed, including reconciliation and the idempotent rerun.

**Table 4.1**
*Pipeline Execution Summary*

| Stage | Output | Key events | Run time (s) |
|---|---|---|---|
| Bronze | 12,057 raw rows in 6 tables | As-received copy with source and load-time columns | 0.085 |
| Silver | 3,629 clean deliveries; 110 quarantined | 112 duplicates removed; 54 dates reformatted | 0.070 |
| Gold | 3,629 fact rows; 24 monthly KPI rows | Rainfall and event features joined by SQL | 0.039 |

*Source: pipeline run metrics (metrics.json), October 2026.*

**Table 4.2**
*Data Quality Before (Raw) and After (Silver) Cleaning*

| Dimension | Before | After | Change (points) |
|---|---|---|---|
| Completeness (gate arrival present) | 98.0% | 100.0% | +2.0 |
| Format validity (timestamp parseable as standard) | 98.5% | 100.0% | +1.5 |
| Uniqueness (one record per PO) | 97.1% | 100.0% | +2.9 |
| Supplier consistency (valid master code) | 97.9% | 100.0% | +2.1 |

*Note: improvement is achieved by cleaning and quarantine, not by correcting the source systems, which would still need fixing at the gate.*

**Interpretation (RQ1, RQ2).** Defect rates of only 1–3% per issue led to about 3% of purchase orders being quarantined and a further 112 duplicate gate records being removed, which shows why integration without validation would mislead planners. By DeLone and McLean's (2003) logic, this raised information quality.

**Figure 4.1**
*Record Flow Through the Pipeline and ETA Model Feature Importance*

![Figure 4.1 – Record Flow and Feature Importance](figure-4-1.png)

*Source: pipeline run metrics and permutation importance on the test set (simulated data).*

### 4.2 Delivery Performance

Across 3,629 deliveries, 56.2% arrived within 30 minutes of plan; the mean delay was 31.8 minutes, the median 27 and the 90th percentile 63. The on-time rate was 64.4% in dry months against 47.8% in wet months (March–May and September–November in the simulation), and mean delay was 25.3 against 38.3 minutes. Figure 4.2 shows on-time performance falling in the rainy months. Mean delay differed little by supplier zone (about 29 to 34 minutes) because planned times already allow for distance; weather explains most of the variation within zones.

**Figure 4.2**
*Monthly Rainfall and On-Time Deliveries, 2024–2025*

![Figure 4.2 – Monthly Rainfall and On-Time Deliveries](figure-4-2.png)

*Source: Gold-layer monthly KPI table (simulated data).*

### 4.3 Rainfall, Road Events and Delay (H2)

Delay rises steeply with rainfall (Table 4.3, Figure 4.3a). Rainfall on the dispatch day correlated positively with delay (Spearman ρ = 0.361, p < .001), and delay differed significantly across rainfall bands (Kruskal–Wallis H = 600.6, p < .001). On days with a flooding event, mean delay was 211.8 minutes against 29.9 on other days (Mann–Whitney p < .001; 38 flood-day deliveries). H2 is supported. The pipeline therefore surfaced the planted rainfall effect, which validates its joins and enrichment.

**Table 4.3**
*Delay by Rainfall on Dispatch Day*

| Rainfall band | Deliveries | Mean delay (min) | On-time rate |
|---|---|---|---|
| No rain | 2,361 | 25.2 | 66.4% |
| Light (0-10 mm) | 799 | 31.2 | 50.9% |
| Moderate (10-25 mm) | 374 | 50.7 | 16.8% |
| Heavy (>25 mm) | 95 | 125.3 | 1.1% |

*Source: Gold-layer delivery fact table (simulated data).*

### 4.4 ETA Prediction: Pipeline Model versus Baseline (H1)

The baseline adds each supplier's average historical delay to the ERP plan, the "standard lead time" habit described in Section 1.2. On the 883 unseen test deliveries (Table 4.4, Figure 4.3b), the pipeline model cut MAE from 18.92 to 14.71 minutes, a 22.2% reduction, and explained 42% of delay variance against about zero for the baseline. The paired differences were significant (Wilcoxon p < .001; paired t = 9.02, p < .001), and the share of ETAs within ±30 minutes rose from 82.9% to 89.8%. The most influential features were same-day rainfall, accident flag and three-day rainfall. H1 is supported.

**Table 4.4**
*ETA Accuracy on the Test Set (July–December 2025)*

| Measure | Baseline | Pipeline model | Planned time only |
|---|---|---|---|
| MAE (minutes) | 18.92 | 14.71 | 32.13 |
| RMSE (minutes) | 24.91 | 18.79 | 39.74 |
| R² | -0.013 | 0.424 | -1.579 |
| ETAs within ±30 min | 82.9% | 89.8% | n/a |

*Source: analysis of the Gold layer; train n = 2,746, test n = 883.*

**Figure 4.3**
*Delay by Rainfall Band and ETA Error of Baseline versus Pipeline Model*

![Figure 4.3 – Delay by Rainfall Band and ETA Error](figure-4-3.png)

*Source: Gold-layer analysis (simulated data).*

### 4.5 Summary of Findings and Interpretation

| Test | Result | Decision |
|---|---|---|
| H1 | MAE 18.92 → 14.71 min (−22.2%), p < .001 | Supported: the weather-aware ETA is more accurate |
| H2 | Spearman ρ = 0.361, p < .001; flood days 211.8 vs 29.9 min | Supported: rainfall is associated with delay |

**Caution.** Both effects were planted in the simulation, so these results demonstrate that the pipeline captures and uses the signal correctly. They are not evidence about Plant X or Namanve in general; that requires the pilot proposed in Chapter Five.

---

## CHAPTER FIVE: DISCUSSION, CONCLUSIONS AND RECOMMENDATIONS

### 5.1 Discussion

The findings are consistent with the theory and literature. Under Information Processing Theory, adding weather and road-event data increased the plant's information-processing capacity and cut ETA error by about a fifth, mirroring the reported link between weather and transport performance (Koetse & Rietveld, 2009) and between road conditions and supply chain performance in Uganda (Nkumba University, 2025). The quality gains in Table 4.2 support the DeLone and McLean (2003) view that information quality precedes benefit. The batch design suits the context: it uses records plants already hold and avoids the sensor costs and legacy-integration barriers reported for Ugandan IoT projects (Bazigu & Mwebaze, 2025). The principal limitation is that the data are simulated, so effect sizes are assumptions; the sample is one anonymised plant; and the dashboard and Airflow scheduler were designed, not deployed.

### 5.2 Conclusions

- **Objective 1:** inbound-logistics data sit in five separate sources with typical defects (duplicates, missing and malformed timestamps, inconsistent codes) that a pipeline must handle.
- **Objective 2:** a bronze–silver–gold batch pipeline was designed and implemented, integrating 3,739 purchase orders into 3,629 analysis-ready records in under a second.
- **Objective 3:** the pipeline reached 100% on all four quality dimensions in Silver, passed reconciliation and idempotency tests, and supported a weather-aware ETA model that was 22.2% more accurate than the baseline.

The main objective is therefore met in a simulated setting.

### 5.3 Recommendations

- **Pilot at a real plant.** Run the 12-week plan with written permission and real extracts, and compare real on-time rate and ETA error before and after.
- **Fix quality at the source.** Add mandatory timestamp and PO fields at the gate to avoid the 3% quarantine rate.
- **Share weather and flood alerts.** Send planners and transporters next-day rain alerts so heavy-rain deliveries are rescheduled.
- **Grow gradually.** Add GPS and streaming ingestion (for example Apache Kafka) only after the batch pipeline proves value.
- **Park-level data.** The UIA could publish road-closure and flood events as an open feed for all Namanve factories.

Future research should replace simulated data with real extracts, test other models and measure cost savings.

---

## REFERENCES

Apache Software Foundation. (n.d.). *Apache Airflow documentation*. https://airflow.apache.org/docs/

Armbrust, M., Ghodsi, A., Xin, R., & Zaharia, M. (2021). Lakehouse: A new generation of open platforms that unify data warehousing and advanced analytics. In *Proceedings of the 11th Annual Conference on Innovative Data Systems Research (CIDR 2021)*. https://www.databricks.com/research/lakehouse-a-new-generation-of-open-platforms-that-unify-data-warehousing-and-advanced-analytics

Barratt, M., & Oke, A. (2007). Antecedents of supply chain visibility in retail supply chains: A resource-based theory perspective. *Journal of Operations Management, 25*(6), 1217–1233. https://doi.org/10.1016/j.jom.2007.01.003

Bazigu, A., & Mwebaze, J. (2025). *A framework for IoT-enabled smart manufacturing for energy and resource optimization* (arXiv:2502.03040). arXiv. https://arxiv.org/abs/2502.03040

Daily Monitor. (n.d.-a). *Brace for rainy season, weather experts warn*. https://www.monitor.co.ug/uganda/news/national/brace-for-rainy-season-weather-experts-warn-1809326

Daily Monitor. (n.d.-b). *Brutal realities of Uganda's logistics industry*. https://www.monitor.co.ug/uganda/business/prosper/brutal-realities-of-uganda-s-logistics-industry-4439594

Daily Monitor. (n.d.-c). *How Namanve is building climate resilience into Uganda's industrial future*. https://www.monitor.co.ug/uganda/special-reports/how-namanve-is-building-climate-resilience-into-uganda-s-industrial-future-5557158

Daily Monitor. (n.d.-d). *Let the flash floods serve as warning*. https://www.monitor.co.ug/uganda/oped/editorial/let-the-flash-floods-serve-as-warning-4584322

DeLone, W. H., & McLean, E. R. (2003). The DeLone and McLean model of information systems success: A ten-year update. *Journal of Management Information Systems, 19*(4), 9–30. https://doi.org/10.1080/07421222.2003.11045748

Galbraith, J. R. (1973). *Designing complex organizations*. Addison-Wesley.

Hevner, A. R., March, S. T., Park, J., & Ram, S. (2004). Design science in information systems research. *MIS Quarterly, 28*(1), 75–105. https://doi.org/10.2307/25148625

Koetse, M. J., & Rietveld, P. (2009). The impact of climate change and weather on transport: An overview of empirical findings. *Transportation Research Part D: Transport and Environment, 14*(3), 205–221. https://doi.org/10.1016/j.trd.2008.12.004

Lulagala, R. (2016). *Delivery cycle time of Ugandan manufacturing firms: A case of food processing firms in Kampala* [Unpublished master's thesis]. Makerere University.

McKinney, W. (2010). Data structures for statistical computing in Python. In *Proceedings of the 9th Python in Science Conference* (pp. 56–61). https://doi.org/10.25080/Majora-92bf1922-00a

Ministry of Finance, Planning and Economic Development. (n.d.). *Namanve Industrial Park monitoring visit*. https://finance.go.ug/node/714

Nkumba University. (2025). *Production facility location and supply chain performance of manufacturing companies in Uganda, a case study of Kassanda sugar factory*. Nkumba University Institutional Repository. https://ir.nkumbauniversity.ac.ug/items/fbf93c1d-3d32-4102-9d4f-80627d3328e7

Pedregosa, F., Varoquaux, G., Gramfort, A., Michel, V., Thirion, B., Grisel, O., Blondel, M., Prettenhofer, P., Weiss, R., Dubourg, V., Vanderplas, J., Passos, A., Cournapeau, D., Brucher, M., Perrot, M., & Duchesnay, É. (2011). Scikit-learn: Machine learning in Python. *Journal of Machine Learning Research, 12*, 2825–2830. https://jmlr.org/papers/v12/pedregosa11a.html

Pulse Uganda. (n.d.). *Uganda's manufacturers seek Government action to reduce production costs*. https://www.pulse.ug/business/ugandas-manufacturers-seek-government-action-to-reduce-production-costs/w7jdyez

Pulse Uganda. (2026, July 11). *Namanve Industrial Park infrastructure project reaches 80% completion*. https://www.pulse.ug/story/namanve-industrial-park-infrastructure-project-reaches-80percent-completion-2026071107123074860

Raasveldt, M., & Mühleisen, H. (2019). DuckDB: An embeddable analytical database. In *Proceedings of the 2019 International Conference on Management of Data* (pp. 1981–1984). ACM. https://doi.org/10.1145/3299869.3320212

Republic of Uganda. (2019). *Data Protection and Privacy Act, 2019* (Act No. 9 of 2019). Uganda Gazette.

The Observer. (n.d.). *Namanve on course to become regional hub for trade and manufacturing*. https://observer.ug/business/namanve-on-course-to-become-regional-hub-for-trade-and-manufacturing/

Tushman, M. L., & Nadler, D. A. (1978). Information processing as an integrating concept in organizational design. *Academy of Management Review, 3*(3), 613–624. https://doi.org/10.5465/amr.1978.4305791

Uganda Investment Authority. (n.d.). *New UIA Board impressed with industrial park developments*. https://ugandainvest.go.ug/?p=790

UNDP Uganda. (2022, December 18). *Designing Uganda's digital transformation roadmap*. United Nations Development Programme. https://www.undp.org/uganda/blog/designing-ugandas-digital-transformation-roadmap

---

## APPENDICES

### Appendix A: Sample Dataset and Simulation Assumptions

Samples below come from the generated files (full files: `raw/*.csv`, `raw/transporter_dispatch.json` and `lake/gold/delivery_fact.parquet` in the project folder). Table A1 shows raw ERP rows; Table A2 raw gate-log rows (note blank or mixed formats); Table A3 Gold-layer rows.

**Table A1**
*Sample Raw ERP Purchase Orders*

| po_number | supplier_id | material | load_tonnes | planned_arrival_ts |
|---|---|---|---|---|
| PO100001 | S02 | Resin/Chemicals | 24.4 | 2024-01-01 15:27:31 |
| PO100002 | S03 | Steel coil | 21.8 | 2024-01-01 10:12:36 |
| PO100003 | S10 | Spare parts | 15.9 | 2024-01-01 12:39:36 |
| PO100004 | S07 | Resin/Chemicals | 19.1 | 2024-01-01 09:58:05 |
| PO100005 | S04 | Agro raw material | 21.2 | 2024-01-01 10:19:45 |
| PO100006 | s01 | Packaging | 11.6 | 2024-01-01 15:45:52 |

**Table A2**
*Sample Raw Gate Log (as received, including defects)*

| po_number | truck_id | gate_arrival_ts | offload_complete_ts |
|---|---|---|---|
| PO100732 | UA147C | 2024-05-09 18:43:56 | 2024-05-09 19:56:17 |
| PO100806 | UA293C | 2024-05-24 13:35:28 | 2024-05-24 14:44:10 |
| PO101587 | UA550D | 2024-10-29 19:08:57 | 2024-10-29 20:02:27 |
| PO102694 | UA369H | 2025-06-02 22:10:42 | 2025-06-02 23:02:13 |
| PO100174 | UA538D | 2024-02-01 12:39:34 | 2024-02-01 13:32:54 |
| PO103630 | UA927D | 2025-12-10 16:24:18 | 2025-12-10 17:22:54 |

**Table A3**
*Sample Gold-Layer Delivery Fact Rows*

| po_number | zone | planned | gate arrival | delay (min) | on time | rain (mm) | rain 3d | flood |
|---|---|---|---|---|---|---|---|---|
| PO100002 | Entebbe | 01-Jan 10:12 | 01-Jan 10:29 | 17 | 1 | 4.5 | 4.5 | 0 |
| PO100004 | Kampala Industrial Area | 01-Jan 09:58 | 01-Jan 11:07 | 69 | 0 | 4.5 | 4.5 | 0 |
| PO100003 | Jinja | 01-Jan 12:39 | 01-Jan 12:40 | 1 | 1 | 4.5 | 4.5 | 0 |
| PO100006 | Kampala Industrial Area | 01-Jan 15:45 | 01-Jan 16:13 | 28 | 1 | 4.5 | 4.5 | 0 |
| PO100001 | Mukono | 01-Jan 15:27 | 01-Jan 15:24 | -3 | 1 | 4.5 | 4.5 | 0 |
| PO100010 | Mbarara | 02-Jan 13:33 | 02-Jan 14:03 | 30 | 1 | 0.0 | 4.5 | 0 |

**Table A4**
*Key Simulation Parameters*

| Parameter | Value |
|---|---|
| Period; seed | 1 Jan 2024 – 31 Dec 2025 (731 days); random seed 2026 |
| Suppliers; zones; materials | 12 suppliers; 6 zones; 5 material categories |
| Standard lead time (h) | Mukono 1.0; Kampala IA 1.5; Entebbe 2.0; Jinja 2.5; Mbale 5.5; Mbarara 6.5 |
| Rain sensitivity (min per mm) | 1.0 (Mukono) to 2.0 (Mbarara); 25% weight on the two prior days |
| Event penalties | Flooding 96–144 min; road works 45 min; accident 60 min |
| Injected defects | 3% duplicate gate rows; 2% missing arrival; 1.5% wrong date format; 1% arrival before dispatch; 2% supplier-code case errors; 1.5% missing dispatch records; 18 missing rainfall days |

### Appendix B: Data Dictionary (Gold Table `delivery_fact`)

| Field | Type | Description |
|---|---|---|
| po_number | string | Unique purchase-order number (primary key) |
| supplier_id, zone, material | string | Supplier code, origin zone and material category |
| load_tonnes | float | Delivered load in tonnes |
| dispatch_ts | timestamp | Time truck left supplier (imputed from plan where missing) |
| planned_arrival_ts | timestamp | ERP planned arrival |
| gate_arrival_ts | timestamp | Actual arrival recorded at the gate |
| std_lead_h | float | Standard lead time for the zone, in hours |
| delay_min | integer | Actual minus planned arrival, in minutes |
| on_time | 0/1 | 1 if delay ≤ 30 minutes |
| rain_mm_day, rain_3d_mm | float | Rainfall on dispatch day; cumulative over day and two prior days |
| flooding_flag, roadworks_flag, accident_flag | 0/1 | Road event logged on dispatch day |
| dispatch_hour, dow, month | integer | Calendar features for modelling |

### Appendix C: Pipeline Code Excerpts

Figure C1 shows the Silver validation logic. Listing C1 is the Silver cleaning stage, condensed from the executed pipeline (`code/02_pipeline.py`). Listing C2 is the designed Airflow DAG, not executed in this project.

**Figure C1**
*Silver-Layer Validation Flowchart*

![Figure C1 – Silver Validation Flowchart](figure-c1.png)

*Source: designed by the author; implemented in `code/02_pipeline.py`.*

**Listing C1**
*Silver-layer cleaning and validation (Python / pandas), condensed from the executed pipeline*

```python
e = erp.drop_duplicates('po_number').copy()
e['supplier_id'] = e['supplier_id'].str.upper().str.strip()
e['planned_arrival_ts'] = pd.to_datetime(e['planned_arrival_ts'], errors='coerce')

gd = gate.drop_duplicates('po_number', keep='first').copy()
a   = pd.to_datetime(gd.gate_arrival_ts, format='%Y-%m-%d %H:%M:%S', errors='coerce')
alt = pd.to_datetime(gd.gate_arrival_ts, format='%d/%m/%Y %H:%M', errors='coerce')
gd['gate_arrival_ts'] = a.fillna(alt)          # mixed formats -> one standard

s = (e.merge(gd[['po_number','truck_id','gate_arrival_ts']], on='po_number', how='left')
      .merge(dd[['po_number','dispatch_ts']], on='po_number', how='left')
      .merge(sup[['supplier_id','zone']], on='supplier_id', how='left'))
s['dispatch_imputed'] = s.dispatch_ts.isna()
s.loc[s.dispatch_imputed, 'dispatch_ts'] = s.planned_arrival_ts - pd.to_timedelta(s.std_lead_h, unit='h')

r_missing    = s.gate_arrival_ts.isna()
r_impossible = (~r_missing) & (s.gate_arrival_ts < s.dispatch_ts)
silver = s[~r_missing & ~r_impossible].copy()   # valid records only
quarantine = s[r_missing | r_impossible]        # kept, with reject_reason
silver.to_parquet('lake/silver/deliveries.parquet', index=False)
```

**Listing C2**
*Designed Airflow DAG (illustrative)*

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pipeline  # module wrapping the three stages

with DAG('namanve_inbound_logistics', start_date=datetime(2026, 11, 1),
         schedule='0 5 * * *', catchup=False, default_args={'retries': 2}) as dag:
    bronze = PythonOperator(task_id='bronze_ingest', python_callable=pipeline.bronze_ingest)
    silver = PythonOperator(task_id='silver_clean',  python_callable=pipeline.silver_clean)
    gold   = PythonOperator(task_id='gold_build',    python_callable=pipeline.gold_build)
    bronze >> silver >> gold
```

### Appendix D: Project Management Tools Used

Microsoft Project holds the work breakdown structure and critical path; ClickUp (or Trello) holds the task board; Zotero stores sources and generates APA citations; Jira can track pipeline defects in the pilot. Figure D1 is the proposed 12-week plan.

**Figure D1**
*Proposed 12-Week Pilot Plan with Stage Gates*

![Figure D1 – 12-Week Pilot Plan](figure-d1.png)

*Source: planned by the author. Gates at weeks 2, 7 and 12.*

**Table D1**
*Project Risk Register*

| ID | Risk | L | I | Mitigation |
|---|---|---|---|---|
| R1 | Plant denies access to real data | 3 | 5 | Obtain written sponsor approval; use anonymised extracts |
| R2 | Poor source data quality | 4 | 3 | Quality rules and quarantine; fix gate procedures |
| R3 | Scope creep (GPS, real-time) | 3 | 3 | Freeze scope per phase; change control |
| R4 | Skills gap in Python/SQL | 3 | 3 | Training week; documented code |
| R5 | Rainfall data source unavailable | 2 | 4 | Use two providers; fall back to monthly medians |
| R6 | Power or network outage on pipeline host | 3 | 3 | Scheduled retries; cloud or UPS-backed host |

*Note: L = likelihood, I = impact (1–5). Ratings are the author's judgement.*

**Table D2**
*Illustrative ClickUp / Trello Board*

| Column | Example cards | Owner |
|---|---|---|
| Backlog | Data-sharing agreement; supplier master clean-up; KPI definitions | Project manager |
| In progress | Bronze ingestion; Silver rules; rainfall connector | Data engineer |
| Review | Reconciliation tests; dashboard review with planners | Supply chain manager |
| Done | Gold tables; ETA model; pilot sign-off | Sponsor |

### Appendix E: Dashboard Mock-up

**Figure E1**
*Planner Dashboard Mock-up Built from Gold Tables*

![Figure E1 – Planner Dashboard Mock-up](figure-e1.png)

*Note: static mock-up generated from the Gold layer; a live Excel or Power BI report is a design deliverable.*