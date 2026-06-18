# 🏎️ Formula 1 Data Pipeline - Databricks Medallion Architecture

An end-to-end data engineering pipeline built on **Azure Databricks** that ingests, transforms, and models Formula 1 race data using a Bronze → Silver → Gold medallion architecture. The project demonstrates production-ready data engineering patterns including incremental batch processing, Delta Lake upserts, and pipeline orchestration.

---

## 📐 Architecture Overview

```
┌──────────────────────────────────┐
│  LANDING ZONE (ADLS Gen2)        │  Raw F1 CSV files dropped here
│  Full load: all files at once    │  Incremental: new batch folder
│  e.g. /landing/2025-01/          │  added per race batch arrival
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│            BRONZE                │  Raw ingestion — schema applied, data landed as Delta tables
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│            SILVER                │  Filtered, cleaned & augmented — standardised and enriched data
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│             GOLD                 │  Aggregated & modelled — fact/dimension tables ready for analytics
└──────────────┬───────────────────┘
               │
               ▼
     Databricks Lakeview Dashboard
         + Genie (NL Queries)
```

---

## 🗂️ Folder Structure

```
├── formula 1-project/               # Full batch pipeline
│   ├── 00-common/                   # Shared utility functions and config
│   ├── 01-setup/                    # Environment setup and batch event configuration
│   ├── 02-bronze/                   # Raw ingestion notebooks (circuits, races, drivers, results, sprints, constructors)
│   ├── 03-silver/                   # Data transformation notebooks
│   ├── 04-gold/                     # Aggregation and dimension/fact table notebooks
│   ├── 05-analytics/                # SQL views for reporting and dashboards
│   └── 06-orchestration/            # Batch management notebooks
│
├── formula 1-project-incremental-load/   # Incremental load version of the pipeline
│   ├── 00-common/
│   ├── 01-setup/
│   ├── 02-bronze/
│   ├── 03-silver/
│   ├── 04-gold/
│   ├── 05-analytics/
│   └── 06-orchestration/
│
└── screenshots/                     # Dashboard and pipeline screenshots
```

---

## ⚙️ Tech Stack

| Layer | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Data Lake Storage | Azure Data Lake Storage Gen2 (ADLS) |
| Compute & Notebooks | Azure Databricks |
| Table Format | Delta Lake |
| Processing | PySpark |
| Orchestration | Databricks LakeFlow Jobs |
| Governance | Unity Catalog |
| Visualization | Databricks Lakeview Dashboard |

---

## 🔄 Key Engineering Patterns

**Incremental Batch Processing** — The pipeline supports incremental loads using a batch date parameter. Each run processes only new race data, making it idempotent and efficient.

**Delta Lake Upserts (Merge)** — Event data like race results uses `MERGE` operations to upsert records without duplicating data across runs. A cold-start guard initializes the target table on the first run.

**Idempotent Rewrites with replaceWhere** — Combined with `partitionBy`, `replaceWhere` ensures that re-running a batch only overwrites the relevant partition, leaving historical data untouched.

**Snapshot vs Event Write Strategies** — Snapshot/reference tables (circuits, races, constructors, drivers) use Append at Bronze and Overwrite/Merge at Silver and Gold. Event/change data tables (results, sprints) use Append at Bronze and Merge all the way through to Gold — preserving full history across incremental runs.

**Single Job Cluster Orchestration** — All pipeline tasks share a single job cluster in LakeFlow Jobs to avoid Azure quota exhaustion and reduce compute costs.

---

## 📊 Data Source

Formula 1 historical dataset sourced from the **[jolpica/jolpica-f1](https://github.com/jolpica/jolpica-f1)** GitHub repository, using the relational-style **Ergast format**.

The dataset includes 6 CSV files covering:

| File | Description |
|---|---|
| `circuits.csv` | Circuit details (location, country, lat/long) |
| `races.csv` | Race schedule per season |
| `constructors.csv` | Constructor/team information |
| `drivers.csv` | Driver details and nationality |
| `results.csv` | Race results per driver per race |
| `sprints.csv` | Sprint race results |

**To download the data:**
1. Go to [https://github.com/jolpica/jolpica-f1](https://github.com/jolpica/jolpica-f1)
2. Download the CSV files in Ergast format
3. Upload them to your ADLS Gen2 container under `/landing/files/` for the full load pipeline
4. For the incremental pipeline, organize them under `/landing/{batch_id}/` (e.g. `/landing/2025-01/`)

Raw data files are not included in this repository — they live in ADLS Gen2 as the bronze layer source.

---

## 📸 Screenshots

### LakeFlow Job DAG
*(See /screenshots/Lakeflow_Jobs/full_data_pipeline.png)*

### Analytics Dashboard
*(See /screenshots/dashboard/)*
- **Driver Championship Standings** — screenshots/dashboard/Drivers_Champs.png
- **Constructor Championship Standings** — screenshots/dashboard/Constructor_Champs.png
- **Dominant Drivers of All Time** — screenshots/dashboard/Dominant_Drivers.png
- **Dominant Teams of All Time** — screenshots/dashboard/Dominant_Teams.png

---

## 🚀 How to Run

1. Set up an Azure Databricks workspace and ADLS Gen2 storage account
2. Upload raw F1 CSV files to your ADLS container
3. Configure storage credentials in `00-common/01.environment-config.py`
4. Run `01-setup/` notebooks to initialize the environment
5. Execute the LakeFlow Job to trigger the full pipeline (Bronze → Silver → Gold)
6. View results in the Lakeview dashboard

---

## 💡 What I Learned

**Delta Lake Internals**
Explored Delta Lake's transaction log (`_delta_log`) which records every operation as a JSON commit, making tables ACID-compliant. Practiced time travel to query previous versions of a table using `VERSION AS OF` and `TIMESTAMP AS OF`, and used `VACUUM` to clean up old file versions while respecting the retention threshold.

**Batch Partitioning & Incremental Load**
Built two versions of the pipeline — a full load and an incremental load. In the full load, all raw F1 CSV files are placed in the ADLS landing folder and the pipeline processes everything in one run. In the incremental version, a utility notebook simulates real-world batch arrivals by dropping a new dated folder (e.g. `2025-01`) into the landing zone alongside existing historical data. The pipeline automatically detects and processes only the new batch, leaving previously loaded partitions untouched. Combined `partitionBy` with `replaceWhere` to make reruns idempotent — only the relevant partition gets overwritten, historical data stays intact.

**Delta Lake Merge (Upsert)**
Implemented `MERGE` operations for event tables so the pipeline can upsert new records without duplicating data across incremental runs. Learned the cold-start pattern — using an `if/else` guard to create the target table on the first run before merge can execute.

**Unity Catalog & Data Governance**
Set up a three-level namespace (catalog → schema → table) in Unity Catalog to organize and govern the F1 dataset. Configured storage credentials and external locations to control access to ADLS Gen2 from within Databricks securely.

**LakeFlow Jobs & Orchestration**
Orchestrated the full Bronze → Silver → Gold pipeline using Databricks LakeFlow Jobs with task dependencies. Configured all tasks to run on a single shared job cluster to avoid Azure subscription quota exhaustion and minimize compute costs.

**Analytics & Genie**
Built a Lakeview dashboard on top of the Gold layer to visualize driver standings and constructor standings. Explored Databricks Genie for natural language querying of the F1 dataset — allowing plain English questions to be answered directly from the Delta tables.
