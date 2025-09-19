# DuckDB, SQLite, and Modern Serverless Database Choices (2025)

This document consolidates our discussions on:
- Choosing between **SQLite**, **DuckDB**, and other “serverless” options (not DBeaver vs. anything—DBeaver is a GUI).
- **Switching from local DuckDB to MotherDuck**.
- **Selecting a GUI** for DuckDB or MotherDuck (including DBeaver), with the correct launch command for DuckDB’s Local UI.

---

## 1) SQLite vs. DuckDB vs. Other Serverless Databases

### SQLite
- **Focus**: OLTP (transactional workloads) in embedded/edge contexts.
- **Strengths**: single-file, zero-config, ACID, tiny footprint, ubiquitous bindings.
- **Limits**: row-store; limited concurrency (single writer, many readers); not optimized for large analytical scans/joins.

**Use SQLite (or libSQL/Turso)** when you need an embedded, transactional store inside an app, CLI, service, or device; or you want a portable file DB with minimal ops.

### DuckDB
- **Focus**: OLAP (analytics). In-process, no daemon—also “serverless,” but for analytics.
- **Strengths**: columnar execution, vectorized engine; excellent with **Parquet/CSV/JSON**; fast joins and aggregations even at 100M+ rows; supports nested types (STRUCT/LIST).
- **Limits**: not built for highly concurrent writes or multi-user transaction processing.

**Use DuckDB (or MotherDuck)** when your workload is read-heavy analytics, ad‑hoc exploration, ETL, or lakehouse-style work over files.

### Other options to consider
- **libSQL / Turso**: SQLite API + sync/replication + hosted edge. Useful if you want SQLite ergonomics with cloud reach.
- **MotherDuck**: Cloud-backed DuckDB (cloud storage, easy sharing). Pairs well with local DuckDB for hybrid workflows.
- **Serverless Postgres/MySQL**: **Neon/Supabase** (Postgres) or **PlanetScale** (MySQL) if you want managed cloud SQL with branching, auth, etc. Not embedded, but “serverless” from an ops standpoint.
- **Document / KV**: **Firestore**, **DynamoDB**, **MongoDB Atlas Serverless**—not embedded, but elastic serverless services for JSON/KV.

**Rule of thumb**
- **Embedded OLTP** → SQLite / libSQL  
- **Local analytics** → DuckDB  
- **Cloud analytics / collaboration** → MotherDuck  
- **Managed OLTP cloud SQL** → Neon / Supabase / PlanetScale  
- **Document/KV** → Firestore / DynamoDB / Atlas Serverless

---

## 2) Switching from Local DuckDB to MotherDuck

**How easy?** Very. MotherDuck speaks DuckDB. Migration is typically a connection change plus a few SQL statements.

### Common paths

**A. Attach MotherDuck and upload local data**
```sql
-- Start from your local DuckDB session (reading local files or a .duckdb file)
ATTACH 'md:';               -- authenticate once (e.g., with an MD token), then attach

CREATE DATABASE prod;       -- create a database in MotherDuck
USE prod;

-- Example: push data from a local Parquet file
CREATE OR REPLACE TABLE t AS
  SELECT * FROM read_parquet('data/local.parquet');
```

**B. Lift the entire current local database into MotherDuck**
```sql
-- From a session opened on your local .duckdb file
CREATE OR REPLACE DATABASE prod FROM CURRENT_DATABASE();
```

**C. Work in MotherDuck directly from the CLI**
```bash
duckdb "md:my_db"
```

**D. Copy between local and cloud in one session**
```sql
-- Attach both sides and copy tables explicitly
ATTACH 'md:clouddb' AS cloud;
ATTACH 'local.duckdb' AS local;

CREATE OR REPLACE TABLE cloud.t AS SELECT * FROM local.some_table;
```

### Versioning notes
If you see odd errors, check your DuckDB client version against what MotherDuck currently supports. Aligning the client usually resolves it.

### Pricing note
Plans and metering change over time. Check MotherDuck’s pricing page for current platform fees, storage, and compute rates.

### When to switch
- Switch if you want **sharing, remote/edge compute, multi-user access, or scheduled cloud jobs** with DuckDB semantics.
- Stay local if your analytics are **single-user** and run fine over local Parquet/CSV or a single `.duckdb` file.

---

## 3) GUIs for DuckDB and MotherDuck (including DBeaver)

### DuckDB Local UI (GUI)
- **What it is**: A **browser-based graphical interface** bundled with modern DuckDB builds for local exploration.
- **How to launch**:  
  ```bash
  duckdb -ui
  ```
  This opens a local GUI where you can run queries, browse schema/tables, and inspect data quality (distributions, null counts, quick histograms).
- **Good for**: Quick inspection, diagnostics, and ad‑hoc exploration without leaving DuckDB.

> If `duckdb -ui` isn’t recognized, you may be on an older build or a packaging variant without the UI.

### MotherDuck Web UI
- A cloud GUI for your MotherDuck databases: browse schemas, run queries, share with teammates. Best paired with cloud-hosted tables.

### VS Code extensions (e.g., DuckDB SQL Tools / Pro Tools)
- Integrate DuckDB into your editor: connection explorer, queries, history, and working with file-backed sources (Parquet/CSV). Great if you work in VS Code already.

### Beekeeper Studio
- A neat, cross‑platform SQL client with solid DuckDB support. Intuitive table browsing, simple import/export. Less analytics‑specific visual diagnostics than DuckDB’s own UI.

### DBeaver
- **What it is**: A **desktop multi‑DB IDE/GUI**. It connects to DuckDB via the JDBC driver and can also connect to MotherDuck.
- **Strengths**: Mature GUI; schema explorer; query history; cross‑platform; great if you already use DBeaver for Postgres/MySQL/etc.
- **Limitations**:
  - DuckDB is file‑based locally; avoid opening the same `.duckdb` from multiple processes (locking/conflicts).
  - Rendering very large result sets may feel slower than DuckDB’s own UIs.
  - Not focused on DuckDB‑specific analytics diagnostics (null distribution, histograms, etc.).

**Which GUI to pick?**
- **Local, fast analytics & diagnostics** → DuckDB Local UI (`duckdb -ui`).
- **Cloud collaboration on MotherDuck** → MotherDuck Web UI (and/or CLI attachment from local DuckDB).
- **Editor-centric workflow** → VS Code extensions.
- **Multi‑DB desktop IDE** → DBeaver.
- **Lightweight desktop SQL client** → Beekeeper Studio.

---

## 4) Quick Recipes

**Create a local DuckDB from Parquet files**
```sql
CREATE DATABASE localdb;
USE localdb;

CREATE OR REPLACE TABLE fact AS
SELECT * FROM read_parquet('data/*.parquet');

-- Save results back to Parquet
COPY (SELECT * FROM fact WHERE some_col IS NOT NULL)
TO 'out/fact_filtered.parquet' (FORMAT PARQUET);
```

**Round‑trip local ↔ MotherDuck**
```sql
-- Local → MotherDuck
ATTACH 'md:' AS md;
CREATE OR REPLACE TABLE md.prod.fact AS SELECT * FROM fact;

-- MotherDuck → Local
CREATE OR REPLACE TABLE local_copy AS SELECT * FROM md.prod.fact;
```

**Flatten nested JSON in DuckDB**
```sql
CREATE TABLE j AS
SELECT * FROM read_json_auto('data/events.json', sample_size = -1);

-- Extract nested fields
SELECT
  meta.user.id       AS user_id,
  meta.user.country  AS country,
  items              AS items_list       -- LIST
FROM j;

-- Explode arrays
SELECT
  meta.user.id AS user_id,
  item
FROM j
LEFT JOIN UNNEST(items) AS t(item);
```

---

## TL;DR

- **SQLite = embedded OLTP** (single‑file transactional engine; use libSQL/Turso for “SQLite with sync/cloud”).  
- **DuckDB = embedded OLAP** (analytics over Parquet/CSV/JSON; keep data local or pair with MotherDuck for cloud collaboration).  
- **Switching to MotherDuck is easy** (attach `md:`, copy tables, or upload your current DB).  
- **GUIs**:  
  - Launch the **DuckDB Local UI** with `duckdb -ui` for quick local visual exploration.  
  - Use the **MotherDuck Web UI** for cloud DBs.  
  - **DBeaver** is fine for multi‑DB workflows, but not optimized for DuckDB‑specific analytics visuals.  
  - **VS Code** and **Beekeeper Studio** are good alternatives depending on your workflow.  

