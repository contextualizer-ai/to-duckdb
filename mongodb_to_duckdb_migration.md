# Migrating Data from MongoDB to DuckDB

Great question—there are a few clean, free/open-source ways to move MongoDB data into DuckDB, whether your docs are already flat or still nested. Below are the fastest, least-surprising paths with copy-pasteable commands.

---

## TL;DR: Best paths

- **Already flat?** Export as **JSON Lines** (not CSV) with `mongoexport`, then let **DuckDB auto-infer** and optionally write to **Parquet**.
- **Nested?** Export as **JSON Lines** and load directly—DuckDB supports **STRUCT** (objects) and **LIST** (arrays). You can keep it nested, or flatten inside DuckDB with SQL.
- **Big data / long-term**: Convert to **Parquet** (via DuckDB or Python) and query Parquet in place.

---

## 1) If the collection is “totally flattened”

### Option A — One-liner via JSON → DuckDB table (recommended)

```bash
# Export JSON Lines (one JSON object per line)
mongoexport   --uri="mongodb://USER:PASS@HOST:PORT/DB?authSource=admin"   --collection=biosamples_flattened   --type=json   --out=biosamples_flattened.json

# In DuckDB (duckdb CLI)
duckdb
-- SQL below (inside the DuckDB prompt)
CREATE TABLE biosamples AS
SELECT *
FROM read_json_auto('biosamples_flattened.json',
                    sample_size = -1  -- scan all lines to learn all columns
                   );
```

### Option B — Immediately convert to Parquet

```sql
-- inside DuckDB
COPY (
  SELECT *
  FROM read_json_auto('biosamples_flattened.json', sample_size=-1)
) TO 'biosamples_flattened.parquet' (FORMAT PARQUET);

-- Later, super-fast reads:
CREATE TABLE biosamples_parq AS
SELECT * FROM read_parquet('biosamples_flattened.parquet');
```

---

## 2) If the collection has **nested** documents (objects/arrays)

DuckDB natively supports nested types, so you can **keep them nested** or **flatten in SQL**.

### Keep nested structure

```bash
mongoexport   --uri="mongodb://USER:PASS@HOST:PORT/DB?authSource=admin"   --collection=biosamples   --type=json   --out=biosamples.json
```

```sql
-- inside DuckDB
CREATE TABLE raw_json AS
SELECT *
FROM read_json_auto('biosamples.json', sample_size=-1);

-- Inspect inferred types (STRUCT/LIST)
DESCRIBE raw_json;
```

### Flatten on read (objects → columns)

Suppose docs look like:

```json
{"_id":"...","sample":{"lat":37.7,"lon":-122.4,"site":{"name":"SF"}},"tags":["soil","urban"]}
```

You can flatten with `struct_extract`/dot notation and unnest arrays:

```sql
-- object fields
CREATE VIEW biosamples_flat AS
SELECT
  _id,
  sample.lat        AS sample_lat,
  sample.lon        AS sample_lon,
  sample.site.name  AS site_name,
  tags              AS tags_list      -- still a LIST
FROM raw_json;

-- explode array -> one row per element
SELECT
  _id,
  sample_lat,
  sample_lon,
  site_name,
  tag
FROM biosamples_flat
LEFT JOIN UNNEST(tags_list) AS t(tag);
```

### Write flattened to Parquet

```sql
COPY (
  SELECT * FROM biosamples_flat
) TO 'biosamples_flat.parquet' (FORMAT PARQUET);
```

---

## 3) If you really want CSV (not recommended for heterogeneity)

CSV needs a stable header. If columns vary by document:

- Export **JSON** first,
- Infer the **union of keys** in DuckDB,
- Then **export to CSV** from DuckDB (it will line up columns and put NULLs).

```sql
COPY (
  SELECT * FROM read_json_auto('biosamples.json', sample_size=-1)
) TO 'biosamples.csv' (HEADER, DELIMITER ',');
```

If you must `mongoexport --type=csv` directly, you’ll have to pass `--fields` with the **full union of columns**. Generating that reliably usually requires a pre-scan (e.g., `jq`/DuckDB/pandas) and is brittle.

---

## 4) Fast and robust: JSON → DuckDB → Parquet (end-to-end)

This is often the sweet spot for performance + simplicity.

```bash
mongoexport   --uri="mongodb://USER:PASS@HOST:PORT/DB?authSource=admin"   --collection=mycoll   --type=json   --out=mycoll.json

duckdb -c "
COPY (
  SELECT *
  FROM read_json_auto('mycoll.json', sample_size=-1)
) TO 'mycoll.parquet' (FORMAT PARQUET);
"
```

---

## 5) Python route (PyMongo → Parquet) if you prefer code

```python
from pymongo import MongoClient
import pyarrow as pa, pyarrow.parquet as pq
import pandas as pd

client = MongoClient("mongodb://USER:PASS@HOST:PORT/DB?authSource=admin")
coll = client["DB"]["COLL"]

batch_size = 50_000
cursor = coll.find({}, batch_size=batch_size)

writer = None
rows = []
for i, doc in enumerate(cursor, 1):
    doc["_id"] = str(doc.get("_id"))
    rows.append(doc)
    if i % batch_size == 0:
        df = pd.DataFrame(rows)
        table = pa.Table.from_pandas(df, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter("out.parquet", table.schema)
        writer.write_table(table)
        rows = []

if rows:
    df = pd.DataFrame(rows)
    table = pa.Table.from_pandas(df, preserve_index=False)
    if writer is None:
        writer = pq.ParquetWriter("out.parquet", table.schema)
    writer.write_table(table)

if writer:
    writer.close()
```

---

## 6) Handling arrays and deep nesting in DuckDB

- **Extract nested fields**: `sample.site.name AS site_name`
- **Explode arrays**: `LEFT JOIN UNNEST(tags) AS t(tag)`
- **Flatten many levels**: chain extracts, e.g. `a.b.c AS a_b_c`

---

## 7) Extended JSON, ObjectId, dates

`mongoexport` emits Extended JSON for special types.  
Use `jq` to unwrap `$date` and `$oid`:

```bash
jq -c '
  def unext:
    with_entries(
      .value |=
        (if type=="object" and has("$date") then .["$date"]
         elif type=="object" and has("$oid") then .["$oid"]
         else (if type=="object" then (.|unext) elif type=="array" then (map(unext)) else . end)
         end)
    );
  unext
' biosamples.json > biosamples_simple.json
```

---

## 8) Very large datasets (10M+ docs)

- Prefer **JSON Lines → DuckDB → Parquet** or **PyMongo → Parquet**.
- In `read_json_auto`, set `sample_size=-1` to avoid missing rare columns.
- Query Parquet directly; avoid making a giant DuckDB table unless needed.

---

## 9) CLI snippets

```bash
mongoexport --uri="..." --collection=COLL --type=json --out=COLL.json
```

```sql
CREATE TABLE t AS SELECT * FROM read_json_auto('COLL.json', sample_size=-1);
COPY (SELECT * FROM read_json_auto('COLL.json', sample_size=-1)) TO 'COLL.parquet' (FORMAT PARQUET);
```

---

## 10) Tooling summary

- **DuckDB**: JSON/Parquet/CSV, nested types, flatten/unnest.
- **mongoexport**: JSON Lines export.
- **jq/yq/Miller**: JSON/CSV shaping.
- **PyMongo + pyarrow**: programmatic Parquet writing.

---

### Picking the path

- **Quickest win**: JSON → DuckDB → Parquet.
- **Strict schema**: Python normalization.
- **Nested data**: Keep JSON, flatten in DuckDB SQL.
