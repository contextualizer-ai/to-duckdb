
# Migrating Data from MongoDB to DuckDB

Great question—there are a few clean, free/open-source ways to move MongoDB data into DuckDB, whether your docs are already flat or still nested. Below are the fastest, least-surprising paths with copy-pasteable commands.

---

## TL;DR: Best paths
- **Already flat?** Export as **JSON Lines** (not CSV) with `mongoexport`, then let **DuckDB auto-infer** and optionally write to **Parquet**.
- **Nested?** Export as **JSON Lines** and load directly—DuckDB supports **STRUCT** (objects) and **LIST** (arrays). You can keep it nested, or flatten inside DuckDB with SQL.
- **Big data / long-term**: Convert to **Parquet** (via DuckDB or Python) and query Parquet in place.

... (full response content as given above) ...
