[![CI](https://github.com/kathirvelrajmohan/silver_transform_standalone/actions/workflows/ci.yml/badge.svg)](https://github.com/kathirvelrajmohan/silver_transform_standalone/actions/workflows/ci.yml)

# Silver Transform — Standalone PySpark Script

A standalone, Databricks-independent version of the Silver-layer transformation
from [sales-etl-pipeline](https://github.com/kathirvelrajmohan/sales-etl-pipeline).
Reads the raw Superstore dataset, applies the same enrichment logic used in
the Databricks notebook (`03_silver_transform.ipynb`), and runs entirely on
local infrastructure — no Databricks workspace, Unity Catalog, or cluster
required.

## Why this exists

The original Silver transform runs inside Databricks, using `spark.table()`
reads and `saveAsTable()` writes against Unity Catalog. This script proves
the actual transformation logic — date derivations, shipping speed,
profitability flags, sales/discount banding, unit price calculations — has
no hidden dependency on the Databricks platform itself. Only the read/write
layer changes; the business logic is identical.

## What it does

Given the raw Superstore CSV, the script:
- Normalizes all column names to snake_case
- Parses `order_date` / `ship_date` into proper date types
- Derives 13 analytical columns: order year/month/quarter/day-of-week,
  shipping days & speed, profit margin & profitability flag, sales &
  discount bands, unit price & discount amount
- Writes the enriched dataset as Parquet
- Reads the output back and verifies row counts and null-checks every
  derived column

## Running locally

**Requirements:** Java 17 (Spark 3.5.x is not validated against newer JDKs),
Python 3.12, and — on Windows — `winutils.exe`/`hadoop.dll` for local
filesystem writes (see [Troubleshooting](#troubleshooting-notes)).

```bash
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
python scripts/silver_transform_standalone.py
```

By default the script reads from and writes to local paths under `data/`.
Override with environment variables:

```bash
set INPUT_PATH=C:\path\to\superstore.csv
set OUTPUT_PATH=C:\path\to\output
```

## Running with Docker

```bash
docker build -t silver-transform:v1 .
docker run --rm silver-transform:v1
```

Docker sidesteps every Windows-specific Hadoop/Java compatibility issue
below — the container runs Linux, where none of them apply.

## Troubleshooting notes

Real issues hit while building this, kept here since they're common and
not always obvious from official docs:

- **CSV column misalignment**: Spark's default CSV reader misparsed rows
  containing embedded quotes in product names (e.g. `5 1/2" X 4"`),
  shifting numeric columns into string type. Fixed with
  `escape='"', multiLine=True` on `spark.read.csv()`.
- **Java version conflicts**: Spark 3.5.x is only validated against
  Java 8/11/17. A stray `javapath` shim in Windows' System `PATH` (added by
  an Oracle Java installer) silently overrode a correctly-set `JAVA_HOME`.
  Diagnosed with `where.exe java`.
- **`winutils.exe` / `HADOOP_HOME`**: local Spark writes on Windows require
  a Hadoop compatibility shim not bundled with PySpark. Binaries from
  [cdarlint/winutils](https://github.com/cdarlint/winutils).
- **Stale environment variables in IDE terminals**: VS Code's integrated
  terminal can hold an environment snapshot from before a system variable
  was set. A full application restart (or a fresh OS-level terminal) is
  required to pick up changes.
- **`openjdk` Docker images deprecated**: `openjdk:*` tags are no longer
  published on Docker Hub. Switched to `eclipse-temurin`, the actively
  maintained community successor.

## Project structure

```
.
├── Dockerfile
├── requirements.txt
├── data/
│   └── superstore.csv
└── scripts/
    └── silver_transform_standalone.py
```
