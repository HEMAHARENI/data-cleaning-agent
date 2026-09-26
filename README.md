# DataGuard — Automated Data Quality & Integrity Pipeline

DataGuard takes a messy tabular dataset and returns a **validated dataset**, a **quarantine file** of rows that break your rules, and a **full audit report**. Along the way it handles imputation, duplicate detection, normalization and outlier detection. Files up to 100 MB run in memory on **Pandas**. Anything larger switches automatically to a **Dask cluster**, and both engines run the same cleaning logic.

```
upload ─► profile (raw) ─► normalize ─► de-duplicate ─► outliers ─► impute ─► validate / quarantine ─► profile (clean) ─► score ─► write
             │                                                                                                                │
             └───────────── Pandas if file ≤ 100 MB  ·  Dask cluster (LocalCluster or external scheduler) if > 100 MB ──────────┘
```

## Features

| Stage | What it does |
|---|---|
| **Engine switch** | Uses file size to choose Pandas or Dask. The threshold is set by `DQ_DASK_THRESHOLD_MB` (default 100). You can also force either engine per job. Dask uses a local cluster by default, or an external scheduler through `DASK_SCHEDULER_ADDRESS`. |
| **Normalization** | Converts column names to snake_case and de-duplicates them. Trims whitespace and collapses repeated spaces. Converts null-like tokens (`N/A`, `null`, `ERROR`, `UNKNOWN`, `#VALUE!`, `-`, `""` …) to missing. Infers types from a sample: numeric (handles `$52,000`, `(300)`, `12%`), datetime (ISO first, then mixed formats), boolean (only when real words like `Yes/no/TRUE` appear, so digit codes stay numeric) and email (lower-cased). Unifies category spellings (`chennai`, `CHENNAI`, `Chennai ` → `Chennai`). |
| **Duplicates** | Removes exact duplicate rows and duplicates on business keys you choose. Also removes **near-duplicates**, meaning rows that differ only in case, spacing or punctuation. These are found with a hashed canonical row signature, so detection scales on Dask without pairwise comparisons. ID columns and timestamps stay in the signature by default, so separate transactions are never merged; `ignore_id_columns` is available when re-imported records get new IDs. |
| **Outliers** | Three detection methods: IQR fence (default k=3, Tukey's "far out"), z-score, or robust MAD. Four actions: **cap** (winsorise), **null** (then impute), **remove** or **flag** (`dq_outlier` column). Strongly skewed columns (prices, counts, durations) are judged on a signed-log scale, so legitimately large values such as airport fares aren't clipped. IDs, coordinates, postcodes, years and low-cardinality numeric codes (e.g. `payment_type` 1–6) are skipped. Whole-number columns keep whole-number bounds. |
| **Imputation** | Drops columns that exceed the missing threshold (default 60%). With the default `auto` strategy, numeric columns get the median when \|skew\| > 1 and the mean otherwise (rounded for integer columns). Numeric codes and IDs get the mode, because an average is not a valid code. Datetimes get the median timestamp. Categories and booleans get the mode (ties broken deterministically), and free text such as names gets `"Unknown"`. Per-column overrides: mean, median, mode, constant, ffill, drop_rows, KNN (Pandas only), none. |
| **Validation** | Rule types: `not_null`, `unique`, `range`, `regex`, `allowed_values`, `min_length`, and the cross-column `less_or_equal` (e.g. pickup ≤ drop-off). Failing rows go to `quarantine.csv` with a `dq_failed_rules` column. Two built-in post-conditions always run: no missing values and no duplicate rows remain. |
| **Report** | Before/after quality score, a per-column profile, an issue log (step · column · issue · count · resolution), validation results, a preview and per-stage timings. |

## Quick start

### Option A — local (Python 3.10+)

```bash
cd backend
pip install -r requirements-dev.txt
uvicorn app.main:app --reload --port 8000
# open http://localhost:8000
```

Click **Generate** to create a messy sample: 5k rows for Pandas, or 1.2M rows (~115 MB) to trigger Dask. Or upload your own CSV/TSV/Parquet/JSON/Excel file. Then configure the checks and click **Run data-quality pipeline**.

### Option B — Docker with a real Dask cluster

```bash
docker compose up --build
# app:            http://localhost:8000
# Dask dashboard: http://localhost:8787
```

This starts one scheduler, two workers and the app, all sharing a `/data` volume.

### Command line (no UI)

```bash
python scripts/generate_data.py --rows 1200000 --out data/big.csv      # ~115 MB
python scripts/run_cli.py data/big.csv --rules rules.json               # auto -> Dask
```

`rules.json` example:

```json
[
  {"column": "age", "rule": "range", "min": 0, "max": 120},
  {"column": "email_address", "rule": "regex", "pattern": "[^@\\s]+@[^@\\s]+\\.[a-z]{2,}"},
  {"column": "plan_type", "rule": "allowed_values", "values": ["Basic", "Premium", "Enterprise"]},
  {"column": "customer_id", "rule": "unique"}
]
```

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/datasets` | Upload a file (multipart, streamed to disk; `.gz`/`.bz2`/`.xz`/`.zip` are unpacked server-side so the engine choice uses the real size). Returns the schema, inferred types, a preview and the chosen engine. |
| `POST` | `/api/datasets/sample?rows=N` | Generate a messy demo dataset |
| `POST` | `/api/jobs` | `{dataset_id, config}` → starts a background job (HTTP 202) |
| `GET` | `/api/jobs/{id}` | Status, progress %, current step, live log |
| `GET` | `/api/jobs/{id}/report` | Full JSON report |
| `GET` | `/api/jobs/{id}/download/{cleaned\|quarantine\|report}` | Output files |
| `GET` | `/api/health` | API + Dask cluster status (workers, threads, memory, dashboard link) |
| `GET` | `/api/config/defaults` | Default pipeline config |

Interactive docs: `http://localhost:8000/docs`.

## How the quality score is computed

`overall = 0.4 × completeness + 0.3 × uniqueness + 0.3 × validity` (each 0–100)

- **Completeness** = 1 − missing cells / total cells. For the raw data, null-like tokens count as missing.
- **Uniqueness** = 1 − exact duplicate rows / rows.
- **Validity (raw)** = 1 − cells that needed a fix / total cells. Fixes include whitespace, casing, unparseable values, category variants and outliers.
- **Validity (clean)** = 1 − remaining rule failures and flagged outliers / total cells.

This is a transparent heuristic, not an industry standard. Tune the weights in `engine/quality.py::score` if you need to.

## Project layout

```
backend/app/
  main.py            FastAPI routes + static frontend
  jobs.py            thread-pool job runner with progress + logs
  config.py          env-driven settings
  engine/
    loader.py        file readers, size-based engine choice, adaptive Dask blocksize
    cluster.py       lazy LocalCluster / external scheduler client
    frame.py         Pandas/Dask abstraction (map_partitions, one-pass compute, persist)
    normalize.py     names, whitespace, null tokens, type inference/coercion, category canonicalisation
    duplicates.py    exact / key / near-duplicate removal
    outliers.py      IQR / z-score / MAD with cap/null/remove/flag
    imputation.py    auto & per-column strategies, sparse-column drop, KNN
    quality.py       profiling, validation rules + quarantine, score
    pipeline.py      orchestration + report
    sample.py        messy synthetic data generator
backend/tests/       pytest: every behaviour tested on both engines + API tests
frontend/            index.html, styles.css, app.js (no build step, light/dark, responsive)
scripts/             generate_data.py, run_cli.py
Dockerfile, docker-compose.yml
```

## Design notes

- **One code path, two engines.** Each cleaning rule is written as a plain Pandas function. It runs directly on Pandas, or per partition through `map_partitions` on Dask. The statistics each step needs (counts, quantiles, moments, modes) are gathered lazily and resolved in a single `dask.compute`, so every stage makes one distributed pass. Intermediate frames are `persist()`ed in cluster memory.
- **Types are inferred, not guessed by the CSV reader.** Text files are read as raw strings by both engines. This avoids Dask's sample-based dtype inference failing partway through a file.
- **Skewness in one pass.** Skew comes from raw moments E[x], E[x²], E[x³], so the Dask graph doesn't need a second pass to choose between mean and median.
- **Tested equivalence.** `test_pandas_and_dask_agree` checks that both engines produce identical cleaned output.

## Tested on

- **Synthetic messy data** (`scripts/generate_data.py`): 5k rows on Pandas, 1.2M rows (~115 MB) automatically on Dask.
- **NYC Yellow Taxi, Jan 2021** (1.37M rows, 126 MB → Dask, 8 partitions): 98k trips missing vendor/passenger/payment fields, negative fares, zero-passenger trips, drop-off before pickup. Rules: `scripts/example_rules_nyc_taxi.json`. Source: [DataTalksClub/nyc-tlc-data](https://github.com/DataTalksClub/nyc-tlc-data/releases) (upload the `.csv.gz` directly).

**Real public datasets** (both downloaded from GitHub mirrors):

| Dataset | What DataGuard found | Settings worth changing |
|---|---|---|
| Dirty Cafe Sales (10,000 rows) | 10,082 `ERROR`/`UNKNOWN`/blank cells turned into missing values and imputed. 0 rows removed, because transaction IDs keep distinct sales apart. | `total_spent` is imputed on its own. It is not recomputed as `quantity × price_per_unit`. |
| NYC Airbnb 2019 (48,895 rows) | 49 missing names set to "Unknown". Skewed prices and nights capped on a log scale (71 and 125 values). | Set `reviews_per_month` → `constant:0` and `last_review` → `none`: those listings have no reviews, so the values aren't unknown. Exclude `calculated_host_listings_count` from outlier checks, since hosts with 300+ listings are real. |

**Honest caveat:** on a 2-core machine, Pandas beats Dask on a 115 MB file because scheduling and shuffle overhead outweigh the parallelism. Dask pays off when data approaches or exceeds RAM (it streams by partition instead of loading the whole file) or when workers run on more cores or machines. The 100 MB threshold is a conservative default; raise `DQ_DASK_THRESHOLD_MB` on big single machines.

**Known limitations**

- Dask quantiles are approximate (t-digest), so outlier counts can differ slightly between engines on large files (22,510 vs 22,587 above).
- The `unique` rule on Dask reports a count based on `nunique_approx` and doesn't quarantine rows.
- Near-duplicate detection catches case, spacing and punctuation variants. It does not catch typos (no fuzzy edit distance).
- Imputation is per column: a missing `total` isn't recomputed from `quantity × price`.
- Missing values that mean "none" rather than "unknown" (e.g. `reviews_per_month` for listings with no reviews) need a per-column override such as `constant:0`.
- Jobs are kept in memory. Restarting the server clears job history, but reports on disk stay downloadable.

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `DQ_DASK_THRESHOLD_MB` | 100 | Size above which Dask is used |
| `DASK_SCHEDULER_ADDRESS` | — | External scheduler, e.g. `tcp://scheduler:8786` |
| `DQ_DASK_WORKERS` / `DQ_DASK_THREADS` | 2 / 2 | LocalCluster size |
| `DQ_DASK_PROCESSES` | true | Process-based workers (set `false` for in-process) |
| `DQ_DASK_BLOCKSIZE` | 64MB | Max partition size (the actual size adapts so each worker thread gets ≥2 partitions) |
| `DQ_DATA_DIR` | `./data` | Uploads, job outputs, reports |
| `DQ_MAX_UPLOAD_MB` | 4096 | Upload limit |

## Tests

```bash
cd backend && pytest -q        # 19 tests; behavioural tests run on both engines
```
