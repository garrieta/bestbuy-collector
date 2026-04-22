# Best Buy Data — Reference

What the pipeline produces, where it lives, and how to read it.

---

## Where the data lives

Everything is in one Dropbox App-folder: `/Apps/AIGarpDataPipeline/`.
On your laptop, the Dropbox client syncs it to:

```
C:\Users\garrie\Dropbox\Apps\AIGarpDataPipeline\
```

Layout:

```
AIGarpDataPipeline/
├── prices/
│   └── year=YYYY/month=MM/day=DD/
│       └── YYYY-MM-DDTHH-MM-SSZ.parquet         ← one file per run (4×/day)
│
├── metadata/
│   ├── current/
│   │   └── metadata_current.parquet             ← latest full state (overwritten)
│   ├── history/
│   │   └── year=YYYY/month=MM/day=DD.parquet    ← only CHANGED rows, per day
│   └── index/
│       └── hash_index.parquet                   ← {sku, content_hash, first_seen, last_seen}
│
└── logs/
    └── runs/
        └── YYYY-MM-DDTHH-MM-SSZ.json            ← per-run summary
```

The hive-partitioned folder names (`year=YYYY/month=MM/day=DD/`) are a DuckDB/polars convention — they let query engines prune files without reading them.

---

## What's in each file

### `prices/**/*.parquet` — price snapshots

One row per SKU per run. Written every ~6 hours.

| Column | Type | Notes |
|---|---|---|
| `sku` | string | Best Buy SKU — stable identifier |
| `regularPrice` | float | "List" / reference price |
| `salePrice` | float | Current sale price (= `regularPrice` when `onSale` is false) |
| `onSale` | bool | Whether currently on sale |
| `dollarSavings` | float | `regularPrice − salePrice` |
| `percentSavings` | string | Percent savings, as string (API quirk) |
| `onlineAvailability` | bool | Purchasable on bestbuy.com |
| `inStoreAvailability` | bool | Available in some physical store |
| `orderable` | string | "Yes" / "No" / "SoldOut" / etc. |
| `clearance` | bool | Clearance flag |
| `priceUpdateDate` | string (ISO 8601) | When Best Buy last updated the price |
| `customerReviewAverage` | float | Average rating (1–5), may be null |
| `customerReviewCount` | int | Number of reviews |
| `bestSellingRank` | int | Overall best-selling rank, may be null |
| `salesRankShortTerm` | int | Short-horizon rank, may be null |
| `fetched_at` | timestamp (UTC) | **When the pipeline fetched this row** |

Review counts and sales ranks live here (not in metadata) because they
change frequently and are best treated as a time-series.

### `metadata/current/metadata_current.parquet` — latest state

Exactly one row per active SKU. Overwritten every run. Use this when you
need the *current* attributes of each product.

Columns (40 total):

| Group | Columns |
|---|---|
| Identity | `sku`, `productId`, `bestBuyItemId`, `name`, `manufacturer`, `modelNumber`, `upc` |
| Taxonomy | `categoryPath` (JSON list), `class`, `classId`, `subclass`, `subclassId`, `department`, `departmentId`, `type` |
| Descriptive | `shortDescription`, `features` (JSON list), `includedItemList` (JSON list), `color`, `condition` |
| Flags | `new`, `preowned`, `digital`, `marketplace`, `freeShipping` |
| Physical | `weight`, `shippingWeight`, `height`, `width`, `depth`, `format`, `platform` |
| Warranty | `warrantyLabor`, `warrantyParts` |
| Timestamps | `releaseDate`, `itemUpdateDate`, `validFrom`, `validUntil` |
| Housekeeping | `content_hash`, `observed_at` |

`categoryPath`, `features`, and `includedItemList` are stored as JSON
strings (not nested structs) for cleaner querying. Parse them with
DuckDB's JSON functions (`json_extract`, `json_each`) or
`polars.Series.str.json_decode()`.

### `metadata/history/year=YYYY/month=MM/day=DD.parquet` — SCD2 change log

Same columns as `metadata_current`. Contains only rows that **changed**
on that day — detected via content-hash diff against the previous run.
The first-ever run bootstraps every SKU, so day-1 history has ~56k rows;
subsequent days are tiny (only churn).

Point-in-time query (SCD2 as-of): for each SKU, take the row with the
latest `observed_at ≤ T`. That's the product's state at time T.

### `metadata/index/hash_index.parquet` — diff index

Tiny table: `sku, content_hash, first_seen, last_seen`. Not usually
interesting for analysis — just how the pipeline decides what to write.

### `logs/runs/*.json` — per-run summaries

One JSON per run: start/end time, status, API calls, row counts, bytes
uploaded, errors. Use `bbpipeline.analysis.runs_summary()` to get a
tidy table.

---

## How to read the data

### Option 1 — The `analysis` module (recommended)

```python
from bbpipeline.analysis import (
    load_prices_latest, load_prices, load_metadata_current,
    load_metadata_at, load_panel, duckdb_connection, runs_summary,
)

latest = load_prices_latest()                        # most recent snapshot
hist   = load_prices(start="2026-04-21",             # time-bounded
                     end="2026-04-22",
                     skus=["1234567", "8901234"])    # and/or SKU-bounded
meta   = load_metadata_current()                     # current attrs, all SKUs
snap   = load_metadata_at("2026-04-22T00:00:00Z")    # as-of past moment
panel  = load_panel(start="2026-04-21")              # prices + current meta joined
runs   = runs_summary()                              # per-run health
```

All loaders return polars DataFrames. They accept an optional `data_root`
if your files are somewhere non-standard.

### Option 2 — DuckDB SQL (for anything custom)

```python
from bbpipeline.analysis import duckdb_connection
con = duckdb_connection()

# View names pre-registered: prices, metadata_current, metadata_history.
con.execute("SELECT COUNT(DISTINCT sku) FROM metadata_current").fetchone()

# Price trajectory of a single SKU
con.execute("""
    SELECT fetched_at, regularPrice, salePrice, onSale
    FROM prices WHERE sku = '1234567' ORDER BY fetched_at
""").pl()

# Category-level average price over time
con.execute("""
    SELECT DATE_TRUNC('day', p.fetched_at) AS day,
           m.department,
           AVG(p.salePrice) AS avg_sale_price,
           COUNT(*)         AS n
    FROM prices p
    JOIN metadata_current m USING (sku)
    GROUP BY 1, 2
    ORDER BY 1, 2
""").pl()
```

### Option 3 — polars direct (simplest, no SQL)

```python
import polars as pl

prices = pl.read_parquet(
    r"C:\Users\garrie\Dropbox\Apps\AIGarpDataPipeline\prices\**\*.parquet"
)
meta = pl.read_parquet(
    r"C:\Users\garrie\Dropbox\Apps\AIGarpDataPipeline\metadata\current\metadata_current.parquet"
)
```

### Option 4 — pandas

```python
import pandas as pd
df = pd.read_parquet(
    r"C:\Users\garrie\Dropbox\Apps\AIGarpDataPipeline\prices\year=2026\month=04\day=22\2026-04-22T07-50-57Z.parquet"
)
```

### Option 5 — Excel

Data tab → **Get Data** → **From File** → **From Parquet**, pick a file.
Fine for eyeballing; slow for the `metadata_current` file (~40 cols × 56k rows).

---

## Example analyses

### "How many products went on sale in the last 24 hours?"

```python
con = duckdb_connection()
con.execute("""
    WITH latest AS (
        SELECT * FROM prices
        WHERE fetched_at = (SELECT MAX(fetched_at) FROM prices)
    ),
    prev AS (
        SELECT * FROM prices
        WHERE fetched_at = (
            SELECT MAX(fetched_at) FROM prices
            WHERE fetched_at < (SELECT MAX(fetched_at) FROM prices) - INTERVAL 1 DAY
        )
    )
    SELECT COUNT(*) AS newly_on_sale
    FROM latest l JOIN prev p USING (sku)
    WHERE l.onSale AND NOT p.onSale
""").fetchone()
```

### "Price volatility per department over the last week"

```python
con.execute("""
    SELECT m.department,
           STDDEV(p.salePrice) AS sd_price,
           COUNT(DISTINCT p.sku) AS n_skus
    FROM prices p
    JOIN metadata_current m USING (sku)
    WHERE p.fetched_at >= NOW() - INTERVAL 7 DAY
    GROUP BY 1
    ORDER BY sd_price DESC
""").pl()
```

### "Did this SKU's category change at some point?"

```python
from bbpipeline.analysis import load_metadata_at
for date in ["2026-04-21", "2026-04-22"]:
    row = load_metadata_at(date, skus=["1234567"])
    print(date, row.select(["class", "department"]).to_dicts())
```

---

## Notes on using the data

- **Timezone**: all timestamps are UTC. `fetched_at` in prices is the
  exact moment the pipeline pulled the row; `observed_at` in metadata
  is the run timestamp.
- **Null handling**: many metadata fields are null for many products
  (a DVD has no `platform`; a digital product has no `shippingWeight`).
  Treat nulls as "not applicable", not as missing-that-should-exist.
- **SKU stability**: SKUs are stable over time for the same product.
  A product going inactive drops from the active catalog; if it
  reactivates later, it reappears with the same SKU.
- **`percentSavings` is a string** (API quirk). Cast with
  `pl.col("percentSavings").cast(pl.Float64)` if you need numeric.
- **JSON columns** (`categoryPath`, `features`, `includedItemList`):
  parse with `json_extract(col, '$[0].name')` (DuckDB) or
  `pl.col("col").str.json_decode()` (polars).

---

## Sharing this data with colleagues

See top-level note — short version: Dropbox shared folder works for
small-team research; broader redistribution needs checking against
the [Best Buy Developer Agreement](https://developer.bestbuy.com/legal).
