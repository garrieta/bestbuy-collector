# Best Buy Data Collector

Continuous snapshots of Best Buy's active catalog (~56k SKUs), 4×/day,
for revealed-preference (GARP) analysis in the AI GARP project.

- Full-catalog sweep every ~6 hours via Best Buy's Products API
- Runs on GitHub Actions — no local infrastructure required
- Data lands in Dropbox as partitioned parquet (append-only prices, SCD2 metadata)
- Analysis-ready loaders in Python and SQL (DuckDB)

## Quickstart (for analysis)

Assuming you have Dropbox access to `Apps/AIGarpDataPipeline/` and the
repo cloned locally:

```bash
pip install -r requirements.txt
python scripts/demo_analysis.py
```

Then open `notebooks/explore_panel.ipynb` for a guided tour.

### The main loaders

```python
from bbpipeline.analysis import (
    load_prices_latest, load_prices,
    load_metadata_current, load_metadata_at,
    load_panel, load_panel_historical,
    duckdb_connection, runs_summary,
)

latest = load_prices_latest()                                 # current snapshot
hist   = load_prices(start="2026-04-20",
                     skus=["1234567", "8901234"])             # time/SKU filters
meta   = load_metadata_current()                              # current attrs
snap   = load_metadata_at("2026-04-22T00:00:00Z",
                          restrict_to_active=True)            # SCD2 as-of
panel  = load_panel(start="2026-04-21")                       # prices × current meta
panel2 = load_panel_historical(start="2026-04-21")            # prices × point-in-time meta
```

All return polars DataFrames. See `DATA.md` for full schemas and more
examples. Every function has a docstring — `help(load_panel_historical)`.

### Exporting to CSV

For collaborators who prefer CSV over parquet:

```bash
python scripts/export_csv.py metadata-current
python scripts/export_csv.py prices --start 2026-04-21 --end 2026-04-30
python scripts/export_csv.py panel  --start 2026-04-21
```

Files land in `./csv/`. Nested columns are JSON-serialised so the
output is valid CSV.

## Key files

| File | What it is |
|---|---|
| [`DATA.md`](DATA.md) | **Full data reference** — schemas, layout, query examples. Start here. |
| [`notebooks/explore_panel.ipynb`](notebooks/explore_panel.ipynb) | Starter notebook: health, distributions, SKU trajectories, GARP hook |
| [`bbpipeline/analysis.py`](bbpipeline/analysis.py) | Analysis-ready loaders with docstrings |
| [`bbpipeline/run.py`](bbpipeline/run.py) | Collection pipeline entrypoint (runs on GitHub Actions cron) |
| [`scripts/demo_analysis.py`](scripts/demo_analysis.py) | Smoke-test that exercises every loader |
| [`scripts/dropbox_status.py`](scripts/dropbox_status.py) | List files in the Dropbox app folder + latest run log |
| [`scripts/export_csv.py`](scripts/export_csv.py) | CSV export CLI |
| [`notes.md`](notes.md) | Decisions log + Phase-2 API audit findings |

## Pipeline architecture

1. GitHub Actions cron triggers `python -m bbpipeline.run` at **00/06/12/18 UTC**.
2. The pipeline paginates through Best Buy's Products API for `(active=true)` —
   ~564 requests at ~1 req/s pacing; ~11 minutes per sweep.
3. Prices (thin, 16 cols) are written as one parquet per run under
   `/prices/year=YYYY/month=MM/day=DD/`.
4. Metadata (wide, 40 cols) is diffed against a content-hash index —
   only changed rows are written to `/metadata/history/day=DD.parquet`.
   `metadata_current/metadata_current.parquet` is overwritten each run.
5. A per-run JSON log is written to `/logs/runs/`.
6. A daily markdown rollup is written/overwritten to `/logs/daily/YYYY-MM-DD.md`
   with alert flags for errored runs, catalog-size drops, etc.
7. All artifacts land in the Dropbox App folder `/Apps/AIGarpDataPipeline/`
   via the Dropbox API — laptop uptime is **not** required.

## For maintainers

### Setting up a fresh fork / clone

1. Register a Best Buy developer API key: https://developer.bestbuy.com/
2. Create a Dropbox app: https://www.dropbox.com/developers/apps
   (**Scoped access → App folder**, not Full Dropbox)
3. Generate a refresh token:
   ```bash
   python scripts/get_dropbox_refresh_token.py
   ```
4. Copy `.env.example` → `.env` and fill in the four secrets.
5. Add the same four to GitHub **Repository Secrets** (Settings → Secrets
   and variables → Actions → Repository secrets, **not** Environment secrets):
   - `BESTBUY_API_KEY`
   - `DROPBOX_APP_KEY`
   - `DROPBOX_APP_SECRET`
   - `DROPBOX_REFRESH_TOKEN`
6. Push to your fork — Actions will start running on cron automatically.
   Trigger a one-off from the Actions tab to verify (use `max_pages=2`
   for a quick 30 s run).

### Running the pipeline locally (no Dropbox writes)

```bash
python -m bbpipeline.run --local --max-pages 2   # dry-run, 200 products
python -m bbpipeline.run --local                 # dry-run, full catalog
```

`--local` writes to `./.local_dropbox/` (gitignored) instead of Dropbox.

### Sanity / diagnostic scripts

```bash
python scripts/test_api_key.py              # verify Best Buy key
python scripts/test_dropbox_connection.py   # verify Dropbox auth + roundtrip
python scripts/api_audit.py                 # re-run the Phase-2 API audit
python scripts/dropbox_status.py            # what's currently in the app folder
```

## Data-sharing note

Best Buy API data is licensed per their
[Developer Agreement](https://developer.bestbuy.com/legal). Sharing raw
data with collaborators **within** a closed research team is usually
fine; redistributing it publicly (in a paper, dataset upload, etc.)
requires checking the agreement or obtaining explicit permission. When
in doubt, publish derived statistics rather than raw rows.
