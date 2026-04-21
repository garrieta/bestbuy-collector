# CLAUDE.md — AI GARP project

Project-specific instructions. Extends the global `C:\Users\garrie\Dropbox\Claude\CLAUDE.md`.

## Purpose
Collect and maintain a panel of Best Buy product prices and metadata for revealed-preference (GARP) analysis. Full catalog, 4×/day, persistent history.

## Tools
- Python 3.11+
- `polars` + `pyarrow` + `duckdb` for data (prefer over pandas for bulk operations)
- `httpx` + `tenacity` for HTTP
- `dropbox` SDK for storage I/O
- `python-dotenv` for local `.env` loading

## Secret hygiene (non-negotiable)
- No API keys, tokens, or secrets in source. Ever.
- GitHub Secrets for CI; gitignored `.env` for local development.
- No hardcoded fallbacks like `os.environ.get("KEY", "literal_key_here")`.
- Read with `os.environ["KEY"]` (hard fail) in production code paths.

## Storage conventions
- Backend: Dropbox App folder (`AIGarpDataPipeline`), written via Dropbox API from GitHub Actions.
- Format: partitioned parquet. Prices append-only per run. Metadata SCD2 diff-only via content-hash index.
- Dropbox-API paths are app-folder-relative (start with `/`, e.g. `/prices/year=2026/…`).
- Local synced path: `C:\Users\garrie\Dropbox\Apps\AIGarpDataPipeline\`.

## Code conventions
- Type hints on function signatures.
- Short module-level docstring explaining purpose.
- Prefer pure functions; isolate side effects to `run.py` and `storage.py`.
- Use `pathlib.Path`, not string concatenation.
- No comments that restate what code does — only why.

## Plan
See `C:\Users\garrie\Dropbox\Claude\tasks\todo.md`, section
"Best Buy Price & Metadata Collection Pipeline (AI GARP)".
