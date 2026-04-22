"""Daily rollup reports written to /logs/daily/YYYY-MM-DD.md.

Aggregates the day's runs and catalog state into a compact markdown
report. Called automatically at the end of each successful ingestion.

Surfaces alerts in an "Alerts" section at the top: fewer than the
expected 4 runs, errored runs, unexpected catalog-size drops, etc.
"""

from __future__ import annotations

import io
import json
from datetime import date, datetime, timezone
from typing import Protocol

import polars as pl

from . import config


class _Sink(Protocol):
    def upload_bytes(self, path: str, data: bytes) -> None: ...
    def download_bytes(self, path: str) -> bytes | None: ...
    def list_files(self, folder: str, recursive: bool = True): ...


def _runs_for_day(sink: _Sink, d: date) -> list[dict]:
    """Download and parse all run-log JSONs whose filename starts with `d`."""
    stamp = d.strftime("%Y-%m-%d")
    out: list[dict] = []
    try:
        for path in sink.list_files(config.DROPBOX_LOGS_RUNS_PREFIX, recursive=False):
            if stamp in path:
                data = sink.download_bytes(path)
                if data:
                    try:
                        out.append(json.loads(data))
                    except Exception:
                        pass
    except Exception:
        pass
    return sorted(out, key=lambda r: str(r.get("started_at", "")))


def _latest_prices_for_day(sink: _Sink, d: date) -> pl.DataFrame | None:
    prefix = (
        f"{config.DROPBOX_PRICES_PREFIX}"
        f"/year={d.year:04d}/month={d.month:02d}/day={d.day:02d}"
    )
    try:
        files = sorted(sink.list_files(prefix, recursive=False))
    except Exception:
        return None
    if not files:
        return None
    data = sink.download_bytes(files[-1])
    if data is None:
        return None
    return pl.read_parquet(io.BytesIO(data))


def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def build_daily_report(sink: _Sink, d: date) -> str:
    runs = _runs_for_day(sink, d)
    latest_prices = _latest_prices_for_day(sink, d)

    # Previous-day catalog for delta
    prev_day_prices = _latest_prices_for_day(sink, date.fromordinal(d.toordinal() - 1))

    lines: list[str] = [
        f"# AI GARP pipeline — daily rollup {d.isoformat()}",
        "",
        f"_Generated at {datetime.utcnow().isoformat(timespec='seconds')}Z_",
        "",
    ]

    # -------- Alerts --------
    alerts: list[str] = []
    n_runs = len(runs)
    n_errors = sum(1 for r in runs if r.get("status") != "ok")
    n_ok = n_runs - n_errors

    # Only alert on run-count if the day is actually over. During the
    # day, regenerating the report mid-day would otherwise fire a false
    # "not enough runs" alert every time we're called.
    utc_today = datetime.now(timezone.utc).date()
    day_is_complete = d < utc_today
    if day_is_complete and n_runs < 4:
        alerts.append(
            f"Only **{n_runs}** run(s) on {d} (expected 4 at UTC 00/06/12/18)."
        )
    if n_errors > 0:
        alerts.append(f"**{n_errors}** errored run(s) today.")
    # 2+ consecutive recent failures (any, not just today)
    recent = _runs_for_day(sink, d) + _runs_for_day(sink, date.fromordinal(d.toordinal() - 1))
    tail = recent[-3:]
    if len(tail) >= 2 and all(r.get("status") != "ok" for r in tail[-2:]):
        alerts.append("Last **2+ runs in a row** failed — investigate.")
    # SKU count drop
    if latest_prices is not None and prev_day_prices is not None:
        delta = latest_prices.height - prev_day_prices.height
        pct = 100.0 * delta / max(1, prev_day_prices.height)
        if pct < -10:
            alerts.append(
                f"Catalog dropped **{abs(pct):.1f}%** day-over-day "
                f"({prev_day_prices.height:,} → {latest_prices.height:,})."
            )

    if alerts:
        lines.append("## ⚠️ Alerts")
        for a in alerts:
            lines.append(f"- {a}")
    else:
        lines.append("## Alerts")
        lines.append("None.")
    lines.append("")

    # -------- Runs --------
    lines.append("## Runs")
    lines.append("")
    lines.append(f"- Total: **{n_runs}** (ok: {n_ok}, errors: {n_errors})")
    lines.append("")
    if runs:
        lines.append("| started_at (UTC) | status | duration | products | metadata changed | API calls |")
        lines.append("|---|---|---|---|---|---|")
        for r in runs:
            start = _parse_iso(r.get("started_at"))
            finish = _parse_iso(r.get("finished_at"))
            dur = f"{(finish - start).total_seconds():.0f}s" if start and finish else "?"
            lines.append(
                f"| {r.get('started_at','?')[:19]} "
                f"| {r.get('status','?')} "
                f"| {dur} "
                f"| {r.get('products_fetched','?'):,} "
                f"| {r.get('metadata_changed_rows','?'):,} "
                f"| {r.get('api_calls','?')} |"
            )
        lines.append("")

    # -------- Catalog health --------
    if latest_prices is not None:
        n = latest_prices.height
        on_sale = latest_prices.filter(pl.col("onSale")).height
        pct_sale = 100.0 * on_sale / n if n else 0.0
        lines.append("## Catalog (last run today)")
        lines.append("")
        lines.append(f"- SKUs: **{n:,}**")
        if prev_day_prices is not None:
            delta = n - prev_day_prices.height
            lines.append(
                f"- Day-over-day SKU change: **{delta:+,}** "
                f"({100.0 * delta / max(1, prev_day_prices.height):+.2f}%)"
            )
        lines.append(f"- On sale: **{on_sale:,}** ({pct_sale:.1f}%)")
        try:
            stats = latest_prices.select([
                pl.col("regularPrice").mean().alias("mean_regular"),
                pl.col("regularPrice").median().alias("med_regular"),
                pl.col("salePrice").mean().alias("mean_sale"),
            ]).row(0, named=True)
            lines.append(
                f"- Regular price: mean ${stats['mean_regular']:.2f}, "
                f"median ${stats['med_regular']:.2f}"
            )
            lines.append(f"- Sale price mean: ${stats['mean_sale']:.2f}")
        except Exception:
            pass
        lines.append("")

    # -------- Metadata churn --------
    total_changed = sum(int(r.get("metadata_changed_rows", 0) or 0) for r in runs)
    if total_changed:
        lines.append("## Metadata churn today")
        lines.append("")
        lines.append(f"- Total rows written to history: **{total_changed:,}**")
        lines.append("")

    return "\n".join(lines) + "\n"


def write_daily_report(sink: _Sink, d: date) -> tuple[str, int]:
    report = build_daily_report(sink, d)
    path = f"{config.DROPBOX_LOGS_DAILY_PREFIX}/{d.isoformat()}.md"
    data = report.encode("utf-8")
    sink.upload_bytes(path, data)
    return path, len(data)
