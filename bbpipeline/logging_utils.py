"""Per-run summary JSON for observability."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone


@dataclass
class RunLog:
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime | None = None
    status: str = "running"
    api_calls: int = 0
    products_fetched: int = 0
    prices_rows: int = 0
    metadata_changed_rows: int = 0
    prices_path: str | None = None
    prices_bytes: int = 0
    metadata_history_path: str | None = None
    metadata_history_bytes: int = 0
    hash_index_bytes: int = 0
    metadata_current_bytes: int = 0
    errors: list[str] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, default=str)

    @property
    def duration_seconds(self) -> float | None:
        if self.finished_at is None:
            return None
        return (self.finished_at - self.started_at).total_seconds()
