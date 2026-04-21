"""Central configuration: paths, schemas, API constants.

Schema determined by the Phase 2 API audit (2026-04-21). See
`audit_report.json` and `notes.md` for the full field inventory.
"""

from __future__ import annotations

# ---------- Best Buy API ----------

BESTBUY_BASE_URL = "https://api.bestbuy.com/v1"
BESTBUY_MAX_PAGE_SIZE = 100
# Observed: 1.2s spacing avoids 403 "per second limit" reliably from one
# client. Keep at 1.2s with retry-on-403 in the HTTP layer.
BESTBUY_MIN_INTERVAL_SECONDS = 1.2
BESTBUY_TIMEOUT_SECONDS = 30.0

# Scope: active catalog only (~56k SKUs). `active=false` is the 2.4M-SKU
# discontinued archive; not relevant for live-price panel / GARP.
BESTBUY_PRODUCT_PATH = "/products(active=true)"

# ---------- Dropbox paths (relative to App-folder root) ----------

DROPBOX_PRICES_PREFIX = "/prices"
DROPBOX_METADATA_CURRENT = "/metadata/current/metadata_current.parquet"
DROPBOX_METADATA_HISTORY_PREFIX = "/metadata/history"
DROPBOX_HASH_INDEX = "/metadata/index/hash_index.parquet"
DROPBOX_LOGS_RUNS_PREFIX = "/logs/runs"
DROPBOX_LOGS_DAILY_PREFIX = "/logs/daily"

# ---------- Schemas ----------

# Prices table — written append-only, one parquet per run.
# Includes numeric behavioral signals (reviews, short-term sales rank)
# that are better stored as a time series than in SCD2 metadata.
PRICE_FIELDS: tuple[str, ...] = (
    "sku",
    "regularPrice",
    "salePrice",
    "onSale",
    "dollarSavings",
    "percentSavings",
    "onlineAvailability",
    "inStoreAvailability",
    "orderable",
    "clearance",
    "priceUpdateDate",
    "customerReviewAverage",
    "customerReviewCount",
    "bestSellingRank",
    "salesRankShortTerm",
)

# Metadata table — SCD2, only rows that change between runs.
# Structural identity + attributes. Excludes high-churn behavioral
# fields (they live in PRICE_FIELDS) and heavy/irrelevant fields
# (images, URLs, HTML variants, carrier/plan/protection-plan data).
METADATA_FIELDS: tuple[str, ...] = (
    # identity
    "sku", "productId", "bestBuyItemId",
    "name", "manufacturer", "modelNumber", "upc",
    # taxonomy
    "categoryPath", "class", "classId",
    "subclass", "subclassId",
    "department", "departmentId", "type",
    # descriptive
    "shortDescription", "features", "includedItemList",
    "color", "condition",
    # flags
    "new", "preowned", "digital", "marketplace", "freeShipping",
    # physical
    "weight", "shippingWeight",
    "height", "width", "depth",
    "format", "platform",
    # warranty
    "warrantyLabor", "warrantyParts",
    # timestamps
    "releaseDate", "itemUpdateDate", "validFrom", "validUntil",
)

# `show=` parameter value: union of price + metadata fields.
# Best Buy's API ignores unknown fields silently.
SHOW_FIELDS: str = ",".join(sorted(set(PRICE_FIELDS) | set(METADATA_FIELDS)))
