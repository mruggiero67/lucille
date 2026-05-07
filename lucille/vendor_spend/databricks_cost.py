"""
Databricks billable-usage fetcher.

Endpoint:
    GET {accounts_host}/api/2.0/accounts/{account_id}/usage/download
        ?start_month=YYYY-MM&end_month=YYYY-MM

The CSV schema varies by Databricks account vintage:

* **New schema** (post-2023 accounts): includes ``usage_quantity`` and
  ``usage_unit_price`` directly. We sum ``usage_quantity * usage_unit_price``.
* **Legacy schema** (older accounts, the one this account currently returns):
  has ``dbus``, ``sku``, ``timestamp`` but **no price column**. We multiply
  ``dbus`` by a per-SKU price provided via ``cfg.sku_prices`` to derive USD.
  Missing SKUs surface as a clear error so spend isn't silently undercounted.

Whole-month granularity at the API level means we may pull a little extra
and trim in code.
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import date
from typing import Iterable

import requests

from lucille.vendor_spend.config import DatabricksConfig
from lucille.vendor_spend.databricks_auth import resolve_bearer_token
from lucille.vendor_spend.weekly_buckets import to_date

logger = logging.getLogger(__name__)


# --- New-schema columns -----------------------------------------------------
COL_USAGE_DATE = "usage_date"
COL_USAGE_QUANTITY = "usage_quantity"
PRICE_COLUMN_CANDIDATES = (
    "usage_unit_price",      # newer schema, effective list price per unit
    "list_price",            # also newer-schema variant
)

# --- Legacy-schema columns --------------------------------------------------
LEGACY_COL_TIMESTAMP = "timestamp"
LEGACY_COL_DBUS = "dbus"
LEGACY_COL_SKU = "sku"
LEGACY_REQUIRED_COLUMNS = {LEGACY_COL_TIMESTAMP, LEGACY_COL_DBUS, LEGACY_COL_SKU}


def _is_legacy_schema(fieldnames: Iterable[str]) -> bool:
    return LEGACY_REQUIRED_COLUMNS.issubset(set(fieldnames or []))


def _verify_csv_response(response: requests.Response) -> None:
    """
    Raise a useful error if Databricks returned something other than CSV.

    The billable-usage endpoint returns 200 + HTML when the URL is wrong or
    the caller is unauthenticated (auth-redirected to a login page); without
    this guard we'd try to parse HTML as CSV and produce a confusing error.
    """
    content_type = (response.headers.get("Content-Type") or "").lower()
    body_head = (response.text or "")[:80].lstrip().lower()
    looks_like_html = body_head.startswith("<!doctype") or body_head.startswith("<html")
    if "text/csv" in content_type or content_type.startswith("application/csv"):
        return
    if looks_like_html or "text/html" in content_type:
        raise RuntimeError(
            "Databricks billable-usage endpoint returned HTML instead of CSV. "
            "This usually means the URL path is wrong or the bearer token is "
            "not account-admin scoped (the request was redirected to a login "
            f"page). URL={response.url!r}, status={response.status_code}, "
            f"content-type={content_type!r}."
        )
    # Unknown but non-HTML payload — let the CSV parser try; if it's empty or
    # malformed it'll surface there. Don't be over-strict about content-type.
    if not content_type and body_head.startswith("usage_"):
        return


# ---- pure ------------------------------------------------------------------

def _months_covering(start: date, end: date) -> tuple[str, str]:
    """Return (start_month, end_month) as ``YYYY-MM`` strings covering the range."""
    if end < start:
        raise ValueError("end must be >= start")
    return (start.strftime("%Y-%m"), end.strftime("%Y-%m"))


def _pick_price_column(fieldnames: Iterable[str]) -> str:
    fields = set(fieldnames or [])
    for c in PRICE_COLUMN_CANDIDATES:
        if c in fields:
            return c
    raise ValueError(
        f"Databricks CSV missing a recognised price column "
        f"(looked for {PRICE_COLUMN_CANDIDATES}, got {sorted(fields)})"
    )


def parse_billable_usage_csv(
    csv_text: str,
    start: date,
    end: date,
    *,
    sku_prices: dict[str, float] | None = None,
) -> list[tuple[date, float]]:
    """
    Parse a billable-usage CSV body and return ``(usage_date, usd_cost)`` rows
    restricted to ``[start, end]`` inclusive. Auto-detects new vs legacy schema.
    """
    reader = csv.DictReader(io.StringIO(csv_text))
    if not reader.fieldnames:
        return []
    if _is_legacy_schema(reader.fieldnames):
        return _parse_legacy(reader, start, end, sku_prices or {})
    return _parse_new(reader, start, end)


def _parse_new(reader, start, end) -> list[tuple[date, float]]:
    price_col = _pick_price_column(reader.fieldnames)
    rows: list[tuple[date, float]] = []
    for raw in reader:
        if not raw.get(COL_USAGE_DATE):
            continue
        d = to_date(raw[COL_USAGE_DATE])
        if d < start or d > end:
            continue
        try:
            qty = float(raw.get(COL_USAGE_QUANTITY) or 0.0)
            price = float(raw.get(price_col) or 0.0)
        except ValueError:
            logger.warning("Skipping malformed Databricks row: %s", raw)
            continue
        rows.append((d, qty * price))
    return rows


def _parse_legacy(
    reader,
    start: date,
    end: date,
    sku_prices: dict[str, float],
) -> list[tuple[date, float]]:
    if not sku_prices:
        raise RuntimeError(
            "Databricks returned the legacy CSV schema (dbus / sku / timestamp) "
            "which has no price column. Add a `databricks.sku_prices` map to "
            "~/bin/vendor_spend.yaml giving USD-per-DBU for each SKU you use, e.g. "
            "`STANDARD_ALL_PURPOSE_COMPUTE: 0.55`."
        )
    rows: list[tuple[date, float]] = []
    missing_skus: set[str] = set()
    for raw in reader:
        ts = raw.get(LEGACY_COL_TIMESTAMP)
        if not ts:
            continue
        try:
            d = to_date(ts)
        except (ValueError, TypeError):
            logger.warning("Skipping Databricks row with bad timestamp: %s", ts)
            continue
        if d < start or d > end:
            continue
        sku = (raw.get(LEGACY_COL_SKU) or "").strip()
        if sku not in sku_prices:
            missing_skus.add(sku)
            continue
        try:
            dbus = float(raw.get(LEGACY_COL_DBUS) or 0.0)
        except ValueError:
            logger.warning("Skipping Databricks row with bad dbus: %s", raw)
            continue
        rows.append((d, dbus * sku_prices[sku]))
    if missing_skus:
        raise RuntimeError(
            "Databricks legacy CSV contains SKUs with no price in "
            f"databricks.sku_prices: {sorted(missing_skus)}. Add USD-per-DBU "
            "entries for each (or set them to 0.0 to ignore)."
        )
    return rows


# ---- side-effecting --------------------------------------------------------

def fetch_raw(
    cfg: DatabricksConfig,
    start: date,
    end: date,
    *,
    session: requests.Session | None = None,
    timeout: float = 120.0,
) -> str:
    """Download the billable-usage CSV body covering the months containing [start, end]."""
    s = session or requests.Session()
    token = resolve_bearer_token(cfg, session=s)
    start_month, end_month = _months_covering(start, end)
    url = (
        f"{cfg.accounts_host.rstrip('/')}"
        f"/api/2.0/accounts/{cfg.account_id}/usage/download"
    )
    params = {"start_month": start_month, "end_month": end_month}
    headers = {"Authorization": f"Bearer {token}", "Accept": "text/csv"}
    logger.info(
        "Databricks: GET %s start_month=%s end_month=%s", url, start_month, end_month
    )
    r = s.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    _verify_csv_response(r)
    return r.text


def fetch_daily_costs(
    cfg: DatabricksConfig,
    start: date,
    end: date,
    *,
    session: requests.Session | None = None,
) -> list[tuple[date, float]]:
    """End-to-end fetch + parse."""
    csv_text = fetch_raw(cfg, start, end, session=session)
    rows = parse_billable_usage_csv(
        csv_text, start, end, sku_prices=cfg.sku_prices
    )
    logger.info("Databricks: parsed %d in-range usage rows", len(rows))
    return rows
