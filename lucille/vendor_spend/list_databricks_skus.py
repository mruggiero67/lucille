"""
One-shot helper: dump the distinct SKUs that appear in your Databricks
billable-usage CSV for the last 6 weeks, and print a starter ``sku_prices``
block to paste into ``~/bin/vendor_spend.yaml``.

Usage:
    python -m lucille.vendor_spend.list_databricks_skus
    python -m lucille.vendor_spend.list_databricks_skus --weeks 12
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import re
from datetime import date, timedelta

from lucille.vendor_spend.config import DEFAULT_CONFIG_PATH, load_config
from lucille.vendor_spend.databricks_cost import (
    LEGACY_COL_SKU,
    fetch_raw,
)
from lucille.common.logging import setup_logging


# Approximate AWS public list prices, USD per DBU, **Premium tier**.
# These are *suggestions* to seed your YAML; verify against your contract
# or https://www.databricks.com/product/pricing.
#
# Notes:
#   * Standard/Premium/Enterprise tiers price differently; values below assume
#     Premium. If you're on Standard, all-purpose is cheaper (~$0.40); if
#     Enterprise, more expensive (~$0.65 all-purpose).
#   * If your org has a negotiated discount, divide by (1 - discount).
KNOWN_LIST_PRICES_USD_PER_DBU: dict[str, float] = {
    # All-purpose / interactive compute
    "STANDARD_ALL_PURPOSE_COMPUTE":         0.40,
    "PREMIUM_ALL_PURPOSE_COMPUTE":          0.55,
    "ENTERPRISE_ALL_PURPOSE_COMPUTE":       0.65,
    "STANDARD_ALL_PURPOSE_COMPUTE_PHOTON":  0.55,
    "PREMIUM_ALL_PURPOSE_COMPUTE_PHOTON":   0.55,
    # Jobs compute
    "STANDARD_JOBS_COMPUTE":                0.10,
    "PREMIUM_JOBS_COMPUTE":                 0.15,
    "ENTERPRISE_JOBS_COMPUTE":              0.20,
    "STANDARD_JOBS_COMPUTE_PHOTON":         0.15,
    "PREMIUM_JOBS_COMPUTE_PHOTON":          0.22,
    "STANDARD_JOBS_LIGHT_COMPUTE":          0.07,
    "PREMIUM_JOBS_LIGHT_COMPUTE":           0.10,
    # DLT (Delta Live Tables)
    "STANDARD_DLT_CORE_COMPUTE":            0.20,
    "PREMIUM_DLT_CORE_COMPUTE":             0.20,
    "PREMIUM_DLT_PRO_COMPUTE":              0.25,
    "PREMIUM_DLT_ADVANCED_COMPUTE":         0.36,
    # SQL warehouses
    "PREMIUM_SQL_COMPUTE":                  0.22,
    "PREMIUM_SQL_PRO_COMPUTE":              0.55,
    "ENTERPRISE_SQL_PRO_COMPUTE":           0.55,
    # Serverless variants (higher per-DBU; in exchange you don't pay for the
    # underlying EC2 in your AWS account). The region-suffixed forms
    # (e.g. ..._US_EAST_OHIO) bill at the same rate; the lookup normalizes
    # those before matching.
    "PREMIUM_SERVERLESS_SQL_COMPUTE":         0.70,
    "ENTERPRISE_SERVERLESS_SQL_COMPUTE":      0.70,
    "PREMIUM_JOBS_SERVERLESS_COMPUTE":        0.35,
    "ENTERPRISE_JOBS_SERVERLESS_COMPUTE":     0.35,
    "PREMIUM_ALL_PURPOSE_SERVERLESS_COMPUTE": 0.75,  # medium confidence
    # Model serving / inference (DBU-billed; verify, these change often)
    "PREMIUM_SERVERLESS_REAL_TIME_INFERENCE": 0.07,  # low confidence
    "PREMIUM_ANTHROPIC_MODEL_SERVING":        0.07,  # low confidence
    # "Lakebase" managed Postgres (newer product)
    "PREMIUM_DATABASE_SERVERLESS_COMPUTE":    0.50,  # low confidence
}


# AWS region suffixes that Databricks stamps onto serverless SKUs. Pricing is
# region-independent for these SKUs, so we strip the suffix before lookup.
# Pattern: _<CONTINENT>_<DIRECTION>(_<CITY>)? at end of string, e.g.:
#   _US_EAST_OHIO   _US_WEST_OREGON   _EU_WEST_IRELAND   _AP_SOUTHEAST_SYDNEY
_REGION_SUFFIX_RE = re.compile(
    r"_(US|EU|AP|CA|SA|AF|ME)_[A-Z]+(?:_[A-Z]+)?$"
)

# Photon-engine variant suffix forms we've seen in the wild.
# Both `_(PHOTON)` (parenthesized, what shows up in the legacy CSV) and the
# bare `_PHOTON` form normalize to a single canonical key.
_PHOTON_PAREN_RE = re.compile(r"_\(PHOTON\)$")


def normalize_sku_for_lookup(sku: str) -> str:
    """
    Map a raw SKU string from the billable-usage CSV to the canonical key we
    use in ``KNOWN_LIST_PRICES_USD_PER_DBU``.

    Pure: idempotent, no I/O.
    Examples:
        PREMIUM_SQL_PRO_COMPUTE_US_EAST_OHIO  -> PREMIUM_SQL_PRO_COMPUTE
        PREMIUM_JOBS_COMPUTE_(PHOTON)         -> PREMIUM_JOBS_COMPUTE_PHOTON
        PREMIUM_ALL_PURPOSE_COMPUTE           -> PREMIUM_ALL_PURPOSE_COMPUTE
    """
    s = (sku or "").strip()
    # Strip region suffix first so any trailing _(PHOTON) is exposed at end
    # of string for the Photon regex.
    s = _REGION_SUFFIX_RE.sub("", s)
    s = _PHOTON_PAREN_RE.sub("_PHOTON", s)
    return s


def lookup_suggested_price(sku: str) -> float | None:
    """Return a known list price for ``sku``, or None if we don't have one."""
    # Try the raw SKU first (for entries that include suffixes verbatim),
    # then fall back to the normalized form.
    if sku in KNOWN_LIST_PRICES_USD_PER_DBU:
        return KNOWN_LIST_PRICES_USD_PER_DBU[sku]
    return KNOWN_LIST_PRICES_USD_PER_DBU.get(normalize_sku_for_lookup(sku))

setup_logging()
logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", default=DEFAULT_CONFIG_PATH)
    p.add_argument("--weeks", type=int, default=6)
    args = p.parse_args(argv)

    cfg = load_config(args.config)
    end = date.today()
    start = end - timedelta(weeks=args.weeks)
    csv_text = fetch_raw(cfg.databricks, start, end)

    reader = csv.DictReader(io.StringIO(csv_text))
    skus = sorted(
        {(r.get(LEGACY_COL_SKU) or "").strip() for r in reader if r.get(LEGACY_COL_SKU)}
    )

    logger.info(
        "Found %d distinct SKUs in the last %d weeks of usage", len(skus), args.weeks
    )
    suggestions = {s: lookup_suggested_price(s) for s in skus}
    known = sum(1 for v in suggestions.values() if v is not None)
    unknown = len(skus) - known
    logger.info(
        "Suggested prices known for %d/%d SKUs (%d need manual fill-in)",
        known, len(skus), unknown,
    )

    print()
    print("# Paste this under `databricks:` in ~/bin/vendor_spend.yaml.")
    print("# Suggested values are AWS *Premium-tier* public list prices (USD/DBU).")
    print("# Verify against your contract or https://www.databricks.com/product/pricing")
    print("# and adjust for your tier (Standard/Premium/Enterprise) and any")
    print("# negotiated discount. Lines marked UNKNOWN need manual entry.")
    print()
    print("  sku_prices:")
    width = max((len(s) for s in skus), default=0)
    for s in skus:
        price = suggestions[s]
        if price is not None:
            print(f"    {s.ljust(width)}: {price:>5.2f}")
        else:
            print(f"    {s.ljust(width)}:  0.00   # UNKNOWN \u2014 please fill in")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
