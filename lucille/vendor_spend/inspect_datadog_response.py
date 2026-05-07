"""
Diagnostic: fetch one short window from the Datadog estimated_cost endpoint
and dump the response structure so we can see exactly what the parser is
summing.

Usage:
    python -m lucille.vendor_spend.inspect_datadog_response
    python -m lucille.vendor_spend.inspect_datadog_response --days 3
"""

from __future__ import annotations

import argparse
import json
import logging
from collections import Counter
from datetime import date, timedelta

from lucille.vendor_spend.config import DEFAULT_CONFIG_PATH, load_config
from lucille.vendor_spend.datadog_cost import fetch_raw

logging.basicConfig(
    format="%(levelname)-10s %(asctime)s %(filename)s %(lineno)d %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", default=DEFAULT_CONFIG_PATH)
    p.add_argument("--days", type=int, default=3, help="Window size in days (default 3).")
    args = p.parse_args(argv)

    cfg = load_config(args.config)
    end = date.today() - timedelta(days=4)  # estimated cost lags ~72h
    start = end - timedelta(days=args.days - 1)

    payload = fetch_raw(cfg.datadog, start, end)
    data = payload.get("data", [])

    print()
    print(f"=== Window: {start} .. {end} ({args.days} days) ===")
    print(f"Total items in `data`: {len(data)}")
    print()

    # First three items, full structure
    print("--- First 3 items (verbatim) ---")
    for i, item in enumerate(data[:3]):
        print(f"[{i}]")
        print(json.dumps(item, indent=2, default=str))
        print()

    # Group items by date and by attribute keys to see what dimensions exist
    if data:
        attr_key_counter: Counter = Counter()
        date_counter: Counter = Counter()
        product_counter: Counter = Counter()
        org_counter: Counter = Counter()
        sum_total_cost = 0.0

        for item in data:
            attrs = item.get("attributes") or {}
            attr_key_counter.update(attrs.keys())
            d = attrs.get("date") or attrs.get("month")
            if d:
                date_counter[str(d)[:10]] += 1
            for k in ("product_name", "charge_type", "charge"):
                if k in attrs:
                    product_counter[f"{k}={attrs[k]}"] += 1
            if "org_name" in attrs:
                org_counter[attrs["org_name"]] += 1
            try:
                sum_total_cost += float(attrs.get("total_cost") or 0)
            except (TypeError, ValueError):
                pass

        print("--- Attribute keys present (key -> count of items) ---")
        for k, n in attr_key_counter.most_common():
            print(f"  {k}: {n}")
        print()

        print("--- Items per date ---")
        for d, n in sorted(date_counter.items()):
            print(f"  {d}: {n} items")
        print()

        if product_counter:
            print("--- Product / charge dimensions ---")
            for k, n in product_counter.most_common(20):
                print(f"  {k}: {n}")
            if len(product_counter) > 20:
                print(f"  ... and {len(product_counter) - 20} more")
            print()

        if org_counter:
            print("--- Distinct org_name values ---")
            for k, n in org_counter.most_common():
                print(f"  {k}: {n}")
            print()

        print(f"--- Sum of attributes.total_cost across ALL items: ${sum_total_cost:,.2f} ---")
        print(f"    (this is what our parser currently produces)")
        print(f"    Expected ballpark: ~${(21627 / 30) * args.days:,.0f}"
              f"  (using your $21,627/mo example as reference)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
