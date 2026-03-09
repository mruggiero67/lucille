#!/usr/bin/env python3
"""
Formats CFR results as a text/markdown summary suitable for Confluence or Slack.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from ..logic.cfr_rollup import CFRResult, DeploymentRecord
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from lucille.cfr.logic.cfr_rollup import CFRResult, DeploymentRecord

logger = logging.getLogger(__name__)

DORA_CONTEXT = (
    "DORA benchmark context:\n"
    "  Elite: <5%  |  High: 5-10%  |  Medium: 10-15%  |  Low: >15%"
)


def format_summary(result: CFRResult, title: Optional[str] = None) -> str:
    period = f"{result.period_start.strftime('%B %Y')}"
    if result.period_start.month != result.period_end.month:
        period = f"{result.period_start.strftime('%b %d')} – {result.period_end.strftime('%b %d, %Y')}"

    header = title or f"CFR Report — {period}"
    lines = [
        header,
        "─" * len(header),
        f"Total deployments:     {result.total_deployments}",
        f"Overall CFR:           {result.cfr_pct} ({result.cfr_fraction})",
        "",
        "By category:",
    ]

    for cat in ("agent", "human", "hybrid"):
        cat_result = result.by_category.get(cat)
        if cat_result is None:
            continue
        low_note = "  ← n<5, low confidence" if cat_result.low_confidence else ""
        cat_label = cat.capitalize() + "-only" if cat != "hybrid" else "Hybrid"
        lines.append(
            f"  {cat_label:<12} {cat_result.cfr_pct:>6} ({cat_result.cfr_fraction}){low_note}"
        )

    lines.append("")
    lines.append(DORA_CONTEXT)

    if result.low_confidence:
        lines.append("")
        lines.append(f"⚠ Low overall sample size (n={result.total_deployments}). Interpret with caution.")

    return "\n".join(lines)


def print_summary(result: CFRResult, title: Optional[str] = None) -> None:
    print(format_summary(result, title))


def write_summary(result: CFRResult, output_path: Path, title: Optional[str] = None) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(format_summary(result, title), encoding="utf-8")
    logger.info(f"CFR summary written to {output_path}")
