"""File-output helpers for weekly SUP analyses."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Sequence

import pandas as pd

logger = logging.getLogger(__name__)


def save_issues_csv(
    issues_data: List[Dict],
    output_path: str,
    *,
    columns: Sequence[str],
    sort_by: str,
) -> None:
    """Write ``issues_data`` to CSV with the given column order.

    Args:
        issues_data: List of row dicts.
        output_path: Destination CSV path.
        columns: Column order for the output file. All must exist in the
            row dicts.
        sort_by: Column to sort by (descending).
    """
    logger.info(f"Saving issues CSV to {output_path}")
    df = pd.DataFrame(issues_data)
    df = df[list(columns)]
    df = df.sort_values(sort_by, ascending=False)
    df.to_csv(output_path, index=False)
    logger.info(f"CSV saved successfully with {len(df)} rows")


def save_summary_txt(lines: List[str], output_path: str) -> None:
    """Write summary lines to a plain-text file (newline-joined, trailing \\n)."""
    logger.info(f"Saving summary to {output_path}")
    Path(output_path).write_text("\n".join(lines) + "\n")
    logger.info("Summary saved successfully")
