#!/usr/bin/env python3
"""Top-N repos by average PR review latency — reads the CSV produced by
``lucille.github.pr_review_latency`` (no GitHub calls).

Given a CSV of per-PR review latencies, group by repo, compute the mean
latency (arithmetic mean of ``review_latency_hours`` over PRs that actually
received a review), and render a horizontal bar chart of the top-N slowest
repos. PRs whose base branch didn't require review, and PRs that were merged
without any qualifying review, are excluded from the mean by construction —
they either aren't in the CSV or have a null ``review_latency_hours``.

Usage:
    python -m lucille.github.pr_review_latency_top_repos \\
        --csv ~/Desktop/debris/2x2/pull_requests/2025_11_01_pr_review_latency.csv \\
        [--weeks N]        # further constrain the window (default: use whole CSV)
        [--top 10]         # how many repos to show
        [--min-prs 3]      # noise floor; repos with fewer reviewed PRs are dropped
        [--output-dir DIR] # default: same folder as the CSV
        [--dry-run]        # print table, write nothing
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from lucille.common.logging import setup_logging

logger = logging.getLogger(__name__)

REFERENCE_HOURS = 18  # match the trend chart
DEFAULT_TOP = 10
DEFAULT_MIN_PRS = 3


# ---------------------------------------------------------------------------
# Pure transforms
# ---------------------------------------------------------------------------


def load_latency_csv(path: Path) -> pd.DataFrame:
    """Load and normalize the PR-latency CSV. Missing file → SystemExit."""
    if not path.exists():
        logger.error(f"CSV not found: {path}")
        sys.exit(1)
    df = pd.read_csv(path)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    df["review_latency_hours"] = pd.to_numeric(
        df["review_latency_hours"], errors="coerce"
    )
    return df


def filter_window(df: pd.DataFrame, weeks: Optional[int]) -> pd.DataFrame:
    """If ``weeks`` given, keep only rows created within the last ``weeks`` weeks."""
    if not weeks:
        return df
    cutoff = datetime.now(timezone.utc) - timedelta(weeks=weeks)
    return df[df["created_at"] >= cutoff].copy()


def compute_repo_averages(
    df: pd.DataFrame,
    min_prs: int = DEFAULT_MIN_PRS,
) -> pd.DataFrame:
    """Per-repo mean latency across reviewed PRs.

    Only rows with a numeric ``review_latency_hours`` contribute (i.e. PRs
    that actually got a review). Repos with fewer than ``min_prs`` reviewed
    PRs are dropped as noise.

    Returns DataFrame sorted by ``mean_hours`` descending, columns:
        repo, n, mean_hours, median_hours, max_hours
    """
    if df.empty or "review_latency_hours" not in df.columns:
        return pd.DataFrame(columns=["repo", "n", "mean_hours", "median_hours", "max_hours"])
    reviewed = df[df["review_latency_hours"].notna()].copy()
    if reviewed.empty:
        return pd.DataFrame(
            columns=["repo", "n", "mean_hours", "median_hours", "max_hours"]
        )

    grouped = (
        reviewed.groupby("repo")["review_latency_hours"]
        .agg(n="count", mean_hours="mean", median_hours="median", max_hours="max")
        .reset_index()
    )
    grouped = grouped[grouped["n"] >= min_prs]
    grouped["mean_hours"] = grouped["mean_hours"].round(1)
    grouped["median_hours"] = grouped["median_hours"].round(1)
    grouped["max_hours"] = grouped["max_hours"].round(1)
    grouped["n"] = grouped["n"].astype(int)
    return grouped.sort_values("mean_hours", ascending=False).reset_index(drop=True)


def pick_top(repo_stats: pd.DataFrame, top: int) -> pd.DataFrame:
    return repo_stats.head(top).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def render_top_repos_chart(
    top_df: pd.DataFrame,
    output_path: Path,
    weeks_label: str,
    total_repos_considered: int,
) -> None:
    """Render a horizontal bar chart of the slowest repos by mean latency."""
    sns.set_style("whitegrid")
    fig, ax = plt.subplots(figsize=(12, max(4.5, 0.5 * max(len(top_df), 1) + 2)))

    if top_df.empty:
        ax.text(
            0.5,
            0.5,
            "No repos met the min-PR floor.",
            ha="center",
            va="center",
            transform=ax.transAxes,
            fontsize=13,
        )
    else:
        # Longest at the top → reverse for horizontal-bar plotting.
        display = top_df.iloc[::-1].reset_index(drop=True)
        colors = [
            "#d95f0e" if v >= REFERENCE_HOURS else "#2c7fb8"
            for v in display["mean_hours"]
        ]
        bars = ax.barh(
            display["repo"], display["mean_hours"], color=colors, edgecolor="white"
        )

        xmax = float(display["mean_hours"].max())
        ax.set_xlim(0, xmax * 1.22)

        # Annotate each bar with "Xh (n=Y)"
        for bar, mean_h, n in zip(bars, display["mean_hours"], display["n"]):
            ax.text(
                bar.get_width() + xmax * 0.01,
                bar.get_y() + bar.get_height() / 2,
                f"{mean_h:.1f}h  (n={n})",
                va="center",
                ha="left",
                fontsize=10,
                color="#333333",
            )

        # 18h reference line
        ax.axvline(REFERENCE_HOURS, color="#888888", linestyle=":", linewidth=1.2)
        ax.text(
            REFERENCE_HOURS,
            len(display) - 0.4,
            f" {REFERENCE_HOURS}h reference",
            ha="left",
            va="top",
            fontsize=9,
            color="#666666",
        )

    ax.set_xlabel("Mean hours from PR open → first review", fontsize=12)
    ax.set_ylabel("")
    ax.set_title(
        f"Slowest repos by mean PR review latency ({weeks_label})",
        fontsize=14,
        fontweight="bold",
        pad=12,
    )
    ax.text(
        0.0,
        -0.14,
        f"Shown: top {len(top_df)} of {total_repos_considered} repos above the min-PR floor.",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=10,
        color="#555555",
    )

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Top-repos chart saved: {output_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Chart the top-N slowest repos by mean PR review latency, "
            "reading the CSV produced by lucille.github.pr_review_latency."
        )
    )
    p.add_argument(
        "--csv",
        type=Path,
        required=True,
        help="Path to a pr_review_latency CSV (from the sibling command).",
    )
    p.add_argument(
        "--weeks",
        type=int,
        default=None,
        help="Further restrict to PRs created within the last N weeks "
        "(default: use the CSV's full window).",
    )
    p.add_argument(
        "--top",
        type=int,
        default=DEFAULT_TOP,
        help=f"How many repos to show (default: {DEFAULT_TOP}).",
    )
    p.add_argument(
        "--min-prs",
        type=int,
        default=DEFAULT_MIN_PRS,
        help=f"Drop repos with fewer than this many reviewed PRs "
        f"(default: {DEFAULT_MIN_PRS}).",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory (default: same folder as --csv).",
    )
    p.add_argument("--dry-run", action="store_true", help="Print table; write nothing.")
    p.add_argument("--verbose", "-v", action="store_true", help="DEBUG logging.")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    setup_logging(verbose=args.verbose)

    df = load_latency_csv(args.csv)
    logger.info(f"Loaded {len(df)} rows from {args.csv}")

    windowed = filter_window(df, args.weeks)
    if args.weeks:
        logger.info(f"After --weeks={args.weeks} filter: {len(windowed)} rows")

    repo_stats = compute_repo_averages(windowed, min_prs=args.min_prs)
    total = len(repo_stats)
    top = pick_top(repo_stats, args.top)

    if args.weeks:
        weeks_label = f"last {args.weeks} weeks"
    else:
        # Infer window from CSV min/max.
        if not windowed.empty and windowed["created_at"].notna().any():
            lo = windowed["created_at"].min().date()
            hi = windowed["created_at"].max().date()
            weeks_label = f"{lo} → {hi}"
        else:
            weeks_label = "CSV window"

    if args.dry_run or top.empty:
        if top.empty:
            print("No repos met the min-PR floor.")
        else:
            print(f"\nTop {len(top)} repos by mean review latency ({weeks_label}):")
            print(top.to_string(index=False))
        if args.dry_run:
            print("\n[dry-run] No files written.")
            return

    output_dir = args.output_dir or args.csv.parent
    timestamp = datetime.now().strftime("%Y_%m_%d")
    png_path = output_dir / f"{timestamp}_pr_review_latency_top_repos.png"
    render_top_repos_chart(top, png_path, weeks_label, total_repos_considered=total)

    print(f"\nDone. Top {len(top)} of {total} repos charted.")
    print(f"PNG: {png_path}")


if __name__ == "__main__":
    main()
