import logging
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from lucille.lead_time.buckets import assign_bucket, bucket_labels, bucket_colors

logger = logging.getLogger(__name__)

sns.set_style("whitegrid")


def render_distribution_chart(
    df: pd.DataFrame,
    output_path: Path,
    date_label: str = "",
) -> None:
    """
    Render and save a bar chart of lead time distribution across buckets.
    df must have: lead_time_hours (valid floats), deployment_id.
    """
    labels = bucket_labels()
    colors = bucket_colors()

    df = df.copy()
    df["bucket"] = df["lead_time_hours"].apply(assign_bucket)
    counts = df["bucket"].value_counts().reindex(labels, fill_value=0)
    total = int(counts.sum())
    pcts = (counts / total * 100) if total > 0 else counts * 0.0

    n_deployments = int(df["deployment_id"].nunique()) if "deployment_id" in df.columns else "?"
    pct_within_7d = round(100.0 * int((df["lead_time_hours"] <= 168).sum()) / total, 1) if total > 0 else 0

    fig, ax = plt.subplots(figsize=(12, 8))
    x = list(range(len(labels)))
    bars = ax.bar(x, counts, color=colors, edgecolor="white", linewidth=0.8)

    for bar, pct in zip(bars, pcts):
        if bar.get_height() > 0:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.5,
                f"{pct:.1f}%",
                ha="center", va="bottom", fontsize=11, fontweight="bold",
            )

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=12)
    ax.set_ylabel("Number of Changes", fontsize=12)
    ax.set_xlabel("Lead Time Bucket", fontsize=12)

    title = "Lead Time Distribution"
    if date_label:
        title += f" - {date_label}"
    ax.set_title(title, fontsize=14, fontweight="bold", pad=16)
    ax.text(
        0.0, -0.12,
        f"Analysis of {total:,} changes across {n_deployments} deployments",
        transform=ax.transAxes, ha="left", va="top", fontsize=11, color="#555555",
    )
    ax.text(
        0.98, 0.97,
        f"{pct_within_7d}% of changes delivered within 7 days\n(target: 80%)",
        transform=ax.transAxes, ha="right", va="top", fontsize=11,
        bbox=dict(boxstyle="round,pad=0.4", facecolor="#f8f8f8", edgecolor="#aaaaaa"),
    )

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Distribution chart saved: {output_path}")


def render_trends_chart(
    weekly_df: pd.DataFrame,
    output_path: Path,
) -> None:
    """
    Render and save a line chart of weekly lead time trends.
    weekly_df must have: week_label, median_days, mean_days, p75_days, change_count.
    """
    if weekly_df.empty:
        logger.warning("No weekly data — skipping trends chart")
        return

    labels = weekly_df["week_label"].tolist()
    x = list(range(len(labels)))
    target_days = 7.0

    fig, ax = plt.subplots(figsize=(12, 8))

    ax.plot(x, weekly_df["median_days"].tolist(), color="#2980b9", linewidth=2,
            marker="o", markersize=5, label="Median")
    ax.plot(x, weekly_df["mean_days"].tolist(), color="#e67e22", linewidth=1.5,
            linestyle="--", marker="o", markersize=4, label="Average")
    ax.plot(x, weekly_df["p75_days"].tolist(), color="#8e44ad", linewidth=1,
            linestyle=":", marker="s", markersize=4, label="p75")

    ax.axhline(y=target_days, color="#e74c3c", linewidth=1.2, linestyle="-.",
               label="High Performer Target (7d)")

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=10)
    ax.set_ylabel("Lead Time (days)", fontsize=12)
    ax.set_xlabel("Week", fontsize=12)
    ax.set_title("Lead Time Trends - Last 12 Weeks", fontsize=14, fontweight="bold")
    ax.legend(loc="upper left", fontsize=10)
    ax.grid(alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Trends chart saved: {output_path}")
