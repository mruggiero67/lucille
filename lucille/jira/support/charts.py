"""Chart helpers for weekly SUP analyses."""

from __future__ import annotations

import logging
from typing import Optional, Sequence, Tuple

import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)


def create_weekly_bar_chart(
    weeks: Sequence[str],
    values: Sequence[float],
    *,
    output_path: str,
    color: str,
    ylabel: str,
    title: str,
    bar_labels: Optional[Sequence[str]] = None,
    bar_label_fontsize: int = 9,
    bar_label_fontweight: str = "normal",
    y_integer: bool = False,
    figsize: Tuple[float, float] = (12, 6),
) -> None:
    """Render a weekly bar chart to ``output_path``.

    Args:
        weeks: X-axis category labels (one per bar).
        values: Bar heights.
        output_path: Where to write the PNG.
        color: Bar fill color (hex or named).
        ylabel: Y-axis label.
        title: Chart title.
        bar_labels: Optional pre-formatted text to place above each bar; must
            be the same length as ``weeks`` if given.
        bar_label_fontsize: Font size for the per-bar text labels.
        bar_label_fontweight: ``'normal'`` or ``'bold'`` for per-bar labels.
        y_integer: If True, force integer tick marks on the y-axis.
        figsize: matplotlib figsize tuple.
    """
    if not weeks:
        logger.warning("No data to chart at %s", output_path)
        return

    logger.info(f"Creating chart at {output_path}")
    fig, ax = plt.subplots(figsize=figsize)

    bars = ax.bar(list(weeks), list(values), color=color, alpha=0.8)

    ax.set_xlabel("Week", fontsize=12, fontweight="bold")
    ax.set_ylabel(ylabel, fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.grid(axis="y", alpha=0.3, linestyle="--")

    if y_integer:
        ax.yaxis.set_major_locator(plt.MaxNLocator(integer=True))

    plt.xticks(rotation=45, ha="right")

    if bar_labels is not None:
        for bar, label in zip(bars, bar_labels):
            height = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                height,
                label,
                ha="center",
                va="bottom",
                fontsize=bar_label_fontsize,
                fontweight=bar_label_fontweight,
            )

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info("Chart saved successfully")
