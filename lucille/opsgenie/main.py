"""CLI + rendering for the OpsGenie alert-noise report.

Usage::

    python -m lucille.opsgenie.main \\
        --csv ~/Desktop/debris/YYYY_MM_DD_opsgenie.csv \\
        --config ~/bin/graphs.yaml \\
        [--top-n 20] [--min-fires 3]

Outputs, all timestamped, all under ``opsgenie_output_directory`` from
``graphs.yaml``:

  * ``YYYY_MM_DD_opsgenie_noise_ranked.csv`` \u2014 every alias with fires \u2265
    ``--min-fires``, sorted by fire count desc.
  * ``YYYY_MM_DD_opsgenie_noise_top_N.png`` \u2014 horizontal bar chart of the
    top-N noisiest aliases.
  * ``YYYY_MM_DD_opsgenie_noise_summary.txt`` \u2014 human-readable summary
    including the concentration numbers (top-5/10/20 share of total volume).
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Sequence

import matplotlib.pyplot as plt

from lucille.common.config import load_yaml_config
from lucille.common.logging import setup_logging
from lucille.opsgenie.io import Alert, load_alerts
from lucille.opsgenie.noise import (
    NoiseRow,
    NoiseSummary,
    coarse_alias,
    compute_noise_rows,
    filter_by_min_fires,
    summarize,
    top_n,
)

setup_logging()
logger = logging.getLogger(__name__)

DEFAULT_TOP_N = 20
DEFAULT_MIN_FIRES = 3
_MAX_LABEL_LEN = 60  # truncate long aliases in the chart y-axis


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------


def _count_raw_aliases_per_coarse_key(alerts: Sequence[Alert]) -> "dict[str, int]":
    """For each coarse key, how many distinct raw aliases collapse into it.

    Exposed as the ``raw_aliases_merged`` column in the coarse CSV so a
    reader can see the coarsening's effect at a glance (e.g. one Datadog
    ``monitor_id`` collapsing 27 per-job fragments into a single row).
    """
    buckets: "dict[str, set]" = {}
    for a in alerts:
        buckets.setdefault(coarse_alias(a.alias), set()).add(a.alias)
    return {k: len(v) for k, v in buckets.items()}


def write_ranked_csv(
    rows: Sequence[NoiseRow],
    out_path: Path,
    *,
    key_column: str = "alias",
    raw_aliases_merged: "dict[str, int] | None" = None,
) -> None:
    """Write the ranked noise report to CSV.

    Columns are chosen for what an on-call engineer would sort/filter by
    when triaging: fire count, ack rate, days active, teams, and enough
    of the message to identify what's firing.

    ``key_column`` names the first column — ``"alias"`` for the raw view
    (default) or ``"coarse_key"`` for the Datadog-collapsed view.

    When ``raw_aliases_merged`` is provided, a ``raw_aliases_merged``
    column is emitted after the key column showing how many distinct raw
    aliases each coarse key collapses. Only meaningful for the coarse
    view; leave ``None`` for the raw view.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    include_merged = raw_aliases_merged is not None
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        header = [key_column]
        if include_merged:
            header.append("raw_aliases_merged")
        header.extend([
            "sample_message",
            "fires",
            "ack_count",
            "ack_rate_pct",
            "auto_closed_no_ack",
            "auto_close_rate_pct",
            "days_active",
            "fires_per_active_day",
            "first_seen",
            "last_seen",
            "teams",
        ])
        w.writerow(header)
        for r in rows:
            row_out = [r.alias]
            if include_merged:
                row_out.append(raw_aliases_merged.get(r.alias, 1))
            row_out.extend([
                r.sample_message,
                r.fires,
                r.ack_count,
                f"{r.ack_rate * 100:.1f}",
                r.auto_closed_no_ack,
                f"{r.auto_close_rate * 100:.1f}",
                r.days_active,
                f"{r.fires_per_active_day:.2f}",
                r.first_seen.isoformat(),
                r.last_seen.isoformat(),
                r.teams,
            ])
            w.writerow(row_out)
    logger.info(f"Wrote ranked CSV: {out_path} ({len(rows)} rows)")


# ---------------------------------------------------------------------------
# Chart
# ---------------------------------------------------------------------------


def _short_label(alias: str, sample_message: str) -> str:
    """Return a chart-friendly label for a monitor.

    OpsGenie aliases from Datadog look like
    ``org_id:305115|metric:X|monitor_id:Y|#service:z`` \u2014 unreadable on a
    chart axis. Prefer the first line of the alert message when it's
    non-empty and short enough; fall back to a truncated alias.
    """
    first_line = (sample_message or "").splitlines()[0].strip() if sample_message else ""
    if first_line and len(first_line) <= _MAX_LABEL_LEN:
        return first_line
    if first_line:
        return first_line[: _MAX_LABEL_LEN - 1] + "\u2026"
    if len(alias) <= _MAX_LABEL_LEN:
        return alias
    return alias[: _MAX_LABEL_LEN - 1] + "\u2026"


def render_top_n_chart(
    rows: Sequence[NoiseRow],
    out_path: Path,
    total_alerts: int,
    window_days: int,
) -> None:
    """Render a horizontal bar chart of the top-N noisiest aliases.

    Bars are colored by ack rate: red for aliases with < 20% ack rate
    (pure noise), gray otherwise. The color rule is a hint for triage,
    not a hard classification.
    """
    if not rows:
        logger.info("No rows to chart; skipping PNG output.")
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)

    labels = [_short_label(r.alias, r.sample_message) for r in rows]
    fires = [r.fires for r in rows]
    colors = ["#c0392b" if r.ack_rate < 0.20 else "#7f8c8d" for r in rows]

    # Taller figure per row so labels don't collide.
    fig, ax = plt.subplots(figsize=(12, max(4, 0.4 * len(rows))))
    y_pos = list(range(len(rows)))
    ax.barh(y_pos, fires, color=colors)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=8)
    ax.invert_yaxis()  # highest count on top
    ax.set_xlabel("Fires in window")

    subtitle_bits = [f"{total_alerts:,} total alerts"]
    if window_days > 0:
        subtitle_bits.append(f"{window_days}-day window")
    subtitle_bits.append("red = <20% ack rate")
    ax.set_title(
        f"Top {len(rows)} noisiest OpsGenie aliases\n"
        + "  \u00b7  ".join(subtitle_bits),
        fontsize=11,
    )

    # Annotate each bar with its raw count on the right.
    for i, count in enumerate(fires):
        ax.text(count, i, f"  {count}", va="center", fontsize=8)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Wrote chart: {out_path}")


# ---------------------------------------------------------------------------
# Summary text file
# ---------------------------------------------------------------------------


def render_summary(
    summary: NoiseSummary,
    rows: Sequence[NoiseRow],
    out_path: Path,
    *,
    coarse_summary: "NoiseSummary | None" = None,
    coarse_rows: "Sequence[NoiseRow] | None" = None,
) -> None:
    """Write a plain-text summary suitable for pasting into a chat/email.

    When ``coarse_summary`` and ``coarse_rows`` are provided, a second
    section is appended showing the same Pareto numbers and top-10 list
    computed against the Datadog-collapsed view. This is where the
    coarse grouping earns its keep: on real data the raw top-5 is often
    ~3% of volume (aliases fragment per instance) while the coarse
    top-5 climbs sharply because same-monitor fragments merge.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    lines: List[str] = []
    lines.append("OpsGenie alert-noise summary")
    lines.append("=" * 60)
    if summary.window_start and summary.window_end:
        span_days = (summary.window_end - summary.window_start).days + 1
        lines.append(
            f"Window:                    "
            f"{summary.window_start.date()} \u2192 {summary.window_end.date()} "
            f"({span_days} days)"
        )
    lines.append(f"Total alerts:              {summary.total_alerts:,}")
    lines.append(f"Unique monitors (aliases): {summary.unique_aliases:,}")
    if coarse_summary is not None:
        lines.append(
            f"Unique monitors (coarse):  {coarse_summary.unique_aliases:,}  "
            f"(after collapsing Datadog monitor_id fragments)"
        )
    lines.append(f"Overall ack rate:          {summary.overall_ack_rate * 100:.1f}%")
    lines.append(
        f"Auto-closed w/o ack:       "
        f"{summary.overall_auto_close_rate * 100:.1f}% "
        f"(the noise floor)"
    )
    lines.append("")
    lines.append("Concentration (Pareto check) \u2014 raw aliases:")
    lines.append(f"  Top 5 aliases  \u2192 {summary.top_5_share * 100:.1f}% of all volume")
    lines.append(f"  Top 10 aliases \u2192 {summary.top_10_share * 100:.1f}% of all volume")
    lines.append(f"  Top 20 aliases \u2192 {summary.top_20_share * 100:.1f}% of all volume")

    if coarse_summary is not None:
        lines.append("")
        lines.append("Concentration (Pareto check) \u2014 coarse (Datadog monitor_id):")
        lines.append(
            f"  Top 5 monitors  \u2192 {coarse_summary.top_5_share * 100:.1f}% of all volume"
        )
        lines.append(
            f"  Top 10 monitors \u2192 {coarse_summary.top_10_share * 100:.1f}% of all volume"
        )
        lines.append(
            f"  Top 20 monitors \u2192 {coarse_summary.top_20_share * 100:.1f}% of all volume"
        )
    lines.append("")

    if rows:
        lines.append(f"Top {min(len(rows), 10)} noisiest monitors (raw):")
        lines.append("-" * 60)
        for i, r in enumerate(rows[:10], start=1):
            label = _short_label(r.alias, r.sample_message)
            lines.append(
                f"  {i:2}. {r.fires:4} fires  "
                f"ack {r.ack_rate * 100:5.1f}%  "
                f"days {r.days_active:3}  "
                f"{label}"
            )

    if coarse_rows:
        lines.append("")
        lines.append(f"Top {min(len(coarse_rows), 10)} noisiest monitors (coarse):")
        lines.append("-" * 60)
        for i, r in enumerate(coarse_rows[:10], start=1):
            label = _short_label(r.alias, r.sample_message)
            lines.append(
                f"  {i:2}. {r.fires:4} fires  "
                f"ack {r.ack_rate * 100:5.1f}%  "
                f"days {r.days_active:3}  "
                f"{label}"
            )

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    logger.info(f"Wrote summary: {out_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Rank OpsGenie alerts by fire count to find noisy monitors."
    )
    p.add_argument("--csv", required=True, type=Path,
                   help="Path to the OpsGenie CSV export.")
    p.add_argument("--config", required=True, type=Path,
                   help="Path to graphs.yaml (for output directory).")
    p.add_argument("--top-n", type=int, default=DEFAULT_TOP_N,
                   help=f"How many aliases to chart (default {DEFAULT_TOP_N}).")
    p.add_argument("--min-fires", type=int, default=DEFAULT_MIN_FIRES,
                   help=(
                       "Minimum fires to appear in the ranked CSV "
                       f"(default {DEFAULT_MIN_FIRES})."
                   ))
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)

    if not args.csv.exists():
        logger.error(f"CSV not found: {args.csv}")
        return 2

    config = load_yaml_config(args.config)
    out_dir = Path(config.get("opsgenie_output_directory", "./output"))
    date_prefix = datetime.now().strftime("%Y_%m_%d")

    logger.info(f"Loading alerts from {args.csv}")
    alerts = load_alerts(args.csv)
    logger.info(f"Loaded {len(alerts):,} alerts")

    # Raw view: group by OpsGenie's Alias verbatim.
    all_rows = compute_noise_rows(alerts)
    ranked_rows = filter_by_min_fires(all_rows, args.min_fires)
    top_rows = top_n(ranked_rows, args.top_n)
    summary = summarize(alerts, all_rows)

    # Coarse view: collapse Datadog monitor_id fragments so same-monitor
    # rows merge. Non-Datadog aliases (UUIDs from other integrations)
    # pass through unchanged.
    all_rows_coarse = compute_noise_rows(alerts, key_fn=lambda a: coarse_alias(a.alias))
    ranked_rows_coarse = filter_by_min_fires(all_rows_coarse, args.min_fires)
    top_rows_coarse = top_n(ranked_rows_coarse, args.top_n)
    summary_coarse = summarize(alerts, all_rows_coarse)
    raw_per_coarse = _count_raw_aliases_per_coarse_key(alerts)

    window_days = (
        (summary.window_end - summary.window_start).days + 1
        if summary.window_start and summary.window_end
        else 0
    )

    csv_path = out_dir / f"{date_prefix}_opsgenie_noise_ranked.csv"
    png_path = out_dir / f"{date_prefix}_opsgenie_noise_top_{args.top_n}.png"
    txt_path = out_dir / f"{date_prefix}_opsgenie_noise_summary.txt"
    csv_path_coarse = out_dir / f"{date_prefix}_opsgenie_noise_ranked_coarse.csv"
    png_path_coarse = out_dir / f"{date_prefix}_opsgenie_noise_top_{args.top_n}_coarse.png"

    write_ranked_csv(ranked_rows, csv_path)
    write_ranked_csv(
        ranked_rows_coarse, csv_path_coarse,
        key_column="coarse_key",
        raw_aliases_merged=raw_per_coarse,
    )
    render_top_n_chart(top_rows, png_path, summary.total_alerts, window_days)
    render_top_n_chart(
        top_rows_coarse, png_path_coarse, summary.total_alerts, window_days,
    )
    render_summary(
        summary, ranked_rows, txt_path,
        coarse_summary=summary_coarse, coarse_rows=ranked_rows_coarse,
    )

    # Terminal-friendly synopsis so the operator sees the punchline
    # without opening the summary file.
    print()
    print(f"OpsGenie noise report ({args.csv.name}):")
    print(
        f"  {summary.total_alerts:,} alerts across "
        f"{summary.unique_aliases:,} monitors"
    )
    print(f"  Ack rate: {summary.overall_ack_rate * 100:.1f}%")
    print(
        f"  Top 5 raw aliases produce  {summary.top_5_share * 100:5.1f}% of all volume"
    )
    print(
        f"  Top 5 coarse monitors     {summary_coarse.top_5_share * 100:5.1f}% "
        f"of all volume  (\u2190 the real Pareto)"
    )
    print(f"  Full report:  {txt_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
