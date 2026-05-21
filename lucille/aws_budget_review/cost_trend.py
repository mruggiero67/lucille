"""Q1: How do AWS costs today compare to October, and is the trend steady?

Reads the per-account monthly CSV, plots monthly totals (Oct -> latest) plus a
per-account stacked breakdown, and writes a short text summary that calls out
whether the trend is monotonic or whether there was a blip.

Usage::

    python -m lucille.aws_budget_review.cost_trend \
        --input ~/Desktop/debris/engineering_budget/2026_05_04_aws_cost_per_account_oct_to_mid_may.csv \
        --output-dir ~/Desktop/debris \
        --partial-month-days 15
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

from lucille.aws_budget_review._common import (
    load_account_csv,
    normalize_partial_month,
)


def _is_blip(totals: pd.Series) -> tuple[bool, pd.Timestamp | None]:
    """Heuristic: a blip is a single month >=40% above both neighbours.

    Note: callers should pass a series with the partial-month value already
    projected to a full month, otherwise the trailing partial month looks
    artificially low and masks a continuing spike.
    """
    for i in range(1, len(totals) - 1):
        prev_v, cur_v, next_v = totals.iloc[i - 1], totals.iloc[i], totals.iloc[i + 1]
        if cur_v >= 1.4 * prev_v and cur_v >= 1.4 * next_v:
            return True, totals.index[i]
    return False, None


def _plot(
    totals: pd.Series,
    per_account: pd.DataFrame,
    out_path: Path,
    partial_month: pd.Timestamp | None,
    projected_total: float | None,
) -> None:
    fig, (ax_top, ax_bot) = plt.subplots(
        2, 1, figsize=(11, 9), gridspec_kw={"height_ratios": [1, 1.2]}
    )

    # --- top: total cost line with markers ---
    ax_top.plot(totals.index, totals.values, marker="o", linewidth=2, color="#1f4e79")
    for x, y in zip(totals.index, totals.values):
        ax_top.annotate(
            f"${y/1000:,.0f}k",
            (x, y),
            textcoords="offset points",
            xytext=(0, 8),
            ha="center",
            fontsize=9,
        )
    if partial_month is not None and projected_total is not None:
        ax_top.plot(
            [partial_month],
            [projected_total],
            marker="x",
            color="#c00000",
            markersize=12,
            mew=2,
            label=f"{partial_month.strftime('%b %Y')} projected full month",
        )
        ax_top.annotate(
            f"~${projected_total/1000:,.0f}k proj.",
            (partial_month, projected_total),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            color="#c00000",
            fontsize=9,
        )
        ax_top.legend(loc="upper left")
    ax_top.set_title("AWS monthly cost — total across all linked accounts")
    ax_top.set_ylabel("USD / month")
    ax_top.grid(True, alpha=0.3)
    ax_top.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))

    # --- bottom: stacked area by account ---
    # Order accounts by total spend descending so the dominant one is visible.
    ordered = per_account.sum().sort_values(ascending=False).index
    per_account = per_account[ordered]
    ax_bot.stackplot(
        per_account.index,
        [per_account[c].values for c in per_account.columns],
        labels=per_account.columns,
        alpha=0.85,
    )
    ax_bot.set_title("Breakdown by linked account")
    ax_bot.set_ylabel("USD / month")
    ax_bot.grid(True, alpha=0.3)
    ax_bot.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax_bot.legend(loc="upper left", fontsize=8, ncol=2)

    fig.suptitle("Q1: AWS spend trend, Oct 2025 → present", fontsize=14, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def _write_summary(
    totals: pd.Series,
    per_account: pd.DataFrame,
    out_path: Path,
    partial_month: pd.Timestamp | None,
    partial_days: int | None,
    projected_total: float | None,
) -> str:
    first_month = totals.index[0]
    last_month = totals.index[-1]
    first_v = totals.iloc[0]
    last_v = totals.iloc[-1]
    # Run the blip detector against a totals series where the trailing partial
    # month has been projected to a full month -- otherwise the partial value
    # makes April look like a one-off when it may not be.
    totals_for_trend = totals.copy()
    if partial_month is not None and projected_total is not None:
        totals_for_trend.loc[partial_month] = projected_total
    blip, blip_month = _is_blip(totals_for_trend)
    peak_month = totals_for_trend.idxmax()
    peak_value = totals_for_trend.max()

    lines: list[str] = []
    lines.append("# Q1 — AWS costs: now vs. October\n")
    lines.append(
        f"Spend in **{first_month.strftime('%b %Y')}** was **${first_v:,.0f}**. "
        f"The most recent month (**{last_month.strftime('%b %Y')}**) reports "
        f"**${last_v:,.0f}** (raw value as appears in the CSV)."
    )
    if partial_month is not None and partial_days is not None and projected_total is not None:
        lines.append(
            f"\n> ⚠️ The {partial_month.strftime('%b %Y')} row is partial — "
            f"assumed to cover **{partial_days} elapsed days**. Linearly projected to a full month "
            f"that becomes **${projected_total:,.0f}**, i.e. "
            f"{((projected_total/first_v)-1)*100:+.0f}% vs. October."
        )

    lines.append("\n## Monthly totals\n")
    lines.append("| Month | Total ($) | MoM change |")
    lines.append("|---|---:|---:|")
    prev = None
    for ts, v in totals.items():
        tag = " *(partial)*" if ts == partial_month else ""
        mom = "" if prev is None else f"{(v/prev-1)*100:+.1f}%"
        lines.append(f"| {ts.strftime('%Y-%m')}{tag} | {v:,.0f} | {mom} |")
        prev = v

    lines.append("\n## Trend assessment\n")
    if blip:
        before = totals_for_trend.loc[:blip_month].iloc[-2]
        after = totals_for_trend.loc[blip_month:].iloc[1] if blip_month != totals_for_trend.index[-1] else None
        msg = (
            f"A single-month spike was detected in {blip_month.strftime('%b %Y')} "
            f"(${peak_value:,.0f}, vs. ${before:,.0f} the prior month"
        )
        if after is not None:
            msg += f" and ${after:,.0f} the following month (projected)"
        msg += ")."
        deltas = (per_account.loc[blip_month] - per_account.loc[:blip_month].iloc[-2]).sort_values(
            ascending=False
        )
        top = deltas.head(3)
        msg += " Top contributors: " + ", ".join(
            f"**{name}** (+${val:,.0f})" for name, val in top.items()
        ) + "."
        lines.append(msg)

    # Always compare projected-May daily rate vs. earlier months -- this is
    # the test of whether the April spike has actually subsided.
    if partial_month is not None and partial_days is not None and projected_total is not None:
        april_v = totals.iloc[-2]  # April is the second-to-last entry
        prior_baseline = totals.iloc[:-2].mean()  # Oct..Mar mean
        lines.append(
            f"\n### Has the spike been corrected?\n"
            f"- **Pre-spike baseline (Oct – Mar mean):** ${prior_baseline:,.0f}/mo\n"
            f"- **April (full month):** ${april_v:,.0f}\n"
            f"- **May (projected from {partial_days}d):** ${projected_total:,.0f}\n"
        )
        if projected_total >= 0.9 * april_v:
            lines.append(
                "⚠️ **The spike does NOT appear corrected.** Projected May spend "
                f"(${projected_total:,.0f}) is within 10% of April's spike level "
                f"and roughly **{projected_total / prior_baseline:.1f}× the pre-spike baseline**. "
                "The April surge is continuing into May, not abating."
            )
        elif projected_total >= 1.2 * prior_baseline:
            lines.append(
                "Partially corrected: projected May is below April but still well "
                f"above the pre-spike baseline (~{projected_total/prior_baseline:.1f}×)."
            )
        else:
            lines.append(
                "✓ Spike appears corrected: projected May is close to the pre-spike baseline."
            )
    else:
        slope = (last_v - first_v) / max(len(totals) - 1, 1)
        lines.append(
            f"\nAverage month-over-month change: ${slope:,.0f}/month."
        )

    text = "\n".join(lines) + "\n"
    out_path.write_text(text)
    return text


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="Per-account CSV")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.home() / "Desktop" / "debris",
        help="Where to write the PNG + summary",
    )
    parser.add_argument(
        "--partial-month-days",
        type=int,
        default=15,
        help="How many days of the final (partial) month the CSV covers. "
        "Set to 0 to disable projection.",
    )
    args = parser.parse_args(argv)

    frame = load_account_csv(args.input.expanduser())
    totals = frame.monthly_total.sort_index()
    per_account = frame.monthly.sort_index()

    partial_month: pd.Timestamp | None = None
    projected_total: float | None = None
    partial_days: int | None = None
    if args.partial_month_days > 0:
        partial_month = totals.index[-1]
        partial_days = args.partial_month_days
        projected_total = normalize_partial_month(
            totals.iloc[-1], partial_month, partial_days
        )

    out_dir = args.output_dir.expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    png_path = out_dir / "q1_aws_cost_trend.png"
    md_path = out_dir / "q1_aws_cost_trend.md"

    _plot(totals, per_account, png_path, partial_month, projected_total)
    summary = _write_summary(
        totals, per_account, md_path, partial_month, partial_days, projected_total
    )
    print(summary)
    print(f"Wrote {png_path}")
    print(f"Wrote {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
