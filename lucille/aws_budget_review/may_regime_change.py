"""Q2-v2: Did the corrective actions started ~May 4 actually bend the cost curve?

This consumes a *daily* per-service CSV exported from AWS Cost Explorer (one
row per day, columns are services, last column is ``Total costs``). It splits
the timeline at ``--action-start`` (default 2026-05-04) and compares daily
run-rates in three regimes:

  * **Pre-action April** -- full April baseline.
  * **Pre-action May** -- May 1 through (action-start - 1). The "hot" days.
  * **Post-action May** -- action-start through end of data. Should show
    the effect of cost-cutting if any.

It produces:
  * A daily total-cost line chart with the action date marked and the three
    regimes shaded.
  * A horizontal bar chart of per-service monthly-equivalent savings
    (April daily rate vs. post-action daily rate, both projected to 30 days).
  * A markdown summary with portfolio-level + per-service tables.

Usage::

    python -m lucille.aws_budget_review.may_regime_change \\
        --input ~/Desktop/debris/engineering_budget/2026_05_18_aws_costs_per_day_march_through_mid_may.csv \\
        --output-dir ~/Desktop/debris \\
        --action-start 2026-05-04
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lucille.aws_budget_review._common import load_service_csv


@dataclass(frozen=True)
class Regime:
    name: str
    start: pd.Timestamp
    end: pd.Timestamp  # inclusive
    color: str

    @property
    def days(self) -> int:
        return (self.end - self.start).days + 1


def _regime_daily_rate(daily: pd.DataFrame, regime: Regime) -> pd.Series:
    """Average per-service $/day over the regime window (inclusive)."""
    window = daily.loc[regime.start : regime.end]
    if window.empty:
        return pd.Series(0.0, index=daily.columns)
    return window.mean(axis=0)


def _plot_daily_totals(
    daily_total: pd.Series,
    regimes: list[Regime],
    action_date: pd.Timestamp,
    out_path: Path,
) -> None:
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(daily_total.index, daily_total.values, marker=".", linewidth=1.2,
            color="#1f4e79")

    # Shade each regime
    ymax = float(daily_total.max()) * 1.1
    for r in regimes:
        ax.axvspan(r.start, r.end + pd.Timedelta(days=1), alpha=0.12, color=r.color)
        mid = r.start + (r.end - r.start) / 2
        ax.text(mid, ymax * 0.95, r.name, ha="center", va="top",
                fontsize=9, color=r.color, fontweight="bold")
        # Mean line for the regime
        mean_v = daily_total.loc[r.start : r.end].mean()
        ax.hlines(mean_v, r.start, r.end + pd.Timedelta(days=1),
                  colors=r.color, linestyles="--", linewidth=1.5)
        ax.text(r.end, mean_v, f" ${mean_v:,.0f}/d",
                va="center", fontsize=8, color=r.color)

    ax.axvline(action_date, color="#c00000", linestyle=":", linewidth=2,
               label=f"Cost-cutting starts ({action_date.date()})")
    ax.set_ylim(0, ymax)
    ax.set_ylabel("Daily AWS cost ($)")
    ax.set_title("Daily AWS cost — March → mid-May, with corrective-action regimes")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def _plot_service_savings(
    per_service: pd.DataFrame,
    out_path: Path,
    top_n: int = 20,
) -> None:
    # Keep biggest savings (most negative monthly_delta)
    top = per_service.sort_values("monthly_delta").head(top_n)
    # And tack on any services that went UP meaningfully -- shown in red below
    risers = per_service[per_service["monthly_delta"] > 100].sort_values(
        "monthly_delta", ascending=False
    ).head(5)
    panel = pd.concat([top, risers])

    fig, ax = plt.subplots(figsize=(11, max(5, 0.42 * len(panel) + 2)))
    monthly_delta = panel["monthly_delta"].values
    colors = ["#2e7d32" if v < 0 else "#c62828" for v in monthly_delta]
    labels = [
        f"{name}  (${a:,.0f}/d → ${p:,.0f}/d)"
        for name, a, p in zip(panel.index, panel["april_daily"], panel["post_daily"])
    ]
    bars = ax.barh(labels, -monthly_delta, color=colors)  # negate so savings are positive bars
    ax.invert_yaxis()
    ax.axvline(0, color="black", linewidth=0.8)
    ax.set_xlabel("Monthly-equivalent savings ($/mo). Green = saved, red = increased.")
    ax.set_title(
        f"Per-service Apr-daily vs. post-action-May-daily, projected to 30-day month"
    )
    for bar, val in zip(bars, monthly_delta):
        ax.text(bar.get_width(),
                bar.get_y() + bar.get_height() / 2,
                f" ${-val:,.0f}/mo" if val < 0 else f" +${val:,.0f}/mo",
                va="center", fontsize=8)
    ax.grid(True, axis="x", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def _build_per_service_table(
    daily: pd.DataFrame,
    march: Regime,
    april: Regime,
    pre: Regime,
    post: Regime,
) -> pd.DataFrame:
    out = pd.DataFrame(
        {
            "march_daily": _regime_daily_rate(daily, march),
            "april_daily": _regime_daily_rate(daily, april),
            "pre_daily": _regime_daily_rate(daily, pre),
            "post_daily": _regime_daily_rate(daily, post),
        }
    )
    # Apr -> Post comparison, projected to a 30-day month
    out["monthly_delta"] = (out["post_daily"] - out["april_daily"]) * 30
    out["rel_change_vs_april"] = np.where(
        out["april_daily"] > 0,
        (out["post_daily"] - out["april_daily"]) / out["april_daily"],
        0.0,
    )
    # Pre -> Post drop (more direct measure of corrective action)
    out["pre_to_post_monthly"] = (out["post_daily"] - out["pre_daily"]) * 30
    return out


def _write_summary(
    *,
    daily_total: pd.Series,
    regimes: dict[str, Regime],
    per_service: pd.DataFrame,
    out_path: Path,
) -> str:
    rates = {
        name: daily_total.loc[r.start : r.end].mean()
        for name, r in regimes.items()
    }
    lines: list[str] = []
    lines.append("# Q2 (v2) — Did the May cost-cutting work?\n")
    lines.append(
        f"Comparison of daily AWS run-rates across four regimes, using the "
        f"daily-granularity CSV (Mar 1 → {daily_total.index[-1].date()}).\n"
    )

    lines.append("## Portfolio daily run-rates\n")
    lines.append("| Regime | Window | Days | Daily rate ($/d) | 30-day projection ($) |")
    lines.append("|---|---|---:|---:|---:|")
    for name, r in regimes.items():
        rate = rates[name]
        lines.append(
            f"| {name} | {r.start.date()} → {r.end.date()} | {r.days} "
            f"| {rate:,.0f} | {rate*30:,.0f} |"
        )

    pre_rate = rates["Pre-action May"]
    post_rate = rates["Post-action May"]
    april_rate = rates["April"]
    march_rate = rates["March"]

    pre_to_post = (post_rate - pre_rate) * 30
    april_to_post = (post_rate - april_rate) * 30
    # Signed deltas: negative = post is lower than reference (a saving).
    delta_vs_peak_pct = (post_rate / pre_rate - 1) * 100 if pre_rate else 0
    delta_vs_april_pct = (post_rate / april_rate - 1) * 100 if april_rate else 0
    delta_vs_march_pct = (post_rate / march_rate - 1) * 100 if march_rate else 0

    lines.append("\n## Headline\n")
    lines.append(
        f"- Pre-action May daily rate (May 1–3): **${pre_rate:,.0f}/day**\n"
        f"- Post-action May daily rate (May 4 onward): **${post_rate:,.0f}/day** "
        f"({delta_vs_peak_pct:+.1f}% vs. pre-action peak)\n"
        f"- Implied monthly savings vs. pre-action peak: "
        f"**${-pre_to_post:,.0f}/month** "
        f"(${-pre_to_post*12:,.0f}/year if sustained)\n"
        f"- Implied monthly savings vs. April baseline: "
        f"**${-april_to_post:,.0f}/month** ({delta_vs_april_pct:+.1f}% vs. April daily rate)\n"
        f"- vs. March (pre-spike baseline): post-action daily rate is "
        f"{delta_vs_march_pct:+.1f}% "
        f"({'still above' if post_rate > march_rate else 'now below'} March)."
    )

    # Per-service: savings table
    savings = per_service[per_service["monthly_delta"] < -100].sort_values(
        "monthly_delta"
    )
    risers = per_service[per_service["monthly_delta"] > 100].sort_values(
        "monthly_delta", ascending=False
    )

    lines.append("\n## Services with the biggest drop (Apr daily → post-action daily, ×30)\n")
    if savings.empty:
        lines.append("_No services dropped by more than $100/month-equivalent._")
    else:
        lines.append(
            "| Service | Mar $/d | Apr $/d | Pre-action May $/d | Post-action May $/d "
            "| Δ vs Apr ($/mo) | Δ vs Apr % |"
        )
        lines.append("|---|---:|---:|---:|---:|---:|---:|")
        for name, row in savings.head(20).iterrows():
            lines.append(
                f"| {name} "
                f"| {row['march_daily']:,.0f} "
                f"| {row['april_daily']:,.0f} "
                f"| {row['pre_daily']:,.0f} "
                f"| {row['post_daily']:,.0f} "
                f"| {row['monthly_delta']:+,.0f} "
                f"| {row['rel_change_vs_april']*100:+.0f}% |"
            )

    if not risers.empty:
        lines.append("\n## Services that went UP post-action (worth investigating)\n")
        lines.append(
            "| Service | Apr $/d | Post-action May $/d | Δ ($/mo) | Δ % |"
        )
        lines.append("|---|---:|---:|---:|---:|")
        for name, row in risers.head(10).iterrows():
            lines.append(
                f"| {name} "
                f"| {row['april_daily']:,.0f} "
                f"| {row['post_daily']:,.0f} "
                f"| {row['monthly_delta']:+,.0f} "
                f"| {row['rel_change_vs_april']*100:+.0f}% |"
            )

    # Sanity check / extrapolation
    total_savings_vs_april = float(savings["monthly_delta"].sum()) if not savings.empty else 0.0
    total_risers = float(risers["monthly_delta"].sum()) if not risers.empty else 0.0
    lines.append("\n## Bottom line\n")
    lines.append(
        f"Sum of per-service drops vs. April baseline: "
        f"**${-total_savings_vs_april:,.0f}/month** of gross savings.\n\n"
        f"Sum of per-service increases: **${total_risers:,.0f}/month** of new spend.\n\n"
        f"**Net portfolio movement Apr → post-action May: "
        f"${april_to_post:,.0f}/month** "
        f"({'savings' if april_to_post < 0 else 'increase'}).\n"
    )
    if post_rate < april_rate * 0.95:
        lines.append(
            "✅ Corrective actions appear to be working: post-action daily rate "
            "is materially below the April baseline."
        )
    elif post_rate < pre_rate * 0.9:
        lines.append(
            "🟡 Partial progress: post-action rate is meaningfully below the "
            "pre-action peak, but still close to the April baseline. The April "
            "step-up has been arrested but not yet reversed."
        )
    else:
        lines.append(
            "⚠️ Limited impact so far: post-action daily rate is not materially "
            "below either the April baseline or the pre-action May peak. Either "
            "the cuts haven't taken effect yet, or the savings are being offset "
            "by other growth."
        )

    text = "\n".join(lines) + "\n"
    out_path.write_text(text)
    return text


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.home() / "Desktop" / "debris",
    )
    parser.add_argument(
        "--action-start",
        type=lambda s: pd.Timestamp(s),
        default=pd.Timestamp("2026-05-04"),
        help="First day the corrective actions took effect (inclusive).",
    )
    args = parser.parse_args(argv)

    frame = load_service_csv(args.input.expanduser())
    daily = frame.monthly.sort_index()  # column name is historical; this is now per-day
    daily_total = frame.monthly_total.sort_index()
    last_day = daily.index.max()

    march = Regime(
        "March",
        pd.Timestamp("2026-03-01"),
        pd.Timestamp("2026-03-31"),
        "#7e57c2",
    )
    april = Regime(
        "April",
        pd.Timestamp("2026-04-01"),
        pd.Timestamp("2026-04-30"),
        "#ef6c00",
    )
    pre = Regime(
        "Pre-action May",
        pd.Timestamp("2026-05-01"),
        args.action_start - pd.Timedelta(days=1),
        "#c62828",
    )
    post = Regime(
        "Post-action May",
        args.action_start,
        last_day,
        "#2e7d32",
    )

    per_service = _build_per_service_table(daily, march, april, pre, post)

    out_dir = args.output_dir.expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    daily_png = out_dir / "q2v2_daily_regimes.png"
    savings_png = out_dir / "q2v2_per_service_savings.png"
    md_path = out_dir / "q2v2_may_regime_change.md"

    _plot_daily_totals(daily_total, [march, april, pre, post], args.action_start, daily_png)
    _plot_service_savings(per_service, savings_png)
    summary = _write_summary(
        daily_total=daily_total,
        regimes={"March": march, "April": april, "Pre-action May": pre, "Post-action May": post},
        per_service=per_service,
        out_path=md_path,
    )
    print(summary)
    print(f"Wrote {daily_png}")
    print(f"Wrote {savings_png}")
    print(f"Wrote {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
