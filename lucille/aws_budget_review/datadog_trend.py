"""Analyze a Datadog daily-spend export to see if total spend is trending
up, down, or flat -- and which products are driving the trend.

Input format (Datadog "Spend trends" CSV export):

    dimension, Total, Feb 19, Feb 20, ..., May 18
    __TOTAL__, ..., daily values...
    profiled_host, ..., daily values...
    ...

The shape is transposed vs. the AWS exports: rows are products, columns are
days. The final day in the export is often zero (incomplete), so we drop any
all-zero trailing days.

Output (in ``--output-dir``):
  * ``datadog_daily_trend.png`` -- daily totals with 7-day rolling mean and
    linear-fit trend line; per-week bars on a secondary axis.
  * ``datadog_per_service_trend.png`` -- top products with biggest absolute
    changes between the first 4 weeks and the last 4 weeks of the window.
  * ``datadog_trend.md`` -- written summary with verdict.

Usage::

    python -m lucille.aws_budget_review.datadog_trend \\
        --input ~/Desktop/debris/engineering_budget/2026_05_18_datadog_3-months-spend-trends-2026-02-19-2026-05-18.csv \\
        --output-dir ~/Desktop/debris
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


@dataclass(frozen=True)
class DatadogFrame:
    #: Index = ``pd.Timestamp`` (day), columns = product dimensions.
    daily: pd.DataFrame
    #: Daily total across all dimensions.
    daily_total: pd.Series


def load_datadog_csv(csv_path: Path, year: int = 2026) -> DatadogFrame:
    raw = pd.read_csv(csv_path)
    if raw.columns[0] != "dimension":
        raise ValueError(f"Unexpected first column: {raw.columns[0]!r}")
    # Drop the ``Total`` summary column -- we'll recompute from dailies so
    # we can be sure rows and columns agree.
    date_cols = [c for c in raw.columns if c not in ("dimension", "Total")]

    # Parse "Feb 19", "Mar 1", ... into Timestamps. The export crosses a
    # year boundary at most once; we assume all months belong to ``year``
    # unless that produces a non-monotonic sequence, in which case we shift
    # months that came earlier in the calendar into ``year+1``.
    def _parse(label: str, default_year: int) -> pd.Timestamp:
        return pd.Timestamp(f"{label} {default_year}")

    timestamps = [_parse(c, year) for c in date_cols]
    # If the export wraps Dec -> Jan, later columns might appear earlier in
    # the year. Detect and bump year for wrapped tail.
    fixed: list[pd.Timestamp] = []
    bump = 0
    prev = None
    for ts in timestamps:
        if prev is not None and ts < prev:
            bump += 1
        fixed.append(ts + pd.DateOffset(years=bump))
        prev = fixed[-1]
    timestamps = fixed

    # Reshape: pivot rows -> date index, columns -> product.
    products = raw["dimension"].tolist()
    matrix = raw[date_cols].to_numpy(dtype=float)
    df = pd.DataFrame(matrix.T, index=pd.DatetimeIndex(timestamps), columns=products)

    # The ``__TOTAL__`` row is the daily total; pull it aside and drop.
    if "__TOTAL__" not in df.columns:
        raise ValueError("Expected a __TOTAL__ row in the Datadog CSV")
    daily_total = df["__TOTAL__"].copy()
    df = df.drop(columns=["__TOTAL__"])

    # Drop trailing days where the total is exactly 0 (incomplete export).
    while len(daily_total) and daily_total.iloc[-1] == 0:
        last = daily_total.index[-1]
        daily_total = daily_total.iloc[:-1]
        df = df.drop(index=last)

    return DatadogFrame(daily=df, daily_total=daily_total)


def _linear_fit(series: pd.Series) -> tuple[float, float]:
    """Return (slope_per_day, intercept) for the daily series."""
    x = np.arange(len(series), dtype=float)
    y = series.to_numpy(dtype=float)
    slope, intercept = np.polyfit(x, y, 1)
    return float(slope), float(intercept)


def _window_means(daily: pd.DataFrame, window_days: int = 28) -> tuple[pd.Series, pd.Series]:
    """Per-column mean for the first and last ``window_days`` days."""
    first = daily.iloc[:window_days].mean(axis=0)
    last = daily.iloc[-window_days:].mean(axis=0)
    return first, last


def _plot_daily(daily_total: pd.Series, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(daily_total.index, daily_total.values, marker=".", linewidth=1,
            color="#1f4e79", alpha=0.6, label="Daily")
    rolling = daily_total.rolling(7, min_periods=3).mean()
    ax.plot(rolling.index, rolling.values, linewidth=2.2, color="#c62828",
            label="7-day rolling mean")

    slope, intercept = _linear_fit(daily_total)
    x = np.arange(len(daily_total))
    fit_y = intercept + slope * x
    ax.plot(daily_total.index, fit_y, linewidth=2, linestyle="--",
            color="#2e7d32", label=f"Linear fit ({slope:+.2f} $/day per day)")

    first_v = daily_total.iloc[0]
    last_v = daily_total.iloc[-1]
    ax.annotate(f"${first_v:,.0f}", (daily_total.index[0], first_v),
                xytext=(5, 8), textcoords="offset points", fontsize=9)
    ax.annotate(f"${last_v:,.0f}", (daily_total.index[-1], last_v),
                xytext=(-5, 8), textcoords="offset points", ha="right", fontsize=9)

    ax.set_title("Datadog daily spend with 7-day rolling mean and linear trend")
    ax.set_ylabel("USD / day")
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def _plot_per_service(per_service: pd.DataFrame, window_days: int, out_path: Path) -> None:
    # Sort by absolute monthly delta, take the top 12 movers
    panel = per_service.reindex(per_service["monthly_delta"].abs().sort_values(ascending=False).index)
    panel = panel.head(12).iloc[::-1]  # reverse so biggest shows on top

    fig, ax = plt.subplots(figsize=(11, max(5, 0.5 * len(panel) + 2)))
    deltas = panel["monthly_delta"].values
    colors = ["#c62828" if v > 0 else "#2e7d32" for v in deltas]
    labels = [
        f"{name}  (${f:,.1f}/d → ${l:,.1f}/d)"
        for name, f, l in zip(panel.index, panel["first_mean"], panel["last_mean"])
    ]
    ax.barh(labels, deltas, color=colors)
    ax.axvline(0, color="black", linewidth=0.8)
    ax.set_xlabel(
        f"Monthly-equivalent change (last {window_days}d mean − first {window_days}d mean) × 30, $/mo"
    )
    ax.set_title("Datadog: top 12 products by absolute change in daily spend")
    for i, v in enumerate(deltas):
        ax.text(v, i, f" {'+' if v > 0 else ''}${v:,.0f}/mo",
                va="center", fontsize=8,
                color="#b71c1c" if v > 0 else "#1b5e20")
    ax.grid(True, axis="x", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def _verdict(slope_per_day: float, daily_total: pd.Series) -> tuple[str, str]:
    """Classify the overall trend.

    We look at the linear-fit slope normalised by the mean. <0.2%/day either
    direction is 'neutral'; otherwise up or down.
    """
    mean = float(daily_total.mean())
    norm = slope_per_day / mean if mean else 0.0  # fractional change per day
    pct_per_week = norm * 7 * 100
    if abs(pct_per_week) < 1.0:
        return "neutral", (
            f"The linear fit moves {pct_per_week:+.2f}%/week off the mean -- "
            "essentially flat. Day-to-day variation dominates any underlying drift."
        )
    if pct_per_week < 0:
        return "down", (
            f"The linear fit drops {pct_per_week:.2f}%/week (≈ "
            f"${slope_per_day*30:+,.0f}/mo per month). Spend is trending **down**."
        )
    return "up", (
        f"The linear fit climbs {pct_per_week:+.2f}%/week (≈ "
        f"${slope_per_day*30:+,.0f}/mo per month). Spend is trending **up**."
    )


def _write_summary(
    *,
    daily_total: pd.Series,
    daily: pd.DataFrame,
    per_service: pd.DataFrame,
    window_days: int,
    out_path: Path,
) -> str:
    slope, intercept = _linear_fit(daily_total)
    verdict_tag, verdict_msg = _verdict(slope, daily_total)

    first_mean = daily_total.iloc[:window_days].mean()
    last_mean = daily_total.iloc[-window_days:].mean()
    abs_delta_monthly = (last_mean - first_mean) * 30
    pct_delta = (last_mean / first_mean - 1) * 100 if first_mean else 0.0

    rising = per_service[per_service["monthly_delta"] > 50].sort_values(
        "monthly_delta", ascending=False
    )
    falling = per_service[per_service["monthly_delta"] < -50].sort_values(
        "monthly_delta"
    )

    lines: list[str] = []
    lines.append("# Datadog spend trend — Feb 19 → May 17, 2026\n")
    lines.append(f"**Verdict: trending {verdict_tag.upper()}.** {verdict_msg}\n")
    lines.append(
        f"## Headline numbers\n"
        f"| Metric | $ |\n"
        f"|---|---:|\n"
        f"| First {window_days}d daily mean | {first_mean:,.0f} |\n"
        f"| Last {window_days}d daily mean | {last_mean:,.0f} |\n"
        f"| Δ daily | {last_mean - first_mean:+,.0f} ({pct_delta:+.1f}%) |\n"
        f"| Δ monthly-equivalent | {abs_delta_monthly:+,.0f}/mo |\n"
        f"| Linear-fit slope | {slope:+.2f} $/day per day "
        f"(≈ {slope*30:+,.0f}/mo per month) |\n"
        f"| Window total | {daily_total.sum():,.0f} over {len(daily_total)} days |"
    )

    lines.append("\n## Products that grew the most\n")
    if rising.empty:
        lines.append("_None above $50/mo of growth._")
    else:
        lines.append("| Product | First-{0}d $/d | Last-{0}d $/d | Δ ($/mo) | Δ % |".format(window_days))
        lines.append("|---|---:|---:|---:|---:|")
        for name, row in rising.head(10).iterrows():
            pct = (row["last_mean"] / row["first_mean"] - 1) * 100 if row["first_mean"] else float("inf")
            pct_str = f"{pct:+.0f}%" if np.isfinite(pct) else "new"
            lines.append(
                f"| {name} | {row['first_mean']:,.2f} | {row['last_mean']:,.2f} "
                f"| {row['monthly_delta']:+,.0f} | {pct_str} |"
            )

    lines.append("\n## Products that shrank the most\n")
    if falling.empty:
        lines.append("_None above $50/mo of reduction._")
    else:
        lines.append("| Product | First-{0}d $/d | Last-{0}d $/d | Δ ($/mo) | Δ % |".format(window_days))
        lines.append("|---|---:|---:|---:|---:|")
        for name, row in falling.head(10).iterrows():
            pct = (row["last_mean"] / row["first_mean"] - 1) * 100 if row["first_mean"] else 0.0
            lines.append(
                f"| {name} | {row['first_mean']:,.2f} | {row['last_mean']:,.2f} "
                f"| {row['monthly_delta']:+,.0f} | {pct:+.0f}% |"
            )

    # Inflection / anomaly detection: any single day > 2 sigma above 7d mean?
    rolling7 = daily_total.rolling(7, min_periods=3).mean()
    resid = daily_total - rolling7
    sigma = float(resid.std())
    spikes = daily_total[resid.abs() > 2 * sigma]
    if not spikes.empty:
        lines.append("\n## Notable single-day deviations (>2σ from 7-day mean)\n")
        for ts, v in spikes.items():
            lines.append(f"- {ts.date()}: ${v:,.0f} (vs ~${rolling7.loc[ts]:,.0f} trailing mean)")

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
        "--window-days",
        type=int,
        default=28,
        help="Compare the first N days vs. the last N days for per-product trend.",
    )
    args = parser.parse_args(argv)

    frame = load_datadog_csv(args.input.expanduser())
    if len(frame.daily_total) < 2 * args.window_days:
        # Shrink the comparison window if we don't have enough data
        args.window_days = max(7, len(frame.daily_total) // 3)

    first_mean, last_mean = _window_means(frame.daily, args.window_days)
    per_service = pd.DataFrame({"first_mean": first_mean, "last_mean": last_mean})
    per_service["daily_delta"] = per_service["last_mean"] - per_service["first_mean"]
    per_service["monthly_delta"] = per_service["daily_delta"] * 30

    out_dir = args.output_dir.expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    daily_png = out_dir / "datadog_daily_trend.png"
    service_png = out_dir / "datadog_per_service_trend.png"
    md_path = out_dir / "datadog_trend.md"

    _plot_daily(frame.daily_total, daily_png)
    _plot_per_service(per_service, args.window_days, service_png)
    summary = _write_summary(
        daily_total=frame.daily_total,
        daily=frame.daily,
        per_service=per_service,
        window_days=args.window_days,
        out_path=md_path,
    )
    print(summary)
    print(f"Wrote {daily_png}")
    print(f"Wrote {service_png}")
    print(f"Wrote {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
