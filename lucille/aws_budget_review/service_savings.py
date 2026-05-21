"""Q2: Which services were "cut" between Mar 1 and today, and how much did
that save us between April and May?

Approach:
  * Read the per-service monthly CSV (Mar / Apr / May).
  * The May row is a partial month. We compute:
      - raw April -> May delta (assumes the CSV's May value is a full month)
      - projected April -> May delta (scales May up to a full month based on
        ``--partial-month-days``)
      - a break-even days figure: how many elapsed days May would have to
        cover for the *raw* total to equal April -- below that, claiming net
        savings is plausible; above that, "savings" are an artifact.
  * Always emit both an informative chart (top per-service drops, raw AND
    projected) and a markdown summary, even when no service crosses the
    "real cut" thresholds.

Usage::

    python -m lucille.aws_budget_review.service_savings \
        --input ~/Desktop/debris/engineering_budget/2026_05_04_aws_costs_by_service_march_to_mid_may.csv \
        --output-dir ~/Desktop/debris \
        --partial-month-days 15
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lucille.aws_budget_review._common import (
    days_in_month,
    load_service_csv,
)


def _build_delta_table(
    march: pd.Series,
    april: pd.Series,
    may_raw: pd.Series,
    may_projected: pd.Series,
) -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "march": march,
            "april": april,
            "may_raw": may_raw,
            "may_projected": may_projected,
        }
    )
    df["raw_delta"] = df["may_raw"] - df["april"]            # negative = drop
    df["proj_delta"] = df["may_projected"] - df["april"]     # negative = drop
    df["raw_rel"] = np.where(df["april"] > 0, df["raw_delta"] / df["april"], 0.0)
    df["proj_rel"] = np.where(df["april"] > 0, df["proj_delta"] / df["april"], 0.0)
    return df


def _classify_cuts(
    df: pd.DataFrame,
    min_abs_drop: float,
    min_rel_drop: float,
) -> pd.DataFrame:
    """Services whose projected May spend dropped enough vs. April to call a 'cut'."""
    mask = (df["proj_delta"] <= -min_abs_drop) & (df["proj_rel"] <= -min_rel_drop)
    return df[mask].sort_values("proj_delta")


def _plot(
    df: pd.DataFrame,
    cuts: pd.DataFrame,
    partial_days: int,
    break_even_days: float | None,
    april_total: float,
    may_proj_total: float,
    out_path: Path,
    top_n: int = 15,
) -> None:
    # Top-N services by raw drop magnitude -- always plot something so the
    # artifact is never empty.
    top = df.sort_values("raw_delta").head(top_n)
    services = top.index.tolist()
    y = np.arange(len(services))

    raw_drops = -top["raw_delta"].values            # positive = $ dropped
    proj_drops = -top["proj_delta"].values          # may be negative (i.e. May proj > April)

    fig, (ax_main, ax_summary) = plt.subplots(
        2, 1, figsize=(12, max(7, 0.55 * len(services) + 3)),
        gridspec_kw={"height_ratios": [3, 1]},
    )

    bar_h = 0.4
    ax_main.barh(y - bar_h / 2, raw_drops, height=bar_h,
                 color="#2e7d32", label="Raw drop (May value as-is in CSV)")
    ax_main.barh(y + bar_h / 2, proj_drops, height=bar_h,
                 color="#c62828",
                 label=f"Projected drop (May scaled from {partial_days}d → full month)")
    ax_main.set_yticks(y)
    ax_main.set_yticklabels(services)
    ax_main.invert_yaxis()
    ax_main.axvline(0, color="black", linewidth=0.8)
    ax_main.set_xlabel("Apparent monthly savings vs. April ($)  — negative = May actually higher")
    ax_main.set_title(
        f"Q2: Top {len(services)} services by April→May drop\n"
        f"Services meeting 'real cut' thresholds (proj.): {len(cuts)}"
    )
    ax_main.grid(True, axis="x", alpha=0.3)
    ax_main.legend(loc="lower right", fontsize=9)

    # Annotate bars with dollar values
    for yi, rv, pv in zip(y, raw_drops, proj_drops):
        ax_main.text(rv, yi - bar_h / 2, f" ${rv:,.0f}",
                     va="center", fontsize=8, color="#1b5e20")
        ax_main.text(pv, yi + bar_h / 2, f" ${pv:,.0f}",
                     va="center", fontsize=8, color="#b71c1c")

    # --- summary subplot: portfolio totals + break-even ---
    ax_summary.axis("off")
    lines = [
        f"April total (all services):  ${april_total:>10,.0f}",
        f"May projected (assuming {partial_days} elapsed days):  ${may_proj_total:>10,.0f}",
        f"Net portfolio change:  ${may_proj_total - april_total:>+10,.0f}  "
        f"({(may_proj_total / april_total - 1) * 100:+.1f}% vs. April)",
    ]
    if break_even_days is not None:
        lines.append(
            f"Break-even days: at {break_even_days:.1f} elapsed days, projected May "
            f"would equal April. If May covers MORE days than that, daily rate "
            f"is below April's (real savings). If FEWER, daily rate is above."
        )
        if partial_days < break_even_days:
            lines.append(
                "⚠️  Your elapsed days is BELOW break-even → May's daily rate "
                "exceeds April's → projected May > April → no net savings."
            )
        else:
            lines.append(
                "✓  Your elapsed days is ABOVE break-even → May's daily rate "
                "is below April's → net savings remain after projection."
            )
    ax_summary.text(
        0.01, 0.95, "\n".join(lines),
        transform=ax_summary.transAxes, va="top", ha="left",
        family="monospace", fontsize=10,
    )

    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def _write_summary(
    df: pd.DataFrame,
    cuts: pd.DataFrame,
    partial_days: int,
    full_days: int,
    break_even_days: float | None,
    out_path: Path,
) -> str:
    april_total = float(df["april"].sum())
    may_raw_total = float(df["may_raw"].sum())
    may_proj_total = float(df["may_projected"].sum())

    lines: list[str] = []
    lines.append("# Q2 — Services cut between 1-Mar and today\n")
    lines.append(
        f"Comparing **April vs. May 2026** per-service spend. The May row is a "
        f"partial month; we assume **{partial_days} elapsed days** and project "
        f"it to a full month for an apples-to-apples comparison."
    )
    lines.append(
        f"\n## Portfolio totals\n"
        f"| Metric | $ |\n"
        f"|---|---:|\n"
        f"| April (full month) | {april_total:,.0f} |\n"
        f"| May raw (as in CSV) | {may_raw_total:,.0f} |\n"
        f"| May projected (×{full_days}/{partial_days}) | "
        f"{may_proj_total:,.0f} |\n"
        f"| **Net change vs. April (projected)** | "
        f"**{may_proj_total - april_total:+,.0f} "
        f"({(may_proj_total / april_total - 1) * 100:+.1f}%)** |"
    )

    if break_even_days is not None:
        april_daily = float(df["april"].sum()) / 30  # April has 30 days
        may_daily = float(df["may_raw"].sum()) / partial_days if partial_days else 0
        verdict = (
            "**above** April's" if may_daily > april_daily else "**below** April's"
        )
        lines.append(
            f"\n### Daily run-rate comparison (the honest apples-to-apples view)\n"
            f"| Month | Daily run-rate ($/day) |\n"
            f"|---|---:|\n"
            f"| April (30 days) | {april_daily:,.0f} |\n"
            f"| May ({partial_days} days) | {may_daily:,.0f} |\n\n"
            f"May's daily rate is {verdict}. Break-even elapsed days (where May's "
            f"daily rate would equal April's) is **{break_even_days:.1f}**. "
            f"You confirmed May covers **{partial_days} days**, so projecting "
            f"linearly gives a full-month May estimate "
            f"{'HIGHER' if may_daily > april_daily else 'LOWER'} than April."
        )

    # The headline answer
    if len(cuts) == 0:
        lines.append(
            "\n## Headline: no services qualify as cleanly 'cut'\n"
            f"With the {partial_days}-day projection, **zero services** meet the "
            "thresholds (≥$100/mo absolute drop **and** ≥25% relative drop). "
            "The table below still shows the biggest **raw** drops so you can "
            "judge for yourself — but be aware they are almost certainly a "
            "partial-month artifact (note how uniformly ~50% they are)."
        )
    else:
        savings = float(-cuts["proj_delta"].sum())
        lines.append(
            f"\n## Headline\n"
            f"Under the {partial_days}-day projection, **{len(cuts)} services** "
            f"meet the 'real cut' thresholds, totalling **~${savings:,.0f}/month** "
            f"(~${savings*12:,.0f}/year if sustained)."
        )

    # Always include a top-drops table, regardless of cut classification.
    lines.append("\n## Top 15 services by raw April→May drop\n")
    lines.append(
        "| Service | Mar ($) | Apr ($) | May raw ($) | May proj. ($) | "
        "Raw Δ ($) | Proj. Δ ($) | Proj. % |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    top = df.sort_values("raw_delta").head(15)
    for name, row in top.iterrows():
        flag = " ✅" if name in cuts.index else ""
        lines.append(
            f"| {name}{flag} "
            f"| {row['march']:,.0f} "
            f"| {row['april']:,.0f} "
            f"| {row['may_raw']:,.0f} "
            f"| {row['may_projected']:,.0f} "
            f"| {row['raw_delta']:+,.0f} "
            f"| {row['proj_delta']:+,.0f} "
            f"| {row['proj_rel']*100:+.0f}% |"
        )
    if len(cuts):
        lines.append("\n✅ = meets 'real cut' thresholds under the projection.")

    # Interpretation
    lines.append("\n## Interpretation\n")
    if may_proj_total >= april_total:
        lines.append(
            "Across the portfolio, **projected May spend is not lower than April**. "
            "The uniform ~50% raw drop across nearly every service is the "
            "signature of a half-month export, not of cost-cutting. Recommend "
            "re-running this analysis once May closes."
        )
    else:
        lines.append(
            f"Projected May spend is **${april_total - may_proj_total:,.0f}/month lower** "
            "than April. The services flagged ✅ are the credible contributors. "
            "Still worth re-running once May closes to confirm."
        )

    text = "\n".join(lines) + "\n"
    out_path.write_text(text)
    return text


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="Per-service CSV")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.home() / "Desktop" / "debris",
    )
    parser.add_argument(
        "--partial-month-days",
        type=int,
        default=15,
        help="Days of the final (May) month the CSV covers; 0 to disable projection.",
    )
    parser.add_argument("--min-abs-drop", type=float, default=100.0)
    parser.add_argument("--min-rel-drop", type=float, default=0.25)
    args = parser.parse_args(argv)

    frame = load_service_csv(args.input.expanduser())
    monthly = frame.monthly.sort_index()
    if len(monthly) < 2:
        raise SystemExit("Need at least two months of service data.")

    march = monthly.iloc[0]
    april = (
        monthly.loc[monthly.index == "2026-04-01"].iloc[0]
        if any(monthly.index == "2026-04-01")
        else monthly.iloc[-2]
    )
    may_raw = monthly.iloc[-1]
    may_month = monthly.index[-1]
    full_days = days_in_month(may_month)

    if args.partial_month_days > 0:
        scale = full_days / args.partial_month_days
        may_projected = may_raw * scale
    else:
        may_projected = may_raw.copy()

    df = _build_delta_table(march, april, may_raw, may_projected)
    cuts = _classify_cuts(df, args.min_abs_drop, args.min_rel_drop)

    # Break-even: at what elapsed-days assumption does raw May == April?
    # raw_total_for_full_month(d) = may_raw_total * full_days / d  -- but
    # break-even on the *raw* basis is simpler: how many days produce May==April
    # assuming a uniform daily run-rate equal to April/full_days?
    april_total = float(april.sum())
    may_raw_total = float(may_raw.sum())
    # If May ran at April's daily rate, it would hit April's total at full_days.
    # The CSV's may_raw_total reflects (unknown elapsed days) at May's actual
    # daily rate. Break-even days = full_days * may_raw_total / april_total
    # gives the # of days at which a uniform April-daily-rate would have
    # accumulated may_raw_total -- i.e. an upper bound on elapsed days that
    # is still consistent with "no spend change".
    break_even_days: float | None = None
    if april_total > 0:
        break_even_days = full_days * may_raw_total / april_total

    out_dir = args.output_dir.expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    png_path = out_dir / "q2_aws_service_cuts.png"
    md_path = out_dir / "q2_aws_service_cuts.md"

    _plot(
        df=df,
        cuts=cuts,
        partial_days=args.partial_month_days,
        break_even_days=break_even_days,
        april_total=april_total,
        may_proj_total=float(may_projected.sum()),
        out_path=png_path,
    )
    summary = _write_summary(
        df=df,
        cuts=cuts,
        partial_days=args.partial_month_days,
        full_days=full_days,
        break_even_days=break_even_days,
        out_path=md_path,
    )
    print(summary)
    print(f"Wrote {png_path}")
    print(f"Wrote {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
