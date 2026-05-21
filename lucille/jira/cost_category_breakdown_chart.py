"""
Render Seaborn-styled charts of cost-category breakdowns by project,
given a labeled-epics CSV (the kind produced upstream by
``cost_category_breakdown.py`` / the epic-tagging pipeline).

Two artefacts per run:
  * **Stacked bar chart** of % of *ticket count* delivered per cost category
    across all projects. Each project's bar sums to 100%; absolute ticket
    counts are annotated above each bar so volume context isn't lost.
  * **Pie chart per major project** (``--pie-projects``, default
    ``OOT,SSJ``) showing each project's cost-category mix in isolation.

Ticket count is used as the effort proxy because the upstream export
currently doesn't sum story points to the epic level (most rows have
``story_points_sum=0``). When that data is fixed upstream the bar chart
is trivial to extend back to two panels.

Usage:
    python -m lucille.jira.cost_category_breakdown_chart \\
        --csv ~/Desktop/debris/2026_05_08_tagged_epics.csv

    # Different pie subjects
    python -m lucille.jira.cost_category_breakdown_chart \\
        --csv ... --pie-projects OOT,SSJ,DIP

    # Skip pies entirely
    python -m lucille.jira.cost_category_breakdown_chart \\
        --csv ... --pie-projects ''
"""

from __future__ import annotations

import argparse
import logging
from datetime import date
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # no display required
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

logging.basicConfig(
    format="%(levelname)-10s %(asctime)s %(filename)s %(lineno)d %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = Path.home() / "Desktop" / "debris"

PROJECT_COL = "project"
CATEGORY_COL = "cost_category"
TICKET_COUNT_COL = "ticket_count"

DEFAULT_PIE_PROJECTS = ("OOT", "SSJ")


# ---- pure ------------------------------------------------------------------

def compute_percent_breakdown(
    df: pd.DataFrame,
    metric_col: str,
    *,
    project_col: str = PROJECT_COL,
    category_col: str = CATEGORY_COL,
) -> pd.DataFrame:
    """
    Compute a project x cost_category matrix of percentages where each row
    sums to 100% (or 0% if the project has no metric volume).

    Pure: no I/O.

    Projects with a zero total for ``metric_col`` produce a row of zeros
    rather than NaN, so the chart still renders a (zero-height) bar slot
    for them and they're visible on the x-axis.
    """
    if df.empty:
        return pd.DataFrame()

    pivot = (
        df.groupby([project_col, category_col])[metric_col]
        .sum()
        .astype(float)
        .unstack(fill_value=0.0)
    )
    totals = pivot.sum(axis=1)
    # Guard zero-total rows: divide only where total > 0, fill the rest with 0.
    pct = pivot.div(totals.where(totals > 0), axis=0).fillna(0.0) * 100.0
    return pct.sort_index()


def project_totals(
    df: pd.DataFrame,
    metric_col: str,
    *,
    project_col: str = PROJECT_COL,
) -> pd.Series:
    """Total of ``metric_col`` per project, sorted by project. Pure."""
    if df.empty:
        return pd.Series(dtype=float)
    return df.groupby(project_col)[metric_col].sum().sort_index()


def all_categories(*pct_dfs: pd.DataFrame) -> list[str]:
    """Union of cost_category columns across the supplied breakdowns. Pure."""
    cats: set[str] = set()
    for df in pct_dfs:
        cats.update(df.columns)
    return sorted(cats)


def category_color_map(categories: list[str]) -> dict[str, tuple]:
    """Stable category -> RGBA color mapping using the seaborn Set2 palette.

    Pure. Used by both the bar chart and the per-project pies so the same
    cost category renders the same color across all artefacts.
    """
    palette = sns.color_palette("Set2", n_colors=max(len(categories), 1))
    return dict(zip(categories, palette))


def parse_pie_projects(arg: str) -> list[str]:
    """Comma-separated CLI list of project keys; empty string -> empty list. Pure."""
    return [p.strip() for p in (arg or "").split(",") if p.strip()]


# ---- side-effecting --------------------------------------------------------

def load_epics(path: Path) -> pd.DataFrame:
    """Read the CSV and normalise whitespace on join columns."""
    df = pd.read_csv(path)
    for col in (PROJECT_COL, CATEGORY_COL):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    logger.info("Loaded %d rows from %s", len(df), path)
    return df


def _annotate_totals(ax, projects: list[str], totals: pd.Series, suffix: str) -> None:
    """Print absolute volume above each project bar."""
    for i, project in enumerate(projects):
        n = int(totals.get(project, 0))
        ax.text(
            i, 101, f"n={n}{suffix}",
            ha="center", va="bottom", fontsize=8, color="#555555",
        )


def render_chart(
    df: pd.DataFrame,
    output_path: Path,
    *,
    title: str | None = None,
    figsize: tuple[float, float] = (10, 6),
) -> Path:
    """Render the single-panel ticket-count stacked bar chart and save a PNG."""
    if df.empty:
        raise ValueError("Input DataFrame is empty; nothing to plot.")

    pct_tc = compute_percent_breakdown(df, TICKET_COUNT_COL)
    totals_tc = project_totals(df, TICKET_COUNT_COL)

    categories = all_categories(pct_tc)
    if not categories:
        raise ValueError(f"No '{CATEGORY_COL}' values found in the CSV.")

    pct_tc = pct_tc.reindex(columns=categories, fill_value=0)

    sns.set_theme(style="whitegrid")
    color_map = category_color_map(categories)

    fig, ax = plt.subplots(figsize=figsize)
    pct_tc.plot(
        kind="bar",
        stacked=True,
        ax=ax,
        color=[color_map[c] for c in pct_tc.columns],
        edgecolor="white",
        width=0.7,
        legend=False,
    )
    ax.set_title("% of ticket count by cost category")
    ax.set_xlabel("Project")
    ax.set_ylabel("% of total")
    ax.set_ylim(0, 110)  # headroom for n=... annotations
    ax.set_yticks([0, 25, 50, 75, 100])
    ax.tick_params(axis="x", rotation=0)
    _annotate_totals(ax, list(pct_tc.index), totals_tc, " tickets")

    handles = [plt.Rectangle((0, 0), 1, 1, color=color_map[c]) for c in categories]
    ax.legend(
        handles,
        categories,
        title="Cost category",
        loc="upper right",
        bbox_to_anchor=(1.0, 1.0),
        frameon=True,
    )

    fig.suptitle(
        title or f"Cost-category breakdown by project (as of {date.today().isoformat()})",
        fontsize=14,
    )
    fig.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    logger.info("Wrote chart to %s", output_path)
    return output_path


def render_pie(
    df: pd.DataFrame,
    project: str,
    output_path: Path,
    *,
    title: str | None = None,
    figsize: tuple[float, float] = (7, 7),
    color_map: dict[str, tuple] | None = None,
) -> Path:
    """
    Render a pie chart of a single project's cost-category mix (ticket
    count) and save a PNG.

    The optional ``color_map`` lets a caller pass in the same palette used
    by the bar chart so categories render in matching colors across all
    artefacts; if omitted, a fresh per-call palette is built.
    """
    if df.empty:
        raise ValueError("Input DataFrame is empty; nothing to plot.")

    project_df = df[df[PROJECT_COL] == project]
    if project_df.empty:
        raise ValueError(
            f"No rows for project {project!r}; available projects: "
            f"{sorted(df[PROJECT_COL].unique())}"
        )

    by_cat = (
        project_df.groupby(CATEGORY_COL)[TICKET_COUNT_COL]
        .sum()
        .astype(float)
        .sort_index()
    )
    # Drop zero-value slices so the pie isn't littered with 0% labels.
    by_cat = by_cat[by_cat > 0]
    if by_cat.empty:
        raise ValueError(f"Project {project!r} has zero tickets across all categories.")

    total_tickets = int(by_cat.sum())
    categories = list(by_cat.index)

    sns.set_theme(style="whitegrid")
    if color_map is None:
        color_map = category_color_map(categories)
    colors = [color_map.get(c) or category_color_map([c])[c] for c in categories]

    fig, ax = plt.subplots(figsize=figsize)
    wedges, _texts, autotexts = ax.pie(
        by_cat.values,
        labels=categories,
        colors=colors,
        autopct=lambda pct: f"{pct:.1f}%",
        startangle=90,
        wedgeprops={"edgecolor": "white", "linewidth": 1.5},
        textprops={"fontsize": 11},
    )
    for t in autotexts:
        t.set_color("white")
        t.set_fontweight("bold")
    ax.set_aspect("equal")

    ax.set_title(
        title
        or f"{project} cost-category mix \u2014 {total_tickets} tickets",
        fontsize=14,
        pad=14,
    )
    fig.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    logger.info("Wrote %s pie chart to %s", project, output_path)
    return output_path


# ---- CLI -------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Render Seaborn cost-category breakdown charts (a project-wide "
            "stacked bar chart and per-project pies) from a labeled-epics CSV."
        )
    )
    p.add_argument("--csv", required=True, help="Path to the labeled-epics CSV.")
    p.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Output directory for PNGs (default: {DEFAULT_OUTPUT_DIR}).",
    )
    p.add_argument(
        "--output",
        default=None,
        help="Explicit output path for the bar chart only (overrides --output-dir).",
    )
    p.add_argument(
        "--pie-projects",
        default=",".join(DEFAULT_PIE_PROJECTS),
        help=(
            "Comma-separated project keys to render pie charts for "
            f"(default: {','.join(DEFAULT_PIE_PROJECTS)}). "
            "Pass an empty string to skip pies."
        ),
    )
    p.add_argument("--title", default=None, help="Override bar-chart title.")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    csv_path = Path(args.csv).expanduser().resolve()
    df = load_epics(csv_path)

    out_dir = Path(args.output_dir).expanduser().resolve()
    today = date.today().strftime("%Y_%m_%d")

    bar_path = (
        Path(args.output).expanduser().resolve()
        if args.output
        else out_dir / f"{today}_cost_category_breakdown.png"
    )
    render_chart(df, bar_path, title=args.title)

    pie_projects = parse_pie_projects(args.pie_projects)
    if pie_projects:
        # Build the shared color map once so pie slice colors match the bar.
        all_cats = sorted(df[CATEGORY_COL].dropna().unique())
        shared_colors = category_color_map(all_cats)
        for project in pie_projects:
            pie_path = out_dir / f"{today}_cost_category_pie_{project.lower()}.png"
            try:
                render_pie(df, project, pie_path, color_map=shared_colors)
            except ValueError as e:
                logger.error("Skipping pie for %s: %s", project, e)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
