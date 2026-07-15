"""
Render bar chart(s) of weekly vendor spend from the CSV produced by
``fetch_vendor_spend``.

Three rendering modes:
  * ``per-vendor`` (default): one PNG per vendor, each with its own y-axis.
    Best for spike detection — a vendor's signal isn't dwarfed by a larger
    vendor on a shared scale.
  * ``combined``: a single grouped bar chart with all vendors on one axis.
    Useful for absolute-spend comparison.
  * ``both``: produce both shapes.

Usage:
    python -m lucille.vendor_spend.graph_vendor_spend \\
        --csv ~/Desktop/debris/2026_05_01_vendor_spend.csv

PNG output goes alongside the CSV in the configured output directory:
    per-vendor:  ~/Desktop/debris/YYYY_MM_DD_vendor_spend_<slug>.png
    combined:    ~/Desktop/debris/YYYY_MM_DD_vendor_spend.png
"""

from __future__ import annotations

import argparse
import logging
from datetime import date
from pathlib import Path

import re

import matplotlib

matplotlib.use("Agg")  # no display required
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

from lucille.vendor_spend.config import DEFAULT_CONFIG_PATH, load_config
from lucille.common.logging import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


# ---- pure ------------------------------------------------------------------

def build_dataframe(csv_path: Path) -> pd.DataFrame:
    """Load and normalise the spend CSV into a tidy DataFrame."""
    df = pd.read_csv(csv_path, parse_dates=["week_start"])
    df["week_start"] = df["week_start"].dt.date
    df["amount_usd"] = df["amount_usd"].astype(float)
    return df.sort_values(["week_start", "vendor"]).reset_index(drop=True)


def pivot_for_plot(df: pd.DataFrame) -> pd.DataFrame:
    """Wide form: index=week_start, columns=vendor, values=amount_usd."""
    wide = df.pivot_table(
        index="week_start", columns="vendor", values="amount_usd", aggfunc="sum"
    ).fillna(0.0)
    return wide.sort_index()


def summarise(df: pd.DataFrame) -> dict:
    """Return a JSON-friendly dict useful for tests and logging."""
    wide = pivot_for_plot(df)
    return {
        "weeks": [d.isoformat() for d in wide.index],
        "vendors": list(wide.columns),
        "totals_by_vendor": {v: float(wide[v].sum()) for v in wide.columns},
        "by_vendor": {v: [float(x) for x in wide[v].tolist()] for v in wide.columns},
    }


def vendor_filename_slug(vendor: str) -> str:
    """
    Map a vendor label to a filesystem-safe lowercase slug.

    ``"AWS (Console export)"`` -> ``"aws_console_export"``.
    Pure, unit-tested separately so renaming a vendor doesn't break filenames.
    """
    s = vendor.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "vendor"


# Vendor -> bar color. Brand-leaning hex values so the same vendor reads the
# same across charts and console-export variants.
DEFAULT_VENDOR_COLORS: dict[str, str] = {
    "aws":        "#205081",  # blue
    "datadog":    "#632CA6",  # Datadog brand purple
    "databricks": "#FF3621",  # Databricks brand red
}

# Fallback for unrecognised vendors. Matplotlib mid-gray; visible without
# stealing attention from the named-vendor bars.
FALLBACK_VENDOR_COLOR = "#888888"

# Strip a trailing parenthetical so "AWS" and "AWS (Console export)" share a key.
_PAREN_SUFFIX_RE = re.compile(r"\s*\([^)]*\)\s*$")


def _color_lookup_key(vendor: str) -> str:
    """Normalise a vendor label for color-map lookup. Pure."""
    s = _PAREN_SUFFIX_RE.sub("", vendor or "").strip().lower()
    return s


def color_for_vendor(
    vendor: str,
    *,
    overrides: dict[str, str] | None = None,
    fallback: str = FALLBACK_VENDOR_COLOR,
) -> str:
    """
    Return the bar color for ``vendor``. Pure.

    Lookup order:
      1. ``overrides`` (caller-supplied, keyed by normalised vendor name)
      2. ``DEFAULT_VENDOR_COLORS``
      3. ``fallback``
    """
    key = _color_lookup_key(vendor)
    if overrides:
        # Normalize override keys the same way so callers can pass
        # "AWS" / "aws" / "AWS (foo)" interchangeably.
        norm_overrides = {_color_lookup_key(k): v for k, v in overrides.items()}
        if key in norm_overrides:
            return norm_overrides[key]
    return DEFAULT_VENDOR_COLORS.get(key, fallback)


# ---- side-effecting --------------------------------------------------------

def render_chart(
    df: pd.DataFrame,
    output_path: Path,
    *,
    title: str | None = None,
    figsize: tuple[float, float] = (12, 6),
) -> dict:
    """Draw the grouped bar chart and write a PNG. Returns the summary dict."""
    wide = pivot_for_plot(df)
    if wide.empty:
        raise ValueError("No data to plot; the CSV produced an empty pivot.")

    fig, ax = plt.subplots(figsize=figsize)
    colors = [color_for_vendor(v) for v in wide.columns]
    wide.plot(kind="bar", ax=ax, width=0.8, edgecolor="white", color=colors)

    ax.set_title(
        title
        or f"Weekly vendor spend — last {len(wide)} weeks (as of {date.today().isoformat()})"
    )
    ax.set_xlabel("Week of (Mon–Sun)")
    ax.set_ylabel("USD")
    ax.set_xticklabels([d.isoformat() for d in wide.index], rotation=30, ha="right")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.legend(title="Vendor")
    ax.grid(axis="y", linestyle=":", alpha=0.5)

    # Annotate bars
    for container in ax.containers:
        ax.bar_label(
            container,
            labels=[f"${v:,.0f}" if v else "" for v in container.datavalues],
            fontsize=8,
            padding=2,
        )

    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    logger.info("Wrote chart to %s", output_path)

    return summarise(df)


def render_per_vendor_charts(
    df: pd.DataFrame,
    output_dir: Path,
    base_stem: str,
    *,
    figsize: tuple[float, float] = (10, 4.5),
) -> dict[str, Path]:
    """
    Render one PNG per vendor with its own y-axis, into
    ``{output_dir}/{base_stem}_{vendor_slug}.png``.

    Returns a mapping of vendor -> PNG path so callers can log / verify.
    """
    wide = pivot_for_plot(df)
    if wide.empty:
        raise ValueError("No data to plot; the CSV produced an empty pivot.")

    output_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    week_labels = [d.isoformat() for d in wide.index]

    for vendor in wide.columns:
        values = wide[vendor].tolist()
        slug = vendor_filename_slug(vendor)
        png_path = output_dir / f"{base_stem}_{slug}.png"

        fig, ax = plt.subplots(figsize=figsize)
        bars = ax.bar(
            week_labels,
            values,
            width=0.7,
            edgecolor="white",
            color=color_for_vendor(vendor),
        )
        ax.set_title(
            f"{vendor} — weekly spend ({len(wide)} weeks, as of {date.today().isoformat()})"
        )
        ax.set_xlabel("Week of (Mon–Sun)")
        ax.set_ylabel("USD")
        ax.tick_params(axis="x", rotation=30)
        for label in ax.get_xticklabels():
            label.set_horizontalalignment("right")
        ax.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: f"${x:,.0f}")
        )
        ax.grid(axis="y", linestyle=":", alpha=0.5)
        ax.bar_label(
            bars,
            labels=[f"${v:,.0f}" if v else "" for v in values],
            fontsize=8,
            padding=2,
        )
        # Pad the y-axis a bit so bar labels don't get clipped at the top.
        if any(v > 0 for v in values):
            ax.set_ylim(top=max(values) * 1.12)

        fig.tight_layout()
        fig.savefig(png_path, dpi=150)
        plt.close(fig)
        logger.info("Wrote %s chart to %s", vendor, png_path)
        paths[vendor] = png_path

    return paths


# ---- CLI -------------------------------------------------------------------

def _png_path_for_csv(csv_path: Path, output_dir: Path | None) -> Path:
    """Mirror the CSV's date-prefixed filename, with .png, in output_dir (or alongside the CSV)."""
    stem = csv_path.stem  # e.g. "2026_05_01_vendor_spend"
    target_dir = output_dir or csv_path.parent
    return target_dir / f"{stem}.png"


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Render bar chart(s) from a vendor-spend CSV."
    )
    p.add_argument("--csv", required=True, help="Path to the vendor-spend CSV.")
    p.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to YAML config (default: {DEFAULT_CONFIG_PATH}).",
    )
    p.add_argument(
        "--output",
        default=None,
        help=(
            "Explicit output PNG path (combined mode only; overrides "
            "--output-dir)."
        ),
    )
    p.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for PNGs (default: from YAML).",
    )
    p.add_argument(
        "--mode",
        choices=("per-vendor", "combined", "both"),
        default="per-vendor",
        help=(
            "per-vendor: one PNG per vendor with independent y-axis (default; "
            "best for spike detection). combined: single grouped chart with "
            "all vendors on a shared y-axis. both: do both."
        ),
    )
    p.add_argument("--title", default=None, help="Override combined chart title.")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    csv_path = Path(args.csv).expanduser().resolve()
    cfg = load_config(args.config)

    output_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else cfg.output_dir
    )
    df = build_dataframe(csv_path)

    if args.mode in ("combined", "both"):
        if args.output and args.mode == "combined":
            png_path = Path(args.output).expanduser().resolve()
        else:
            png_path = _png_path_for_csv(csv_path, output_dir)
        render_chart(df, png_path, title=args.title)

    if args.mode in ("per-vendor", "both"):
        render_per_vendor_charts(df, output_dir, base_stem=csv_path.stem)

    summary = summarise(df)
    logger.info("Totals by vendor: %s", summary["totals_by_vendor"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
