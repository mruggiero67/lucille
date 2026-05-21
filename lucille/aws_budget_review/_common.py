"""Shared helpers for the AWS budget-review one-off analyses.

The two CSVs exported from AWS Cost Explorer have a peculiar header shape:

    row 0: ``Linked account`` / ``Service``     -> account id / service id (ignored)
    row 1: ``Linked account name`` / ``Service`` -> human-readable column names
    row 2: ``Linked account total`` / ``Service total`` -> per-column totals
    row 3+: ``YYYY-MM-DD`` -> monthly values

This module exposes ``load_account_csv`` and ``load_service_csv`` which return
a tidy ``pandas.DataFrame`` indexed by month, with a ``total`` column dropped
into a separate Series so callers can reason about it explicitly.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class CostFrame:
    """A monthly cost matrix plus its row totals."""

    #: Monthly values indexed by ``pd.Timestamp`` (month start), columns are
    #: account names or service names (the ``Total`` column is removed).
    monthly: pd.DataFrame
    #: Per-month totals as reported in the CSV's ``Total costs`` column.
    monthly_total: pd.Series
    #: Per-column totals from the CSV's ``... total`` row.
    column_totals: pd.Series


def _load(
    csv_path: Path, label_row_name: str, total_row_name: str, skiprows: int = 0
) -> CostFrame:
    # ``skiprows`` lets callers strip a leading numeric-id header row that some
    # AWS Cost Explorer exports include (the account CSV has one, the service
    # CSV does not). After skipping, row 0 of the resulting frame is the
    # human-readable header.
    raw = pd.read_csv(csv_path, skiprows=skiprows, header=0)
    # The first column header is the label column (``Linked account name`` or
    # ``Service``); rename for clarity.
    raw = raw.rename(columns={raw.columns[0]: "label"})

    totals_row = raw[raw["label"] == total_row_name].iloc[0]
    data = raw[~raw["label"].isin([total_row_name])].copy()
    data["label"] = data["label"].astype(str)
    # Keep only rows whose label parses as a date.
    data["month"] = pd.to_datetime(data["label"], errors="coerce")
    data = data.dropna(subset=["month"]).set_index("month").drop(columns=["label"])

    # Coerce everything to floats; blanks become NaN -> 0.
    data = data.apply(pd.to_numeric, errors="coerce").fillna(0.0)

    # The CSVs have a trailing column called either ``Total costs ($)`` or
    # ``Total costs($)``; normalise.
    total_col = next(c for c in data.columns if c.lower().startswith("total costs"))
    monthly_total = data[total_col].copy()
    monthly = data.drop(columns=[total_col])

    column_totals = (
        totals_row.drop(labels=["label"])
        .apply(pd.to_numeric, errors="coerce")
        .fillna(0.0)
        .drop(labels=[total_col])
    )

    # Strip the trailing " ($)" or "($)" from column names for nicer display.
    def _clean(name: str) -> str:
        return (
            name.replace(" ($)", "")
            .replace("($)", "")
            .strip()
        )

    monthly.columns = [_clean(c) for c in monthly.columns]
    column_totals.index = [_clean(c) for c in column_totals.index]

    return CostFrame(monthly=monthly, monthly_total=monthly_total, column_totals=column_totals)


def load_account_csv(csv_path: Path) -> CostFrame:
    # The account CSV starts with a row of numeric AWS account ids; skip it.
    return _load(csv_path, "Linked account name", "Linked account total", skiprows=1)


def load_service_csv(csv_path: Path) -> CostFrame:
    # The service CSV has no id row; the first row is already the header.
    return _load(csv_path, "Service", "Service total", skiprows=0)


def days_in_month(month_start: pd.Timestamp) -> int:
    return (month_start + pd.offsets.MonthEnd(0)).day


def normalize_partial_month(value: float, month_start: pd.Timestamp, elapsed_days: int) -> float:
    """Scale a partial-month value up to a full-month projection."""
    if elapsed_days <= 0:
        return value
    return value * days_in_month(month_start) / elapsed_days
