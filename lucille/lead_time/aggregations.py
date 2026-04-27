import logging
import statistics
from datetime import date, timedelta
from typing import Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

MAX_VALID_HOURS = 365 * 24  # 8760


def filter_valid_records(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """
    Remove rows with null, negative, or implausibly large lead_time_hours.
    Returns (clean_df, excluded_count).
    """
    original_len = len(df)
    mask = (
        df["lead_time_hours"].notna()
        & (df["lead_time_hours"] >= 0)
        & (df["lead_time_hours"] <= MAX_VALID_HOURS)
    )
    clean = df[mask].copy()
    excluded = original_len - len(clean)
    if excluded:
        logger.info(f"Excluded {excluded} records with invalid lead_time_hours")
    return clean, excluded


def categorize_performance(median_days: float) -> str:
    """Classify a repo's performance tier based on its median lead time in days."""
    if median_days < 3:
        return "Fast"
    if median_days < 7:
        return "Normal"
    if median_days < 14:
        return "Slow"
    return "Critical"


def _week_starting_sunday(dt) -> date:
    """Return the Sunday on or before the given date."""
    d = dt.date() if hasattr(dt, "date") else dt
    return d - timedelta(days=(d.weekday() + 1) % 7)


def week_label(dt) -> str:
    """Format as 'Week of MM/DD' using Sunday-based week starts."""
    return _week_starting_sunday(dt).strftime("Week of %m/%d")


def compute_weekly_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group records by Sunday-based week and compute median, mean, p75 lead times in days.
    df must have: lead_time_hours (valid floats), deployed_at (datetime).
    Returns DataFrame with columns: week_start, week_label, median_days, mean_days,
    p75_days, change_count — sorted chronologically.
    """
    df = df.copy()
    df["week_start"] = df["deployed_at"].apply(_week_starting_sunday)

    rows = []
    for ws, group in df.groupby("week_start"):
        hours = group["lead_time_hours"].dropna().tolist()
        if not hours:
            continue
        n = len(hours)
        median_h = statistics.median(hours)
        mean_h = statistics.mean(hours)
        p75_h = float(np.percentile(hours, 75)) if n >= 2 else hours[0]
        rows.append({
            "week_start": ws,
            "week_label": ws.strftime("Week of %m/%d"),
            "median_days": round(median_h / 24.0, 1),
            "mean_days": round(mean_h / 24.0, 1),
            "p75_days": round(p75_h / 24.0, 1),
            "change_count": n,
        })

    return pd.DataFrame(rows).sort_values("week_start").reset_index(drop=True)


def compute_repo_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-repository performance stats.
    df must have: repo, deployment_id, lead_time_hours, jira_project.
    Returns DataFrame sorted by changes_count descending with a TOTAL row appended.
    """
    rows = []
    for repo, group in df.groupby("repo"):
        hours = group["lead_time_hours"].dropna().tolist()
        n = len(hours)
        if n == 0:
            continue
        median_d = round(statistics.median(hours) / 24.0, 1)
        mean_d = round(statistics.mean(hours) / 24.0, 1)
        p75_d = round(float(np.percentile(hours, 75)) / 24.0, 1) if n >= 2 else round(hours[0] / 24.0, 1)
        p90_d = round(float(np.percentile(hours, 90)) / 24.0, 1) if n >= 2 else round(hours[0] / 24.0, 1)
        outliers = int(sum(1 for h in hours if h > 14 * 24))
        pct_7d = round(100.0 * sum(1 for h in hours if h <= 168) / n, 1)
        rows.append({
            "repository": repo,
            "changes_count": n,
            "deployments_count": int(group["deployment_id"].nunique()),
            "median_lead_time_days": median_d,
            "average_lead_time_days": mean_d,
            "p75_lead_time_days": p75_d,
            "p90_lead_time_days": p90_d,
            "min_lead_time_days": round(min(hours) / 24.0, 1),
            "max_lead_time_days": round(max(hours) / 24.0, 1),
            "jira_projects": ", ".join(sorted(group["jira_project"].dropna().unique())),
            "performance_category": categorize_performance(median_d),
            "vs_target_7d": pct_7d,
            "outlier_count": outliers,
        })

    result = pd.DataFrame(rows).sort_values("changes_count", ascending=False).reset_index(drop=True)

    if not result.empty:
        all_hours = df["lead_time_hours"].dropna().tolist()
        n_all = len(all_hours)
        total_row = {
            "repository": "TOTAL",
            "changes_count": int(result["changes_count"].sum()),
            "deployments_count": int(df["deployment_id"].nunique()),
            "median_lead_time_days": round(statistics.median(all_hours) / 24.0, 1) if all_hours else None,
            "average_lead_time_days": round(statistics.mean(all_hours) / 24.0, 1) if all_hours else None,
            "p75_lead_time_days": round(float(np.percentile(all_hours, 75)) / 24.0, 1) if n_all >= 2 else None,
            "p90_lead_time_days": round(float(np.percentile(all_hours, 90)) / 24.0, 1) if n_all >= 2 else None,
            "min_lead_time_days": round(min(all_hours) / 24.0, 1) if all_hours else None,
            "max_lead_time_days": round(max(all_hours) / 24.0, 1) if all_hours else None,
            "jira_projects": "",
            "performance_category": "",
            "vs_target_7d": round(100.0 * sum(1 for h in all_hours if h <= 168) / n_all, 1) if n_all else None,
            "outlier_count": int(sum(1 for h in all_hours if h > 14 * 24)),
        }
        result = pd.concat([result, pd.DataFrame([total_row])], ignore_index=True)

    return result
