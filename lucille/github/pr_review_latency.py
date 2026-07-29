#!/usr/bin/env python3
"""PR review latency — a lucille CLI metric.

For each **merged** PR in the last N weeks (default 8), compute:

    review_latency_hours = (first_non_author_non_bot_review_submitted_at
                            - pr.created_at) / 1 hour

Where "first review" is a submission to ``/repos/{o}/{r}/pulls/{n}/reviews``
(GitHub review actions: approve / request changes / comment-review). Top-level
issue comments on the PR are **not** counted — this is "time to first review
submission", not "time to first reviewer engagement".

The metric is filtered to PRs whose base branch has branch protection
requiring at least one approving review (via
``/repos/{o}/{r}/branches/{branch}/protection``). If the token can't read
protection (403), we fall back to counting all merged PRs and log a warning.

Outputs:
  * CSV — one row per merged in-window PR (with per-PR fields for ad-hoc
    analysis — repo, reviewer, latency, base branch, etc.).
  * PNG — a single line chart, org-wide weekly p50 and p90 in hours over the
    trailing N weeks (Sunday-week buckets, keyed by ``created_at``). This is
    the only chart on purpose: one glance answers "is cycle time going up?"
    Anything more granular lives in the CSV.

Config: reuses ``~/bin/github_config.yaml``. An optional ``pr_review_latency``
block controls bot list, branch-protection fallback, and repo scope.

Usage:
    python -m lucille.github.pr_review_latency \\
        --github-config ~/bin/github_config.yaml \\
        [--weeks N] [--repos r1 r2 ...] [--output-dir DIR] [--dry-run]

Activate venv first:
    source ~/venv/basic-pandas/bin/activate
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import seaborn as sns

from lucille.common.config import load_yaml_config
from lucille.common.logging import setup_logging
from lucille.common.paths import BIN_DIR, DEBRIS_DIR, TWO_X_TWO_DIR
from lucille.github.github_utils import fetch_org_repos
from lucille.github.session import GITHUB_API_BASE, create_github_session, paginate

logger = logging.getLogger(__name__)

DEFAULT_GITHUB_CONFIG = BIN_DIR / "github_config.yaml"
DEFAULT_WEEKS = 8
DEFAULT_BOT_LOGINS = (
    "dependabot",
    "renovate",
    "github-actions",
    "copilot-pull-request-reviewer",
)
# Motivating reference line — annotated on the chart.
REFERENCE_HOURS = 18

CSV_COLUMNS = [
    "repo",
    "number",
    "title",
    "author",
    "first_reviewer",
    "created_at",
    "first_review_at",
    "review_latency_hours",
    "base_branch",
    "review_required",
    "merged_at",
    "pr_url",
]


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested; no network)
# ---------------------------------------------------------------------------


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def review_latency_hours(created_at: datetime, first_review_at: datetime) -> float:
    """Return latency in hours between PR creation and first review submission."""
    return (first_review_at - created_at).total_seconds() / 3600.0


def is_bot_login(
    login: Optional[str], user_type: Optional[str], bot_logins: Iterable[str]
) -> bool:
    """Return True if the reviewer looks like a bot.

    A reviewer is considered a bot if:
      * their GitHub user ``type`` is "Bot", OR
      * their login ends with ``[bot]`` (GitHub App convention), OR
      * their login is in the configured bot list.
    """
    if not login:
        return True
    if user_type == "Bot":
        return True
    if login.endswith("[bot]"):
        return True
    return login in set(bot_logins)


def pick_first_review(
    reviews: List[Dict[str, Any]],
    author: str,
    bot_logins: Iterable[str],
) -> Tuple[Optional[datetime], Optional[str]]:
    """Return (submitted_at, login) of the earliest non-author, non-bot review.

    Returns (None, None) if no such review exists.
    """
    candidates: List[Tuple[datetime, str]] = []
    for r in reviews:
        user = r.get("user") or {}
        login = user.get("login")
        if not login or login == author:
            continue
        if is_bot_login(login, user.get("type"), bot_logins):
            continue
        submitted_at = _parse_iso(r.get("submitted_at"))
        if submitted_at is None:
            continue
        candidates.append((submitted_at, login))
    if not candidates:
        return None, None
    candidates.sort(key=lambda t: t[0])
    return candidates[0]


def _week_starting_sunday(dt: datetime) -> date:
    """Return the Sunday on or before the given datetime (mirrors lead_time)."""
    d = dt.date() if hasattr(dt, "date") else dt
    return d - timedelta(days=(d.weekday() + 1) % 7)


def compute_weekly_stats(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """Pool latencies across all repos and bucket by Sunday-week of ``created_at``.

    Only rows with a numeric ``review_latency_hours`` (i.e. actually reviewed
    PRs) contribute to the percentiles. Merged-but-never-reviewed PRs are
    counted in ``awaiting_review`` for that week so a slow-review week isn't
    hidden by zeros.

    Returns DataFrame with columns:
        week_start, week_label, n, awaiting_review, p50_hours, p90_hours
    Sorted chronologically.
    """
    if not rows:
        return pd.DataFrame(
            columns=[
                "week_start",
                "week_label",
                "n",
                "awaiting_review",
                "p50_hours",
                "p90_hours",
            ]
        )

    df = pd.DataFrame(rows)
    df["created_at_dt"] = df["created_at"].apply(
        lambda v: v if isinstance(v, datetime) else _parse_iso(v)
    )
    df["week_start"] = df["created_at_dt"].apply(_week_starting_sunday)

    out = []
    for ws, group in df.groupby("week_start"):
        reviewed = group[group["review_latency_hours"].notna()]
        hours = reviewed["review_latency_hours"].astype(float).tolist()
        n = len(hours)
        awaiting = int(group["review_latency_hours"].isna().sum())
        if n == 0:
            p50 = p90 = None
        elif n == 1:
            p50 = p90 = round(hours[0], 1)
        else:
            p50 = round(float(np.percentile(hours, 50)), 1)
            p90 = round(float(np.percentile(hours, 90)), 1)
        out.append(
            {
                "week_start": ws,
                "week_label": ws.strftime("Week of %m/%d"),
                "n": n,
                "awaiting_review": awaiting,
                "p50_hours": p50,
                "p90_hours": p90,
            }
        )
    return pd.DataFrame(out).sort_values("week_start").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Fetchers (side-effecting; use session)
# ---------------------------------------------------------------------------


def fetch_merged_prs(
    session: requests.Session,
    org: str,
    repo: str,
    cutoff: datetime,
) -> List[Dict[str, Any]]:
    """Fetch merged PRs in ``repo`` created on or after ``cutoff``.

    Uses newest-first pagination on closed PRs so we can hard-break the moment
    we page past the window — no risk of dragging through years of history.
    """
    url = f"{GITHUB_API_BASE}/repos/{org}/{repo}/pulls"
    params = {"state": "closed", "sort": "created", "direction": "desc"}
    out: List[Dict[str, Any]] = []
    for pr in paginate(session, url, params):
        created = _parse_iso(pr.get("created_at"))
        if created is None:
            continue
        if created < cutoff:
            break  # newest-first → we've paged past the window
        if pr.get("merged_at"):
            out.append(pr)
    return out


def fetch_first_review(
    session: requests.Session,
    org: str,
    repo: str,
    number: int,
    author: str,
    bot_logins: Iterable[str],
) -> Tuple[Optional[datetime], Optional[str]]:
    """Return (submitted_at, login) of the earliest qualifying review, or (None, None)."""
    url = f"{GITHUB_API_BASE}/repos/{org}/{repo}/pulls/{number}/reviews"
    reviews = list(paginate(session, url))
    return pick_first_review(reviews, author, bot_logins)


def fetch_review_required(
    session: requests.Session,
    org: str,
    repo: str,
    branch: str,
    cache: Dict[Tuple[str, str], Optional[bool]],
    require_branch_protection: bool,
) -> bool:
    """Return True if ``branch`` requires at least one approving review.

    Cached per (repo, branch). On 403/404 (no protection, or token lacks
    admin:repo scope), we honour ``require_branch_protection``:
      * False → count the PR anyway (treat as required); log a warning.
      * True  → skip the PR (return False).
    """
    key = (repo, branch)
    if key in cache:
        return bool(cache[key])

    url = f"{GITHUB_API_BASE}/repos/{org}/{repo}/branches/{branch}/protection"
    try:
        resp = session.get(url, timeout=30)
    except requests.exceptions.RequestException as e:
        logger.warning(f"[{repo}] branch protection GET failed for {branch}: {e}")
        result = not require_branch_protection
        cache[key] = result
        return result

    if resp.status_code in (403, 404):
        if require_branch_protection:
            logger.info(
                f"[{repo}] no readable branch protection on {branch} "
                f"({resp.status_code}); skipping PRs against this branch."
            )
            cache[key] = False
            return False
        logger.warning(
            f"[{repo}] can't read branch protection on {branch} "
            f"({resp.status_code}); counting merged PRs anyway."
        )
        cache[key] = True
        return True

    resp.raise_for_status()
    body = resp.json() or {}
    required_reviews = body.get("required_pull_request_reviews") or {}
    count = required_reviews.get("required_approving_review_count", 0)
    result = int(count) >= 1
    cache[key] = result
    return result


# ---------------------------------------------------------------------------
# Row assembly
# ---------------------------------------------------------------------------


def build_pr_rows(
    session: requests.Session,
    org: str,
    repo: str,
    merged_prs: List[Dict[str, Any]],
    bot_logins: Iterable[str],
    require_branch_protection: bool,
    protection_cache: Dict[Tuple[str, str], Optional[bool]],
) -> List[Dict[str, Any]]:
    """Turn a list of merged PR payloads into flat CSV-ready rows.

    PRs whose base branch doesn't require review (per config) are dropped.
    """
    rows: List[Dict[str, Any]] = []
    for pr in merged_prs:
        base = ((pr.get("base") or {}).get("ref")) or "main"
        try:
            required = fetch_review_required(
                session, org, repo, base, protection_cache, require_branch_protection
            )
        except Exception as e:
            logger.warning(f"[{repo}] protection check errored for {base}: {e}")
            required = not require_branch_protection
        if not required:
            continue

        author = ((pr.get("user") or {}).get("login")) or ""
        number = int(pr["number"])
        created = _parse_iso(pr["created_at"])
        merged = _parse_iso(pr.get("merged_at"))

        first_at, first_login = fetch_first_review(
            session, org, repo, number, author, bot_logins
        )
        latency = (
            round(review_latency_hours(created, first_at), 2)
            if (created and first_at)
            else None
        )
        rows.append(
            {
                "repo": repo,
                "number": number,
                "title": pr.get("title", ""),
                "author": author,
                "first_reviewer": first_login or "",
                "created_at": created.isoformat() if created else "",
                "first_review_at": first_at.isoformat() if first_at else "",
                "review_latency_hours": latency,
                "base_branch": base,
                "review_required": True,
                "merged_at": merged.isoformat() if merged else "",
                "pr_url": pr.get("html_url", ""),
            }
        )
    return rows


# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------


def write_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in CSV_COLUMNS})
    logger.info(f"CSV written: {path}  ({len(rows)} rows)")


def render_weekly_trend(
    weekly: pd.DataFrame,
    output_path: Path,
    weeks: int,
) -> None:
    """Render one PNG: org-wide weekly p50 and p90 in hours, over N weeks.

    The only chart on purpose. If cycle time is going up, this shows it.
    Per-repo, per-reviewer, per-PR breakdown lives in the CSV alongside.
    """
    sns.set_style("whitegrid")

    fig, ax = plt.subplots(figsize=(12, 7))

    if weekly.empty:
        ax.text(
            0.5,
            0.5,
            "No merged, review-required PRs in the window.",
            ha="center",
            va="center",
            transform=ax.transAxes,
            fontsize=13,
        )
    else:
        w = weekly.copy()
        w["week_start"] = pd.to_datetime(w["week_start"])
        w = w.sort_values("week_start")

        ax.plot(
            w["week_start"],
            w["p50_hours"],
            marker="o",
            linewidth=2.4,
            color="#2c7fb8",
            label="p50 (median)",
        )
        ax.plot(
            w["week_start"],
            w["p90_hours"],
            marker="s",
            linewidth=2.0,
            color="#d95f0e",
            label="p90",
            linestyle="--",
        )

        # 18h reference line
        ax.axhline(
            REFERENCE_HOURS,
            color="#888888",
            linestyle=":",
            linewidth=1.2,
        )
        ax.text(
            w["week_start"].iloc[-1],
            REFERENCE_HOURS,
            f"  {REFERENCE_HOURS}h reference",
            va="center",
            ha="left",
            fontsize=9,
            color="#666666",
        )

        # Weekly n annotated below the axis so nobody over-reads a
        # low-volume week.
        ymin = 0
        ymax_val = max(
            [v for v in list(w["p90_hours"]) + list(w["p50_hours"]) if v is not None]
            + [REFERENCE_HOURS]
        )
        ax.set_ylim(ymin, ymax_val * 1.18)
        for _, row in w.iterrows():
            ax.text(
                row["week_start"],
                ymax_val * 1.10,
                f"n={int(row['n'])}",
                ha="center",
                va="top",
                fontsize=9,
                color="#555555",
            )

        ax.set_xticks(list(w["week_start"]))
        ax.set_xticklabels(
            [ws.strftime("%m/%d") for ws in w["week_start"]],
            rotation=0,
            fontsize=10,
        )
        ax.legend(loc="upper left", frameon=True)

    ax.set_xlabel("Week (Sunday)", fontsize=12)
    ax.set_ylabel("Hours from PR open → first review", fontsize=12)
    ax.set_title(
        f"PR Review Latency (org-wide, last {weeks} weeks)",
        fontsize=14,
        fontweight="bold",
        pad=14,
    )

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Trend chart saved: {output_path}")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def _load_config(github_config_path: Path) -> Dict[str, Any]:
    gh = load_yaml_config(github_config_path, required_keys=("org", "github_token"))
    prl = gh.get("pr_review_latency") or {}
    bot_logins = prl.get("bot_logins") or list(DEFAULT_BOT_LOGINS)
    require_bp = bool(prl.get("require_branch_protection", True))
    repos_cfg = prl.get("repos")  # None → resolve via fetch_org_repos
    output_dir = Path(
        gh.get("pr_output_directory") or (TWO_X_TWO_DIR / "pull_requests")
    )
    return {
        "token": gh["github_token"],
        "org": gh["org"],
        "bot_logins": bot_logins,
        "require_branch_protection": require_bp,
        "repos": repos_cfg,
        "output_dir": output_dir,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Compute org-wide weekly p50/p90 PR review latency for merged, "
            "review-required PRs. Writes one CSV + one PNG."
        )
    )
    p.add_argument(
        "--github-config",
        type=Path,
        default=DEFAULT_GITHUB_CONFIG,
        help=f"github_config.yaml (default: {DEFAULT_GITHUB_CONFIG})",
    )
    p.add_argument(
        "--weeks",
        type=int,
        default=DEFAULT_WEEKS,
        help=f"Rolling window in weeks (default: {DEFAULT_WEEKS})",
    )
    p.add_argument(
        "--repos",
        nargs="+",
        default=None,
        metavar="REPO",
        help="Repos to scan (default: all non-archived org repos, or "
        "pr_review_latency.repos from config)",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="CSV + PNG output directory (default: pr_output_directory from config)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the weekly stats table; write nothing.",
    )
    p.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="DEBUG logging",
    )
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    setup_logging(verbose=args.verbose)

    cfg = _load_config(args.github_config)
    output_dir = args.output_dir or cfg["output_dir"]

    repos = args.repos or cfg["repos"] or fetch_org_repos(cfg["org"], cfg["token"])
    if not repos:
        logger.error("No repos to scan.")
        sys.exit(1)

    cutoff = datetime.now(timezone.utc) - timedelta(weeks=args.weeks)
    logger.info(
        f"Scanning {len(repos)} repos for merged PRs created since {cutoff.isoformat()}"
    )

    session = create_github_session(cfg["token"])
    protection_cache: Dict[Tuple[str, str], Optional[bool]] = {}
    all_rows: List[Dict[str, Any]] = []

    for repo in repos:
        try:
            merged = fetch_merged_prs(session, cfg["org"], repo, cutoff)
            logger.info(f"  {repo}: {len(merged)} merged in-window PRs")
            rows = build_pr_rows(
                session,
                cfg["org"],
                repo,
                merged,
                cfg["bot_logins"],
                cfg["require_branch_protection"],
                protection_cache,
            )
            all_rows.extend(rows)
        except Exception as e:
            logger.warning(f"  {repo}: failed — {e}")

    logger.info(
        f"Collected {len(all_rows)} review-required merged PRs across "
        f"{len(repos)} repos."
    )

    weekly = compute_weekly_stats(all_rows)

    if args.dry_run:
        if weekly.empty:
            print("No data.")
        else:
            print("\nWeekly review latency (org-wide):")
            print(weekly.to_string(index=False))
        print("\n[dry-run] No files written.")
        return

    timestamp = datetime.now().strftime("%Y_%m_%d")
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / f"{timestamp}_pr_review_latency.csv"
    png_path = output_dir / f"{timestamp}_pr_review_latency_trend.png"

    write_csv(all_rows, csv_path)
    render_weekly_trend(weekly, png_path, weeks=args.weeks)

    print(
        f"\nDone. {len(all_rows)} PRs across {len(repos)} repos, last {args.weeks} weeks."
    )
    print(f"CSV: {csv_path}")
    print(f"PNG: {png_path}")


if __name__ == "__main__":
    main()
