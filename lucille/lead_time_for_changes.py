"""
DORA Lead Time for Changes — weekly KPI reporting.

Deployment-back methodology: GitHub releases → commit diffs → Jira ticket changelogs
→ lead time from first "In Progress" to deployment.

Usage:
    python -m lucille.lead_time_for_changes [--since YYYY-MM-DD] [--dry-run] [--help]
"""
import argparse
import csv
import logging
import random
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import yaml

try:
    from lucille.github.commit_fetcher import (
        DEFAULT_TICKET_PATTERN,
        extract_project_key,
        fetch_all_releases_with_commits,
    )
    from lucille.github.github_utils import fetch_org_repos
    from lucille.jira.ticket_changelog import fetch_ticket_start_dates
    from lucille.jira.utils import create_jira_session
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from lucille.github.commit_fetcher import (
        DEFAULT_TICKET_PATTERN,
        extract_project_key,
        fetch_all_releases_with_commits,
    )
    from lucille.github.github_utils import fetch_org_repos
    from lucille.jira.ticket_changelog import fetch_ticket_start_dates
    from lucille.jira.utils import create_jira_session

logging.basicConfig(
    format="%(levelname)-8s %(asctime)s %(filename)s:%(lineno)d %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

DEFAULT_GITHUB_CONFIG = Path.home() / "bin" / "github_config.yaml"
DEFAULT_JIRA_EPIC_CONFIG = Path.home() / "bin" / "jira_epic_config.yaml"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class ChangeRecord:
    deployment_id: str      # "{repo}/{version}"
    repo: str
    version: str
    deployed_at: datetime
    commit_sha: str
    ticket_key: str
    jira_project: str
    ticket_started: Optional[datetime]
    lead_time_hours: Optional[float]


@dataclass
class WeeklyMetrics:
    week_start: date
    deployment_count: int
    change_count: int
    unmapped_commits: int
    median_lead_time_hours: Optional[float]
    p75_lead_time_hours: Optional[float]
    p90_lead_time_hours: Optional[float]
    mean_lead_time_hours: Optional[float]


@dataclass
class WeeklyProjectMetrics:
    week_start: date
    jira_project: str
    deployment_count: int
    change_count: int
    unmapped_commits: int   # always 0; unmapped commits cannot be attributed to a project
    median_lead_time_hours: Optional[float]
    p75_lead_time_hours: Optional[float]
    p90_lead_time_hours: Optional[float]
    mean_lead_time_hours: Optional[float]


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------

def calculate_lead_time_hours(deployed_at: datetime, ticket_started: datetime) -> float:
    return (deployed_at - ticket_started).total_seconds() / 3600.0


def compute_percentile(values: List[float], pct: int) -> float:
    return statistics.quantiles(values, n=100)[pct - 1]


def _iso_week_start(dt: datetime) -> date:
    d = dt.date()
    return d - timedelta(days=d.weekday())


def _compute_stats(
    lead_times: List[float],
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Return (median, p75, p90, mean), or all-None if no data."""
    if not lead_times:
        return None, None, None, None
    if len(lead_times) == 1:
        v = lead_times[0]
        return v, v, v, v
    return (
        statistics.median(lead_times),
        compute_percentile(lead_times, 75),
        compute_percentile(lead_times, 90),
        statistics.mean(lead_times),
    )


def build_change_records(
    deployments: List[Dict[str, Any]],
    ticket_start_dates: Dict[str, datetime],
) -> List[ChangeRecord]:
    """
    Join deployment+commit data with ticket start dates.
    Creates one ChangeRecord per (deployment, commit, ticket_key) triple.
    Commits with no ticket keys produce no records (counted as unmapped_commits in aggregates).
    Records for tickets absent from ticket_start_dates have lead_time_hours=None.
    """
    records: List[ChangeRecord] = []
    for dep in deployments:
        dep_id = f"{dep['repo']}/{dep['version']}"
        for commit in dep["commits"]:
            for ticket_key in commit["ticket_keys"]:
                ticket_start = ticket_start_dates.get(ticket_key)
                lead_time = (
                    calculate_lead_time_hours(dep["deployed_at"], ticket_start)
                    if ticket_start is not None
                    else None
                )
                records.append(
                    ChangeRecord(
                        deployment_id=dep_id,
                        repo=dep["repo"],
                        version=dep["version"],
                        deployed_at=dep["deployed_at"],
                        commit_sha=commit["sha"],
                        ticket_key=ticket_key,
                        jira_project=extract_project_key(ticket_key),
                        ticket_started=ticket_start,
                        lead_time_hours=lead_time,
                    )
                )
    return records


def aggregate_weekly_metrics(
    records: List[ChangeRecord],
    deployments: List[Dict[str, Any]],
) -> List[WeeklyMetrics]:
    """Group ChangeRecords by ISO week and compute summary statistics."""
    week_lead_times: Dict[date, List[float]] = defaultdict(list)
    week_deployment_ids: Dict[date, set] = defaultdict(set)
    week_change_count: Dict[date, int] = defaultdict(int)

    for rec in records:
        ws = _iso_week_start(rec.deployed_at)
        week_deployment_ids[ws].add(rec.deployment_id)
        week_change_count[ws] += 1
        if rec.lead_time_hours is not None:
            week_lead_times[ws].append(rec.lead_time_hours)

    week_unmapped: Dict[date, int] = defaultdict(int)
    for dep in deployments:
        ws = _iso_week_start(dep["deployed_at"])
        week_deployment_ids[ws].add(f"{dep['repo']}/{dep['version']}")
        for commit in dep["commits"]:
            if not commit["ticket_keys"]:
                week_unmapped[ws] += 1

    all_weeks = sorted(week_deployment_ids)
    result = []
    for ws in all_weeks:
        median, p75, p90, mean = _compute_stats(week_lead_times[ws])
        result.append(
            WeeklyMetrics(
                week_start=ws,
                deployment_count=len(week_deployment_ids[ws]),
                change_count=week_change_count[ws],
                unmapped_commits=week_unmapped[ws],
                median_lead_time_hours=median,
                p75_lead_time_hours=p75,
                p90_lead_time_hours=p90,
                mean_lead_time_hours=mean,
            )
        )
    return result


def aggregate_weekly_metrics_by_project(
    records: List[ChangeRecord],
    deployments: List[Dict[str, Any]],
) -> List[WeeklyProjectMetrics]:
    """Group ChangeRecords by (ISO week, jira_project) and compute per-project statistics."""
    key_lead_times: Dict[Tuple[date, str], List[float]] = defaultdict(list)
    key_deployment_ids: Dict[Tuple[date, str], set] = defaultdict(set)
    key_change_count: Dict[Tuple[date, str], int] = defaultdict(int)

    for rec in records:
        ws = _iso_week_start(rec.deployed_at)
        k = (ws, rec.jira_project)
        key_deployment_ids[k].add(rec.deployment_id)
        key_change_count[k] += 1
        if rec.lead_time_hours is not None:
            key_lead_times[k].append(rec.lead_time_hours)

    result = []
    for (ws, project) in sorted(key_deployment_ids):
        k = (ws, project)
        median, p75, p90, mean = _compute_stats(key_lead_times[k])
        result.append(
            WeeklyProjectMetrics(
                week_start=ws,
                jira_project=project,
                deployment_count=len(key_deployment_ids[k]),
                change_count=key_change_count[k],
                unmapped_commits=0,
                median_lead_time_hours=median,
                p75_lead_time_hours=p75,
                p90_lead_time_hours=p90,
                mean_lead_time_hours=mean,
            )
        )
    return result


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config(
    github_config_path: Path = DEFAULT_GITHUB_CONFIG,
    jira_epic_config_path: Path = DEFAULT_JIRA_EPIC_CONFIG,
) -> Dict[str, Any]:
    def _load(p: Path) -> Dict:
        try:
            with open(p) as f:
                return yaml.safe_load(f) or {}
        except FileNotFoundError:
            logger.error(f"Config file not found: {p}")
            sys.exit(1)

    gh = _load(github_config_path)
    epic = _load(jira_epic_config_path)

    token = gh.get("github_token")
    org = gh.get("org")
    if not token or not org:
        logger.error("github_token and org are required in github_config.yaml")
        sys.exit(1)

    ltfc = epic.get("lead_time_for_changes", {})
    jira_cfg = epic.get("jira", {})
    dev_statuses = epic.get("development_statuses", ["In Progress", "In Development"])

    return {
        "token": token,
        "org": org,
        "jira": jira_cfg,
        "dev_statuses": dev_statuses,
        "since_date": ltfc.get("since_date"),
        "ticket_pattern": ltfc.get("ticket_pattern", DEFAULT_TICKET_PATTERN),
        "weeks_back": ltfc.get("weeks_back", 12),
        "output_directory": Path(
            ltfc.get("output_directory", str(Path.home() / "Desktop" / "debris"))
        ),
        "chart_output_directory": Path(
            ltfc.get(
                "chart_output_directory",
                str(Path.home() / "Desktop" / "debris" / "2x2" / "lead_time"),
            )
        ),
    }


def _resolve_since(since_arg: Optional[str], cfg: Dict[str, Any]) -> datetime:
    if since_arg:
        d = datetime.strptime(since_arg, "%Y-%m-%d")
    elif cfg.get("since_date"):
        d = datetime.strptime(str(cfg["since_date"]), "%Y-%m-%d")
    else:
        d = datetime.now() - timedelta(weeks=cfg["weeks_back"])
    return d.replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Output: CSV writers
# ---------------------------------------------------------------------------

_DETAILED_FIELDS = [
    "deployment_id", "repo", "version", "deployed_at", "commit_sha",
    "ticket_key", "jira_project", "ticket_started", "lead_time_hours",
]
_WEEKLY_FIELDS = [
    "week_start", "deployment_count", "change_count", "unmapped_commits",
    "median_lead_time_hrs", "p75_lead_time_hrs", "p90_lead_time_hrs", "mean_lead_time_hrs",
]
_WEEKLY_PROJECT_FIELDS = [
    "week_start", "jira_project", "deployment_count", "change_count", "unmapped_commits",
    "median_lead_time_hrs", "p75_lead_time_hrs", "p90_lead_time_hrs", "mean_lead_time_hrs",
]


def _fdt(dt: Optional[datetime]) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else ""


def _ff(val: Optional[float]) -> str:
    return f"{val:.2f}" if val is not None else ""


def _datestamp() -> str:
    return datetime.now().strftime("%Y_%m_%d")


def write_detailed_csv(records: List[ChangeRecord], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{_datestamp()}_lead_time_changes_detailed.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_DETAILED_FIELDS)
        writer.writeheader()
        for r in records:
            writer.writerow(
                {
                    "deployment_id": r.deployment_id,
                    "repo": r.repo,
                    "version": r.version,
                    "deployed_at": _fdt(r.deployed_at),
                    "commit_sha": r.commit_sha,
                    "ticket_key": r.ticket_key,
                    "jira_project": r.jira_project,
                    "ticket_started": _fdt(r.ticket_started),
                    "lead_time_hours": _ff(r.lead_time_hours),
                }
            )
    logger.info(f"Detailed CSV: {path}  ({len(records)} rows)")
    return path


def write_weekly_summary_csv(weekly: List[WeeklyMetrics], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{_datestamp()}_lead_time_changes_weekly.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_WEEKLY_FIELDS)
        writer.writeheader()
        for m in weekly:
            writer.writerow(
                {
                    "week_start": m.week_start.isoformat(),
                    "deployment_count": m.deployment_count,
                    "change_count": m.change_count,
                    "unmapped_commits": m.unmapped_commits,
                    "median_lead_time_hrs": _ff(m.median_lead_time_hours),
                    "p75_lead_time_hrs": _ff(m.p75_lead_time_hours),
                    "p90_lead_time_hrs": _ff(m.p90_lead_time_hours),
                    "mean_lead_time_hrs": _ff(m.mean_lead_time_hours),
                }
            )
    logger.info(f"Weekly summary CSV: {path}  ({len(weekly)} rows)")
    return path


def write_weekly_project_csv(
    weekly: List[WeeklyProjectMetrics], output_dir: Path
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{_datestamp()}_lead_time_changes_weekly_by_project.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_WEEKLY_PROJECT_FIELDS)
        writer.writeheader()
        for m in weekly:
            writer.writerow(
                {
                    "week_start": m.week_start.isoformat(),
                    "jira_project": m.jira_project,
                    "deployment_count": m.deployment_count,
                    "change_count": m.change_count,
                    "unmapped_commits": m.unmapped_commits,
                    "median_lead_time_hrs": _ff(m.median_lead_time_hours),
                    "p75_lead_time_hrs": _ff(m.p75_lead_time_hours),
                    "p90_lead_time_hrs": _ff(m.p90_lead_time_hours),
                    "mean_lead_time_hrs": _ff(m.mean_lead_time_hours),
                }
            )
    logger.info(f"Weekly by-project CSV: {path}  ({len(weekly)} rows)")
    return path


# ---------------------------------------------------------------------------
# Output: charts
# ---------------------------------------------------------------------------

def write_project_chart(
    project: str,
    weekly: List[WeeklyProjectMetrics],
    output_dir: Path,
) -> Optional[Path]:
    """
    Render a bar chart of median lead time (days) per week for one Jira project.
    Orange markers show the p75 value above each bar. Returns None if no data.
    """
    data = [m for m in weekly if m.median_lead_time_hours is not None]
    if not data:
        logger.warning(f"No lead time data for project {project} — skipping chart")
        return None

    data.sort(key=lambda m: m.week_start)
    week_labels = [m.week_start.strftime("%Y-%m-%d") for m in data]
    medians = [m.median_lead_time_hours / 24.0 for m in data]  # type: ignore[operator]
    p75s = [
        (m.p75_lead_time_hours / 24.0) if m.p75_lead_time_hours is not None else None
        for m in data
    ]

    fig_width = max(8, len(data) * 0.9)
    fig, ax = plt.subplots(figsize=(fig_width, 6))
    x = list(range(len(week_labels)))

    ax.bar(x, medians, color="steelblue", alpha=0.85, label="Median")

    p75_plotted = False
    for i, (med, p75) in enumerate(zip(medians, p75s)):
        if p75 is not None and p75 > med:
            label = "p75" if not p75_plotted else ""
            ax.plot([i, i], [med, p75], color="darkorange", linewidth=2)
            ax.plot(i, p75, marker="^", color="darkorange", markersize=7, label=label)
            p75_plotted = True

    ax.set_xticks(x)
    ax.set_xticklabels(week_labels, rotation=45, ha="right", fontsize=9)
    ax.set_xlabel("Week starting")
    ax.set_ylabel("Lead time (days)")
    ax.set_title(f"Lead Time for Changes — {project} (weekly median)")
    ax.legend(loc="upper left")

    total_changes = sum(m.change_count for m in data)
    ax.annotate(
        f"p75 markers shown  ·  n={total_changes} changes over period",
        xy=(0.01, 0.97),
        xycoords="axes fraction",
        fontsize=8,
        va="top",
        color="gray",
    )

    plt.tight_layout()
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{_datestamp()}_lead_time_{project}_weekly.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Chart saved: {path}")
    return path


def write_all_project_charts(
    weekly_by_project: List[WeeklyProjectMetrics], output_dir: Path
) -> List[Path]:
    """Render one chart per discovered Jira project."""
    projects = sorted({m.jira_project for m in weekly_by_project})
    paths = []
    for project in projects:
        project_data = [m for m in weekly_by_project if m.jira_project == project]
        path = write_project_chart(project, project_data, output_dir)
        if path:
            paths.append(path)
    return paths


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Calculate DORA Lead Time for Changes from GitHub releases + Jira tickets."
    )
    p.add_argument(
        "--since",
        default=None,
        metavar="YYYY-MM-DD",
        help="Fetch releases on or after this date (default: today minus weeks_back from config)",
    )
    p.add_argument(
        "--repos",
        nargs="+",
        default=None,
        metavar="REPO",
        help="Restrict to specific repos (default: all non-archived jarisdev repos)",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Override CSV output directory",
    )
    p.add_argument(
        "--chart-output-dir",
        type=Path,
        default=None,
        help="Override chart output directory",
    )
    p.add_argument(
        "--github-config",
        type=Path,
        default=DEFAULT_GITHUB_CONFIG,
        help=f"Path to github_config.yaml (default: {DEFAULT_GITHUB_CONFIG})",
    )
    p.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_JIRA_EPIC_CONFIG,
        help=f"Path to jira_epic_config.yaml (default: {DEFAULT_JIRA_EPIC_CONFIG})",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip writing files; log what would be produced",
    )
    p.add_argument(
        "--test-mode",
        action="store_true",
        help="Run the full pipeline against one randomly chosen repo (for smoke-testing)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config(args.github_config, args.config)

    output_dir = args.output_dir or cfg["output_directory"]
    chart_dir = args.chart_output_dir or cfg["chart_output_directory"]
    since = _resolve_since(args.since, cfg)

    # Step 1: enumerate repos
    repos = args.repos or fetch_org_repos(cfg["org"], cfg["token"])
    if not repos:
        logger.error("No repos found — check GitHub config and token scope.")
        sys.exit(1)

    if args.test_mode:
        chosen = random.choice(repos)
        logger.info(f"TEST MODE: randomly selected repo '{chosen}' ({len(repos)} repos available)")
        repos = [chosen]

    logger.info(f"Scanning {len(repos)} repo(s) since {since.date()}")

    # Step 2: fetch releases + commits
    deployments = fetch_all_releases_with_commits(
        cfg["token"], cfg["org"], repos, since, cfg["ticket_pattern"]
    )
    if not deployments:
        logger.warning("No deployments found in the given window.")
        return

    # Step 3: collect all referenced ticket keys
    all_ticket_keys = sorted(
        {
            key
            for dep in deployments
            for commit in dep["commits"]
            for key in commit["ticket_keys"]
        }
    )
    logger.info(f"Unique ticket keys found: {len(all_ticket_keys)}")

    # Step 4: fetch Jira start dates
    jira_session = create_jira_session(
        cfg["jira"]["base_url"],
        cfg["jira"]["username"],
        cfg["jira"]["api_token"],
    )
    ticket_start_dates = fetch_ticket_start_dates(
        jira_session, cfg["jira"]["base_url"], all_ticket_keys, cfg["dev_statuses"]
    )

    # Step 5: build change records and aggregate
    records = build_change_records(deployments, ticket_start_dates)
    weekly = aggregate_weekly_metrics(records, deployments)
    weekly_by_project = aggregate_weekly_metrics_by_project(records, deployments)

    logger.info(
        f"Results: {len(records)} change records across "
        f"{len(weekly)} weeks and "
        f"{len({m.jira_project for m in weekly_by_project})} projects"
    )

    if args.dry_run:
        logger.info("--- dry-run: weekly summary ---")
        for w in weekly:
            logger.info(
                f"  {w.week_start}  deploys={w.deployment_count}  "
                f"changes={w.change_count}  unmapped={w.unmapped_commits}  "
                f"median={_ff(w.median_lead_time_hours)}h"
            )
        return

    # Step 6: write outputs
    write_detailed_csv(records, output_dir)
    write_weekly_summary_csv(weekly, output_dir)
    write_weekly_project_csv(weekly_by_project, output_dir)
    chart_paths = write_all_project_charts(weekly_by_project, chart_dir)

    print(f"\nDone.")
    print(f"  Deployments: {len(deployments)}  |  Change records: {len(records)}")
    print(f"  CSVs:   {output_dir}/")
    print(f"  Charts: {len(chart_paths)} PNG(s) in {chart_dir}/")


if __name__ == "__main__":
    main()
