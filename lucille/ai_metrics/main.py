"""AI Metrics — measure the impact of AI-assistant use across GitHub + Jira.

Outputs (all datestamped):
  YYYY_MM_DD_ai_pr_metrics.csv         one row per PR (repo, number, ai_touched,
                                       merged, reverted, ticket keys, ...)
  YYYY_MM_DD_ai_ticket_metrics.csv     one row per Jira ticket linked to those
                                       PRs (In Progress → Done cycle time)
  YYYY_MM_DD_ai_metrics_by_repo.csv    one row per repo (PR volume, AI share,
                                       merge rate), sorted by AI share desc
  YYYY_MM_DD_ai_metrics_summary.txt    headline numbers
  YYYY_MM_DD_ai_metrics.png            4-panel figure: AI-share trend,
                                       merge-rate compare, revert-rate compare,
                                       cycle-time boxplot
  YYYY_MM_DD_ai_metrics_top_repos.png  horizontal bar chart of top-N repos
                                       by AI share (min-PR threshold applied)

Usage:
    python -m lucille.ai_metrics.main --config ~/bin/github_config.yaml
"""

from __future__ import annotations

import argparse
import csv
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import matplotlib.pyplot as plt

from lucille.ai_metrics.analyze import (
    Ratio,
    RepoRow,
    ai_touched_share,
    by_repo_summary,
    compare_ticket_cycle_times,
    merge_rate,
    revert_rate,
    split_by_ai,
    summarize_bucket,
    top_repos_by_ai_share,
    weekly_trend,
)
from lucille.ai_metrics.detect import (
    DEFAULT_AI_SIGNATURES,
    AISignature,
    detect_ai_signatures,
    is_bot_pr,
    is_revert_pr,
)
from lucille.ai_metrics.fetch import PRCache, PRRecord, fetch_all_prs, resolve_reverted_prs
from lucille.ai_metrics.jira_cycle import TicketCycle, fetch_ticket_cycles
from lucille.common.config import load_yaml_config
from lucille.common.logging import setup_logging
from lucille.common.paths import BIN_DIR, DEBRIS_DIR, TWO_X_TWO_DIR
from lucille.github.github_utils import fetch_org_repos
from lucille.github.commit_fetcher import DEFAULT_TICKET_PATTERN, parse_ticket_keys
from lucille.jira.utils import create_jira_session

setup_logging()
logger = logging.getLogger(__name__)

DEFAULT_DAYS = 90
DEFAULT_GITHUB_CONFIG = str(BIN_DIR / "github_config.yaml")
DEFAULT_JIRA_CONFIG = str(BIN_DIR / "jira.yaml")
DEFAULT_OUTPUT_DIR = str(TWO_X_TWO_DIR / "ai_metrics")
DEFAULT_CACHE_DIR = DEBRIS_DIR / "ai_metrics_cache"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def load_ai_signatures(config: dict) -> Sequence[AISignature]:
    """Read ``ai_signatures`` list from config; fall back to defaults."""
    raw = config.get("ai_signatures")
    if not raw:
        return DEFAULT_AI_SIGNATURES
    sigs: List[AISignature] = []
    for entry in raw:
        try:
            sigs.append(AISignature(entry["name"], entry["pattern"]))
        except (KeyError, TypeError):
            logger.warning(f"Skipping malformed ai_signatures entry: {entry!r}")
    return sigs or DEFAULT_AI_SIGNATURES


def resolve_jira_creds(jira_config_path: Path) -> Optional[Tuple[str, str, str]]:
    """Return (base_url, username, api_token) or None if jira.yaml missing."""
    try:
        jira_cfg = load_yaml_config(jira_config_path, on_missing="raise")
    except FileNotFoundError:
        logger.warning(f"Jira config not found at {jira_config_path}; ticket metrics disabled")
        return None
    base = jira_cfg.get("url", "").replace("/rest/api/3/search/jql", "")
    if not base:
        base = "https://jarisinc.atlassian.net"
    return base, jira_cfg.get("email", ""), jira_cfg.get("api_token", "")


# ---------------------------------------------------------------------------
# Enrichment (populate the "derived" fields on PRRecord)
# ---------------------------------------------------------------------------


def enrich_prs(
    records: List[PRRecord],
    signatures: Sequence[AISignature],
    ticket_pattern: str,
) -> Tuple[List[PRRecord], int]:
    """Populate ``ai_touched``, ``ai_signatures``, ``ticket_keys``, and
    ``is_revert`` on each record. Bots are filtered out.

    Returns ``(kept_records, dropped_bots_count)``.
    """
    kept: List[PRRecord] = []
    dropped = 0
    for r in records:
        if is_bot_pr(r.author_login, r.author_type):
            dropped += 1
            continue
        r.ai_signatures = detect_ai_signatures(r.commit_messages, signatures)
        r.ai_touched = bool(r.ai_signatures)
        r.ticket_keys = sorted({
            k for msg in [r.title] + r.commit_messages
            for k in parse_ticket_keys(msg or "", ticket_pattern)
        })
        r.is_revert = is_revert_pr(r.title, r.commit_messages)
        kept.append(r)
    return kept, dropped


# ---------------------------------------------------------------------------
# Ticket-side classification (which tickets are AI-touched?)
# ---------------------------------------------------------------------------


def bucket_tickets_by_ai(
    prs: Sequence[PRRecord],
    cycles: Dict[str, TicketCycle],
) -> Tuple[Dict[str, TicketCycle], Dict[str, TicketCycle]]:
    """Partition ticket cycles into (ai, human)."""
    ai_keys: set = set()
    human_keys: set = set()
    for p in prs:
        target = ai_keys if p.ai_touched else human_keys
        for k in p.ticket_keys:
            target.add(k)
    # A ticket touched by BOTH an AI PR and a human PR counts as AI.
    human_keys -= ai_keys
    ai_bucket = {k: cycles[k] for k in ai_keys if k in cycles}
    human_bucket = {k: cycles[k] for k in human_keys if k in cycles}
    return ai_bucket, human_bucket


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


PR_CSV_COLUMNS = [
    "repo", "number", "title", "author", "state", "merged", "is_revert",
    "ai_touched", "ai_signatures", "created_at", "merged_at", "closed_at",
    "ticket_keys", "reverted_by_pr", "url",
]

TICKET_CSV_COLUMNS = [
    "ticket_key", "ai_touched", "started_at", "done_at", "cycle_time_days",
]

REPO_CSV_COLUMNS = [
    "repo", "prs_opened", "ai_touched", "human_only", "ai_share_pct",
    "merged", "ai_merged", "merge_rate_pct",
]


def write_pr_csv(
    prs: Sequence[PRRecord],
    revert_map: Dict[int, int],
    path: Path,
) -> None:
    # invert revert_map: {original_pr: revert_pr}
    reverted_by: Dict[int, int] = {}
    for revert_pr, original_pr in revert_map.items():
        reverted_by[original_pr] = revert_pr

    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=PR_CSV_COLUMNS)
        writer.writeheader()
        for p in prs:
            writer.writerow({
                "repo": p.repo,
                "number": p.number,
                "title": p.title,
                "author": p.author_login or "",
                "state": p.state,
                "merged": "yes" if p.merged else "no",
                "is_revert": "yes" if p.is_revert else "no",
                "ai_touched": "yes" if p.ai_touched else "no",
                "ai_signatures": ",".join(p.ai_signatures),
                "created_at": p.created_at.isoformat(),
                "merged_at": p.merged_at.isoformat() if p.merged_at else "",
                "closed_at": p.closed_at.isoformat() if p.closed_at else "",
                "ticket_keys": ",".join(p.ticket_keys),
                "reverted_by_pr": reverted_by.get(p.number, ""),
                "url": p.url,
            })
    logger.info(f"Wrote {len(prs)} PR rows to {path}")


def write_ticket_csv(
    ai_cycles: Dict[str, TicketCycle],
    human_cycles: Dict[str, TicketCycle],
    path: Path,
) -> None:
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=TICKET_CSV_COLUMNS)
        writer.writeheader()
        for label, bucket in (("yes", ai_cycles), ("no", human_cycles)):
            for tc in bucket.values():
                writer.writerow({
                    "ticket_key": tc.key,
                    "ai_touched": label,
                    "started_at": tc.started_at.isoformat() if tc.started_at else "",
                    "done_at": tc.done_at.isoformat() if tc.done_at else "",
                    "cycle_time_days": (
                        f"{tc.cycle_time_days:.2f}" if tc.cycle_time_days is not None else ""
                    ),
                })
    logger.info(f"Wrote {len(ai_cycles) + len(human_cycles)} ticket rows to {path}")


def write_repo_csv(rows: Sequence[RepoRow], path: Path) -> None:
    """Write per-repo aggregate metrics. Rows are already sorted by AI share desc."""
    def _pct(x: Optional[float]) -> str:
        return f"{x * 100:.1f}" if x is not None else ""
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=REPO_CSV_COLUMNS)
        writer.writeheader()
        for r in rows:
            writer.writerow({
                "repo": r.repo,
                "prs_opened": r.prs_opened,
                "ai_touched": r.ai_touched,
                "human_only": r.human_only,
                "ai_share_pct": _pct(r.ai_share),
                "merged": r.merged,
                "ai_merged": r.ai_merged,
                "merge_rate_pct": _pct(r.merge_rate),
            })
    logger.info(f"Wrote {len(rows)} repo rows to {path}")


def build_summary(
    prs: List[PRRecord],
    revert_map: Dict[int, int],
    ai_cycles: Dict[str, TicketCycle],
    human_cycles: Dict[str, TicketCycle],
    since: datetime,
    until: datetime,
    dropped_bots: int,
) -> List[str]:
    ai_prs, human_prs = split_by_ai(prs)
    ai_share = ai_touched_share(prs)
    overall_merge = merge_rate(prs)
    ai_merge = merge_rate(ai_prs)
    hu_merge = merge_rate(human_prs)
    overall_revert = revert_rate(prs, revert_map.values())
    ai_revert = revert_rate(ai_prs, revert_map.values())
    hu_revert = revert_rate(human_prs, revert_map.values())

    ai_days = [tc.cycle_time_days for tc in ai_cycles.values() if tc.cycle_time_days is not None]
    hu_days = [tc.cycle_time_days for tc in human_cycles.values() if tc.cycle_time_days is not None]
    ai_bucket, hu_bucket = compare_ticket_cycle_times(ai_days, hu_days)

    # Linkability: what fraction of PRs in each bucket carried at least one
    # ticket key? Diverging rates mean the cycle-time comparison below is
    # sampling different populations and should be squinted at.
    ai_linked = Ratio(sum(1 for p in ai_prs if p.ticket_keys), len(ai_prs))
    hu_linked = Ratio(sum(1 for p in human_prs if p.ticket_keys), len(human_prs))

    lines = [
        f"AI-Metrics Report",
        f"Window: {since.date()} — {until.date()} ({(until - since).days} days)",
        f"Bot PRs excluded: {dropped_bots}",
        "",
        f"PRs opened in window: {len(prs)}",
        f"  AI-touched: {ai_share.numerator} ({ai_share.as_percent()})",
        f"  Human-only: {len(human_prs)}",
        "",
        f"Merge rate:",
        f"  Overall: {overall_merge.as_percent()}  ({overall_merge.numerator}/{overall_merge.denominator})",
        f"  AI PRs:  {ai_merge.as_percent()}  ({ai_merge.numerator}/{ai_merge.denominator})",
        f"  Humans:  {hu_merge.as_percent()}  ({hu_merge.numerator}/{hu_merge.denominator})",
        "",
        f"Revert rate (merged PRs reverted later):",
        f"  Overall: {overall_revert.as_percent()}  ({overall_revert.numerator}/{overall_revert.denominator})",
        f"  AI PRs:  {ai_revert.as_percent()}  ({ai_revert.numerator}/{ai_revert.denominator})",
        f"  Humans:  {hu_revert.as_percent()}  ({hu_revert.numerator}/{hu_revert.denominator})",
        "",
        f"PR → Jira linkability (share of PRs with an extractable ticket key):",
        f"  AI PRs: {ai_linked.as_percent()}  ({ai_linked.numerator}/{ai_linked.denominator})",
        f"  Humans: {hu_linked.as_percent()}  ({hu_linked.numerator}/{hu_linked.denominator})",
        f"  (Big gap here means the cycle-time comparison below is biased —",
        f"   we're comparing PR populations with different linkage rates.)",
        "",
        f"Ticket cycle time (In Progress → Done):",
    ]
    for b in (ai_bucket, hu_bucket):
        if b.n == 0:
            lines.append(f"  {b.label:6s}: n=0 (no data)")
        else:
            lines.append(
                f"  {b.label:6s}: n={b.n:<4d} median={b.median_days:.1f}d  "
                f"mean={b.mean_days:.1f}d  p90={b.p90_days:.1f}d"
            )
    return lines


# ---------------------------------------------------------------------------
# Chart
# ---------------------------------------------------------------------------


def render_chart(
    prs: List[PRRecord],
    revert_map: Dict[int, int],
    ai_days: List[float],
    hu_days: List[float],
    output_path: Path,
) -> None:
    weekly = weekly_trend(prs)
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))

    # (1) AI-touched share, weekly trend
    ax = axes[0][0]
    if weekly:
        weeks = [w.week for w in weekly]
        shares = [(w.ai_share or 0) * 100 for w in weekly]
        ax.plot(weeks, shares, marker="o", color="#7B1FA2")
        ax.set_ylabel("% AI-touched")
        ax.set_ylim(0, max(100, max(shares) * 1.1))
        ax.grid(alpha=0.3, linestyle="--")
        ax.tick_params(axis="x", rotation=45)
    ax.set_title("Weekly % of PRs that are AI-touched")

    # (2) Merge rate: AI vs Human
    ax = axes[0][1]
    ai_prs, human_prs = split_by_ai(prs)
    labels = ["AI", "Human"]
    values = [
        (merge_rate(ai_prs).value or 0) * 100,
        (merge_rate(human_prs).value or 0) * 100,
    ]
    bars = ax.bar(labels, values, color=["#2196F3", "#9E9E9E"])
    ax.set_ylabel("Merge rate (%)")
    ax.set_ylim(0, 100)
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    for bar, v in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                f"{v:.1f}%", ha="center", va="bottom", fontweight="bold")
    ax.set_title("Merge rate: AI vs Human")

    # (3) Revert rate: AI vs Human
    ax = axes[1][0]
    reverted = list(revert_map.values())
    values = [
        (revert_rate(ai_prs, reverted).value or 0) * 100,
        (revert_rate(human_prs, reverted).value or 0) * 100,
    ]
    bars = ax.bar(labels, values, color=["#E53935", "#9E9E9E"])
    ax.set_ylabel("Revert rate (%)")
    top = max(values + [1]) * 1.4
    ax.set_ylim(0, top)
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    for bar, v in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                f"{v:.1f}%", ha="center", va="bottom", fontweight="bold")
    ax.set_title("Revert rate: AI vs Human")

    # (4) Cycle-time boxplot
    ax = axes[1][1]
    box_data = [d for d in (ai_days, hu_days)]
    ax.boxplot(box_data, labels=["AI", "Human"], showfliers=False)
    ax.set_ylabel("Cycle time (days)")
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    ax.set_title("Ticket cycle time: AI vs Human (In Progress → Done)")

    fig.suptitle("AI Metrics", fontsize=16, fontweight="bold")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Wrote chart to {output_path}")


def render_top_repos_chart(
    top: Sequence[RepoRow],
    min_prs: int,
    output_path: Path,
) -> None:
    """Horizontal bar chart of the top-N repos by AI share."""
    if not top:
        logger.warning(
            f"No repos with >= {min_prs} PRs; skipping top-repos chart"
        )
        return
    # Reverse so the highest share sits at the top of the chart.
    ordered = list(reversed(top))
    fig, ax = plt.subplots(figsize=(10, max(4, 0.5 * len(ordered) + 2)))
    labels = [r.repo.split("/", 1)[-1] for r in ordered]
    values = [(r.ai_share or 0) * 100 for r in ordered]
    bars = ax.barh(labels, values, color="#7B1FA2", alpha=0.85)
    ax.set_xlabel("% of opened PRs that are AI-touched", fontweight="bold")
    upper = max(100, max(values) * 1.15 if values else 100)
    ax.set_xlim(0, upper)
    ax.grid(axis="x", alpha=0.3, linestyle="--")
    ax.set_title(
        f"Top {len(ordered)} repos by AI adoption (min {min_prs} PRs)",
        fontsize=13, fontweight="bold",
    )
    for bar, r in zip(bars, ordered):
        share_pct = (r.ai_share or 0) * 100
        ax.text(
            bar.get_width() + upper * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"{share_pct:.0f}%  ({r.ai_touched}/{r.prs_opened})",
            va="center", fontsize=9,
        )
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Wrote top-repos chart to {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="AI-assist impact metrics across GitHub + Jira")
    parser.add_argument("--config", default=DEFAULT_GITHUB_CONFIG,
                        help=f"GitHub config YAML (default: {DEFAULT_GITHUB_CONFIG})")
    parser.add_argument("--jira-config", default=DEFAULT_JIRA_CONFIG,
                        help=f"Jira config YAML (default: {DEFAULT_JIRA_CONFIG})")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR,
                        help=f"Where to write CSVs/PNG/summary (default: {DEFAULT_OUTPUT_DIR})")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS,
                        help=f"Analysis window in days (default: {DEFAULT_DAYS})")
    parser.add_argument("--repos", nargs="+",
                        help="Limit to these repo names (default: all non-archived in org)")
    parser.add_argument("--no-cache", action="store_true",
                        help="Force full refresh; ignore ~/Desktop/debris/ai_metrics_cache")
    parser.add_argument("--skip-jira", action="store_true",
                        help="Skip Jira ticket cycle-time analysis")
    parser.add_argument("--min-repo-prs", type=int, default=5,
                        help="Minimum PRs for a repo to appear in the top-repos chart (default: 5)")
    parser.add_argument("--top-repos", type=int, default=10,
                        help="How many repos to show in the top-repos chart (default: 10)")
    args = parser.parse_args()

    gh_config = load_yaml_config(args.config)
    token = gh_config["github_token"]
    org = gh_config["org"]
    ticket_pattern = gh_config.get("ticket_pattern", DEFAULT_TICKET_PATTERN)
    signatures = load_ai_signatures(gh_config)

    repos = args.repos or fetch_org_repos(org, token)
    logger.info(f"Scanning {len(repos)} repos in {org}")

    until = datetime.now(timezone.utc)
    since = until - timedelta(days=args.days)
    logger.info(f"Window: {since.date()} → {until.date()}")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    cache = PRCache(DEFAULT_CACHE_DIR, enabled=not args.no_cache)

    records = fetch_all_prs(token, org, repos, since=since, cache=cache)
    records, dropped = enrich_prs(records, signatures, ticket_pattern)

    # Build SHA index for in-window revert resolution
    sha_index: Dict[Tuple[str, str], int] = {}
    for r in records:
        for sha in r.commit_shas:
            sha_index[(r.repo, sha.lower())] = r.number
    revert_map = resolve_reverted_prs(token, org, records, sha_index)

    # Jira cycle time
    ai_cycles: Dict[str, TicketCycle] = {}
    human_cycles: Dict[str, TicketCycle] = {}
    if not args.skip_jira:
        creds = resolve_jira_creds(Path(args.jira_config))
        if creds:
            base, user, tok = creds
            if user and tok:
                jsession = create_jira_session(base, user, tok)
                # Collect all ticket keys referenced by any PR
                all_keys = sorted({k for r in records for k in r.ticket_keys})
                logger.info(f"Fetching Jira changelogs for {len(all_keys)} tickets")
                cycles = fetch_ticket_cycles(jsession, base, all_keys)
                ai_cycles, human_cycles = bucket_tickets_by_ai(records, cycles)
            else:
                logger.warning("Missing Jira credentials; skipping ticket metrics")

    # Emit
    timestamp = datetime.now().strftime("%Y_%m_%d")
    write_pr_csv(records, revert_map, output_dir / f"{timestamp}_ai_pr_metrics.csv")
    write_ticket_csv(ai_cycles, human_cycles, output_dir / f"{timestamp}_ai_ticket_metrics.csv")

    summary_lines = build_summary(
        records, revert_map, ai_cycles, human_cycles, since, until, dropped,
    )
    summary_path = output_dir / f"{timestamp}_ai_metrics_summary.txt"
    summary_path.write_text("\n".join(summary_lines) + "\n")
    logger.info(f"Wrote summary to {summary_path}")
    print("\n".join(summary_lines))

    ai_days = [tc.cycle_time_days for tc in ai_cycles.values() if tc.cycle_time_days is not None]
    hu_days = [tc.cycle_time_days for tc in human_cycles.values() if tc.cycle_time_days is not None]
    render_chart(records, revert_map, ai_days, hu_days,
                 output_dir / f"{timestamp}_ai_metrics.png")

    # Per-repo breakdown: full CSV of all repos, top-N chart.
    repo_rows = by_repo_summary(records)
    write_repo_csv(repo_rows, output_dir / f"{timestamp}_ai_metrics_by_repo.csv")
    top = top_repos_by_ai_share(repo_rows, min_prs=args.min_repo_prs, limit=args.top_repos)
    render_top_repos_chart(top, args.min_repo_prs,
                           output_dir / f"{timestamp}_ai_metrics_top_repos.png")


if __name__ == "__main__":
    main()
