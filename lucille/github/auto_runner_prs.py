#!/usr/bin/env python3
"""Find every PR produced by the Frontend repo's auto-runner workflow and
classify its final state, so we can compute a "change defect rate" for the
LLM-driven code agent.

Identification strategy
-----------------------

The auto-runner builds every PR body via ``buildPrBody`` in
``Frontend/.pi/extensions/auto-runner/phases.ts``. That function always
emits an ``## Auto-runner artifacts`` section pointing at the GHA run.
We use the GitHub PR search API with that phrase as the query, then hydrate
each match with full PR + reviews data.

State classification
--------------------

Each PR gets one of these final states:

* ``merged``                — PR was merged. Approved-equivalent for defect-rate purposes.
* ``closed_without_merge``  — PR was closed without merging. Counts as "not approved".
* ``open_approved``         — PR is open and the latest review per reviewer is APPROVED.
* ``open_changes_requested``— PR is open with outstanding change requests. Counts as "not approved".
* ``open_review_pending``   — PR is open with no review decision yet. Excluded from defect rate.

Defect rate (the headline metric)
---------------------------------

``defect_rate = not_approved_count / decided_count`` where
``decided_count = merged + closed_without_merge + open_approved + open_changes_requested``
and ``not_approved_count = closed_without_merge + open_changes_requested``.
PRs still pending review are not counted in either numerator or denominator.

Outputs
-------

Two artifacts under ``--output-dir`` (default ``~/Desktop/debris``):

* ``auto_runner_prs_YYYY_MM_DD.csv`` — one row per PR with state, dates, URL, etc.
* ``auto_runner_prs_YYYY_MM_DD.png`` — donut showing approved / not-approved / pending,
  with the defect-rate headline.

Usage::

    python -m lucille.github.auto_runner_prs ~/bin/github_config.yaml \\
        --repo jarisdev/Frontend \\
        --output-dir ~/Desktop/debris
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
import time
from collections import Counter
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import matplotlib.pyplot as plt
import requests
import yaml

logger = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"
BODY_MARKER = "## Auto-runner artifacts"
SEARCH_QUERY_TEMPLATE = 'repo:{repo} is:pr "{marker}" in:body'

# Final-state labels
S_MERGED = "merged"
S_CLOSED = "closed_without_merge"
S_OPEN_APPROVED = "open_approved"
S_OPEN_CHANGES = "open_changes_requested"
S_OPEN_PENDING = "open_review_pending"

APPROVED_STATES = {S_MERGED, S_OPEN_APPROVED}
NOT_APPROVED_STATES = {S_CLOSED, S_OPEN_CHANGES}
PENDING_STATES = {S_OPEN_PENDING}


@dataclass
class PRRecord:
    number: int
    title: str
    author: str
    created_at: str
    updated_at: str
    closed_at: str
    merged_at: str
    base: str
    head: str
    state: str            # raw GH state ("open"/"closed")
    merged: bool
    final_state: str      # one of the S_* labels
    review_decision: str  # latest aggregate review decision (APPROVED/CHANGES_REQUESTED/PENDING/"")
    reviewers: str        # comma-separated list of reviewers that left a non-COMMENT review
    additions: int
    deletions: int
    changed_files: int
    url: str


def _headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _request_json(url: str, token: str, params: dict | None = None) -> dict:
    """GET with basic rate-limit awareness."""
    for attempt in range(5):
        resp = requests.get(url, headers=_headers(token), params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in (403, 429):
            # Secondary rate limit -- wait per ``X-RateLimit-Reset`` if present.
            reset = resp.headers.get("X-RateLimit-Reset")
            wait = 30
            if reset:
                wait = max(5, int(reset) - int(time.time()) + 2)
            logger.warning("Rate-limited (%s). Sleeping %ds.", resp.status_code, wait)
            time.sleep(min(wait, 120))
            continue
        resp.raise_for_status()
    raise RuntimeError(f"Failed after retries: {url}")


def search_auto_runner_prs(repo: str, token: str) -> Iterator[dict]:
    """Yield search-result items for PRs whose body contains the marker."""
    query = SEARCH_QUERY_TEMPLATE.format(repo=repo, marker=BODY_MARKER)
    page = 1
    while True:
        data = _request_json(
            f"{GITHUB_API}/search/issues",
            token,
            params={"q": query, "per_page": 100, "page": page,
                    "sort": "created", "order": "asc"},
        )
        items = data.get("items", [])
        if not items:
            break
        for item in items:
            yield item
        if len(items) < 100:
            break
        page += 1
        # Search API tops out at 1000 results across 10 pages.
        if page > 10:
            logger.warning("Hit search-API page limit; some PRs may be missing.")
            break


def fetch_pr_detail(repo: str, number: int, token: str) -> dict:
    return _request_json(f"{GITHUB_API}/repos/{repo}/pulls/{number}", token)


def fetch_pr_reviews(repo: str, number: int, token: str) -> list[dict]:
    reviews: list[dict] = []
    page = 1
    while True:
        url = f"{GITHUB_API}/repos/{repo}/pulls/{number}/reviews"
        resp = requests.get(
            url,
            headers=_headers(token),
            params={"per_page": 100, "page": page},
            timeout=30,
        )
        resp.raise_for_status()
        chunk = resp.json()
        if not chunk:
            break
        reviews.extend(chunk)
        if len(chunk) < 100:
            break
        page += 1
    return reviews


def latest_decision(reviews: list[dict]) -> tuple[str, list[str]]:
    """Return (decision, distinct_reviewer_logins).

    GitHub's "files-changed" review API gives us a sequence of reviews per
    user. We take each reviewer's *most recent* non-COMMENT review and
    aggregate: any CHANGES_REQUESTED wins, else any APPROVED wins, else
    PENDING/empty.
    """
    latest_per_user: dict[str, str] = {}
    for r in reviews:
        login = (r.get("user") or {}).get("login")
        state = r.get("state")  # APPROVED | CHANGES_REQUESTED | COMMENTED | DISMISSED
        if not login or not state or state == "COMMENTED":
            continue
        # reviews come ordered chronologically; later overwrites earlier
        latest_per_user[login] = state
    states = set(latest_per_user.values())
    if "CHANGES_REQUESTED" in states:
        decision = "CHANGES_REQUESTED"
    elif "APPROVED" in states:
        decision = "APPROVED"
    elif states:
        decision = "DISMISSED"
    else:
        decision = ""
    reviewers = sorted(latest_per_user.keys())
    return decision, reviewers


def classify(pr: dict, decision: str) -> str:
    if pr.get("merged_at"):
        return S_MERGED
    if pr.get("state") == "closed":
        return S_CLOSED
    # open
    if decision == "APPROVED":
        return S_OPEN_APPROVED
    if decision == "CHANGES_REQUESTED":
        return S_OPEN_CHANGES
    return S_OPEN_PENDING


def collect_records(repo: str, token: str) -> list[PRRecord]:
    records: list[PRRecord] = []
    for item in search_auto_runner_prs(repo, token):
        number = item["number"]
        try:
            pr = fetch_pr_detail(repo, number, token)
            reviews = fetch_pr_reviews(repo, number, token)
        except requests.HTTPError as exc:
            logger.warning("Skipping PR #%d: %s", number, exc)
            continue
        decision, reviewers = latest_decision(reviews)
        final_state = classify(pr, decision)
        records.append(
            PRRecord(
                number=number,
                title=pr.get("title", ""),
                author=(pr.get("user") or {}).get("login", ""),
                created_at=pr.get("created_at", ""),
                updated_at=pr.get("updated_at", ""),
                closed_at=pr.get("closed_at") or "",
                merged_at=pr.get("merged_at") or "",
                base=(pr.get("base") or {}).get("ref", ""),
                head=(pr.get("head") or {}).get("ref", ""),
                state=pr.get("state", ""),
                merged=bool(pr.get("merged_at")),
                final_state=final_state,
                review_decision=decision,
                reviewers=",".join(reviewers),
                additions=pr.get("additions", 0),
                deletions=pr.get("deletions", 0),
                changed_files=pr.get("changed_files", 0),
                url=pr.get("html_url", ""),
            )
        )
        logger.info("  #%d  %-25s  %s", number, final_state, pr.get("title", "")[:60])
    return records


def write_csv(records: list[PRRecord], out_path: Path) -> None:
    if not records:
        out_path.write_text("")  # empty file
        return
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(asdict(records[0]).keys()))
        writer.writeheader()
        for r in records:
            writer.writerow(asdict(r))


def compute_summary(records: list[PRRecord]) -> dict:
    counts = Counter(r.final_state for r in records)
    approved = sum(counts[s] for s in APPROVED_STATES)
    not_approved = sum(counts[s] for s in NOT_APPROVED_STATES)
    pending = sum(counts[s] for s in PENDING_STATES)
    decided = approved + not_approved
    defect_rate = (not_approved / decided) if decided else 0.0
    return {
        "total": len(records),
        "by_state": dict(counts),
        "approved": approved,
        "not_approved": not_approved,
        "pending": pending,
        "decided": decided,
        "defect_rate": defect_rate,
    }


def plot_donut(summary: dict, repo: str, out_path: Path) -> None:
    approved = summary["approved"]
    not_approved = summary["not_approved"]
    pending = summary["pending"]
    total = summary["total"]
    decided = summary["decided"]
    defect_rate = summary["defect_rate"]

    fig, (ax_pie, ax_table) = plt.subplots(
        1, 2, figsize=(12, 6), gridspec_kw={"width_ratios": [1.1, 1]}
    )

    sizes = [approved, not_approved, pending]
    labels = ["Approved\n(merged + open-approved)",
              "Not approved\n(closed + changes-requested)",
              "Pending review"]
    colors = ["#2e7d32", "#c62828", "#9e9e9e"]
    # Drop any zero slices so the donut renders cleanly
    nonzero = [(s, l, c) for s, l, c in zip(sizes, labels, colors) if s > 0]
    if nonzero:
        slice_sizes, slice_labels, slice_colors = zip(*nonzero)
        wedges, _, autotexts = ax_pie.pie(
            slice_sizes,
            labels=slice_labels,
            colors=slice_colors,
            autopct=lambda p: f"{p:.1f}%\n({int(round(p*total/100))})",
            startangle=90,
            wedgeprops={"width": 0.45, "edgecolor": "white"},
            pctdistance=0.78,
            textprops={"fontsize": 10},
        )
        for t in autotexts:
            t.set_color("white")
            t.set_fontweight("bold")
    # Centre annotation: defect rate
    rate_color = "#2e7d32" if defect_rate < 0.20 else (
        "#ef6c00" if defect_rate < 0.50 else "#c62828"
    )
    rate_msg = (
        f"Defect rate\n{defect_rate*100:.1f}%"
        if decided
        else "Defect rate\n(no decided PRs yet)"
    )
    ax_pie.text(0, 0, rate_msg, ha="center", va="center",
                fontsize=15, fontweight="bold", color=rate_color)
    ax_pie.set_title(f"Auto-runner PRs in {repo}\n{total} PRs total, {decided} with a verdict")

    # Right panel: state breakdown table
    ax_table.axis("off")
    rows = [
        ("merged",                   summary["by_state"].get(S_MERGED, 0)),
        ("open & approved",          summary["by_state"].get(S_OPEN_APPROVED, 0)),
        ("closed (no merge)",        summary["by_state"].get(S_CLOSED, 0)),
        ("open & changes requested", summary["by_state"].get(S_OPEN_CHANGES, 0)),
        ("open & review pending",    summary["by_state"].get(S_OPEN_PENDING, 0)),
    ]
    table_text = ["State                              Count"]
    table_text.append("-" * 42)
    for name, n in rows:
        table_text.append(f"{name:<35} {n:>6}")
    table_text.append("-" * 42)
    table_text.append(f"{'TOTAL':<35} {total:>6}")
    table_text.append("")
    if decided:
        verdict = (
            "🎯 Real optimization (<20% defect rate)"
            if defect_rate < 0.20
            else "🧪 Interesting experiment (20–50%)"
            if defect_rate < 0.50
            else "⚠️  Not yet a win (>=50%)"
        )
        table_text.append(verdict)
    ax_table.text(
        0, 1, "\n".join(table_text),
        transform=ax_table.transAxes, va="top", ha="left",
        family="monospace", fontsize=11,
    )

    fig.suptitle("Pi-enabled auto-runner: change defect rate",
                 fontsize=13, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", help="Path to a YAML file with `github_token` (e.g. ~/bin/github_config.yaml)")
    parser.add_argument("--repo", default="jarisdev/Frontend",
                        help="owner/repo to scan (default: jarisdev/Frontend)")
    parser.add_argument("--output-dir", type=Path,
                        default=Path.home() / "Desktop" / "debris")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    cfg_path = Path(args.config).expanduser()
    with cfg_path.open() as f:
        cfg = yaml.safe_load(f)
    token = cfg.get("github_token")
    if not token:
        logger.error("github_token missing from %s", cfg_path)
        return 2

    logger.info("Searching for auto-runner PRs in %s ...", args.repo)
    records = collect_records(args.repo, token)
    logger.info("Found %d PRs total.", len(records))

    out_dir = args.output_dir.expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y_%m_%d")
    csv_path = out_dir / f"{stamp}_auto_runner_prs.csv"
    png_path = out_dir / f"{stamp}_auto_runner_prs.png"

    write_csv(records, csv_path)
    summary = compute_summary(records)
    plot_donut(summary, args.repo, png_path)

    # Echo the summary to stdout
    print()
    print(f"Total auto-runner PRs found: {summary['total']}")
    for state, n in sorted(summary["by_state"].items(), key=lambda kv: -kv[1]):
        print(f"  {state:<28} {n}")
    print()
    if summary["decided"]:
        print(f"Approved:     {summary['approved']}")
        print(f"Not approved: {summary['not_approved']}")
        print(f"Pending:      {summary['pending']}  (excluded from defect rate)")
        print(f"Defect rate:  {summary['defect_rate']*100:.1f}%  "
              f"({summary['not_approved']} / {summary['decided']} decided)")
    else:
        print("No PRs with a verdict yet — defect rate undefined.")
    print()
    print(f"Wrote {csv_path}")
    print(f"Wrote {png_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
