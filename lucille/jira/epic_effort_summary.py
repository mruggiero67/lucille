#!/usr/bin/env python3
"""
epic_effort_summary.py

Summarize engineering effort per epic over the last N days across all Jira projects.
Outputs a CSV for manual cost-category labeling and a markdown summary for review.

Usage:
    python -m lucille.jira.epic_effort_summary
    python -m lucille.jira.epic_effort_summary --days 90 --config ~/bin/jira.yaml
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml

from lucille.jira.utils import create_jira_session, fetch_all_issues

logging.basicConfig(
    format="%(levelname)-8s %(asctime)s %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PROJECTS = ["FED", "DIP", "SUP", "OOT", "SSJ", "JAR"]
STORY_POINTS_FIELD = "customfield_10016"

CHILD_FIELDS = [
    "summary",
    "issuetype",
    "status",
    "project",
    "assignee",
    "created",
    "resolutiondate",
    STORY_POINTS_FIELD,
    "parent",
    "labels",
    "components",
]

EPIC_FIELDS = [
    "summary",
    "status",
    "project",
    "assignee",
    STORY_POINTS_FIELD,
    "labels",
    "created",
    "resolutiondate",
    "issuetype",
]


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_config(config_path):
    with open(config_path, "rt", encoding="utf-8") as f:
        return yaml.safe_load(f)


def extract_base_url(config):
    """Strip any trailing API path from the config url to get the Atlassian root."""
    url = config.get("url", "")
    for suffix in ["/rest/api/3/search/jql", "/rest/api/3/search", "/rest/api/3"]:
        if suffix in url:
            return url.split(suffix)[0]
    return url.rstrip("/")


# ---------------------------------------------------------------------------
# Fetch child issues (stories, tasks, bugs — not epics)
# ---------------------------------------------------------------------------

def fetch_child_issues(session, base_url, cutoff):
    projects_jql = ", ".join(PROJECTS)
    jql = (
        f"project IN ({projects_jql}) "
        f"AND issuetype NOT IN (Epic) "
        f"AND statusCategory = Done "
        f'AND resolved >= "{cutoff}" '
        f"ORDER BY project ASC, resolved DESC"
    )
    logger.info(f"Fetching resolved child issues since {cutoff} ...")
    issues = fetch_all_issues(
        session=session,
        base_url=base_url,
        jql=jql,
        fields=CHILD_FIELDS,
    )
    logger.info(f"  -> {len(issues)} issues found")
    return issues


# ---------------------------------------------------------------------------
# Epic resolution: walk parent chain to find the top-level Epic for each issue
# ---------------------------------------------------------------------------

def _get_parent(issue):
    """Return (parent_key, parent_issuetype) or (None, None)."""
    parent = issue.get("fields", {}).get("parent")
    if not parent:
        return None, None
    key = parent.get("key")
    itype = (parent.get("fields") or {}).get("issuetype", {}).get("name")
    return key, itype


def resolve_epic_keys(issues, session, base_url):
    """
    Return a dict mapping every issue key -> its top-level epic key (or None).

    Direct children of an Epic are resolved in one pass.
    Children of Stories/Tasks need one extra lookup to find their grandparent epic.
    """
    epic_map = {}       # issue_key -> epic_key (or intermediate key placeholder)
    mid_parents = set() # story/task keys we still need to look up

    for issue in issues:
        key = issue["key"]
        parent_key, parent_type = _get_parent(issue)

        if parent_key is None:
            epic_map[key] = None  # orphan
        elif parent_type == "Epic":
            epic_map[key] = parent_key
        else:
            # Parent is a Story, Task, Bug, or Sub-task — record and resolve later
            mid_parents.add(parent_key)
            epic_map[key] = parent_key  # placeholder; replaced below

    if mid_parents:
        logger.info(f"Resolving {len(mid_parents)} intermediate parents to find their epics ...")
        mid_to_epic = {}

        chunks = [list(mid_parents)[i:i + 100] for i in range(0, len(mid_parents), 100)]
        for chunk in chunks:
            keys_jql = ", ".join(chunk)
            parent_issues = fetch_all_issues(
                session=session,
                base_url=base_url,
                jql=f"key IN ({keys_jql})",
                fields=["parent", "issuetype", "summary"],
            )
            for p in parent_issues:
                gp_key, gp_type = _get_parent(p)
                # Accept whatever the grandparent is; best-effort for deep nesting
                mid_to_epic[p["key"]] = gp_key if gp_type == "Epic" else gp_key

        # Replace placeholders in epic_map
        for issue in issues:
            key = issue["key"]
            placeholder = epic_map.get(key)
            if placeholder and placeholder in mid_to_epic:
                epic_map[key] = mid_to_epic[placeholder]

    return epic_map


# ---------------------------------------------------------------------------
# Fetch epic metadata for all discovered epic keys
# ---------------------------------------------------------------------------

def fetch_epic_metadata(epic_keys, session, base_url):
    epic_keys = {k for k in epic_keys if k}
    if not epic_keys:
        return {}

    logger.info(f"Fetching metadata for {len(epic_keys)} epics ...")
    epics = {}

    chunks = [list(epic_keys)[i:i + 100] for i in range(0, len(epic_keys), 100)]
    for chunk in chunks:
        keys_jql = ", ".join(chunk)
        issues = fetch_all_issues(
            session=session,
            base_url=base_url,
            jql=f"key IN ({keys_jql})",
            fields=EPIC_FIELDS,
        )
        for issue in issues:
            f = issue.get("fields", {})
            epics[issue["key"]] = {
                "epic_key": issue["key"],
                "epic_summary": f.get("summary", ""),
                "epic_status": (f.get("status") or {}).get("name", ""),
                "epic_owner": (f.get("assignee") or {}).get("displayName", "unassigned"),
                "project": (f.get("project") or {}).get("key", ""),
                "existing_labels": ", ".join(f.get("labels") or []),
            }

    return epics


# ---------------------------------------------------------------------------
# Flatten raw issue JSON into rows
# ---------------------------------------------------------------------------

def flatten_children(issues, epic_map):
    rows = []
    for issue in issues:
        key = issue["key"]
        f = issue.get("fields", {})
        rows.append({
            "issue_key": key,
            "issue_type": (f.get("issuetype") or {}).get("name", ""),
            "summary": f.get("summary", ""),
            "project": (f.get("project") or {}).get("key", ""),
            "assignee": (f.get("assignee") or {}).get("displayName", "unassigned"),
            "created": (f.get("created") or "")[:10],
            "resolved": (f.get("resolutiondate") or "")[:10],
            "story_points": f.get(STORY_POINTS_FIELD),
            "components": ", ".join(c.get("name", "") for c in (f.get("components") or [])),
            "labels": ", ".join(f.get("labels") or []),
            "epic_key": epic_map.get(key),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Aggregate effort metrics per epic
# ---------------------------------------------------------------------------

def _agg_spec():
    return dict(
        ticket_count=("issue_key", "count"),
        story_points_sum=("story_points", lambda x: x.sum(skipna=True)),
        story_points_estimated=("story_points", lambda x: int(x.notna().sum())),
        assignee_count=("assignee", "nunique"),
        assignees=(
            "assignee",
            lambda x: ", ".join(sorted(set(v for v in x if v and v != "unassigned"))),
        ),
        components=(
            "components",
            lambda x: ", ".join(sorted({c for s in x for c in s.split(", ") if c})),
        ),
        earliest_resolved=("resolved", "min"),
        latest_resolved=("resolved", "max"),
        avg_cycle_days=(
            "cycle_days",
            lambda x: round(float(x.mean()), 1) if x.notna().any() else None,
        ),
    )


def aggregate_by_epic(df):
    df = df.copy()
    df["story_points"] = pd.to_numeric(df["story_points"], errors="coerce")
    df["resolved_dt"] = pd.to_datetime(df["resolved"], errors="coerce")
    df["created_dt"] = pd.to_datetime(df["created"], errors="coerce")
    df["cycle_days"] = (df["resolved_dt"] - df["created_dt"]).dt.days
    df["epic_key_filled"] = df["epic_key"].fillna("(no epic)")

    # Real epics: one row per epic regardless of which projects their children belong to
    epic_rows = (
        df[df["epic_key_filled"] != "(no epic)"]
        .groupby(["epic_key_filled"])
        .agg(**_agg_spec())
        .reset_index()
        .rename(columns={"epic_key_filled": "epic_key"})
    )

    # Orphan tickets: one row per project so cost_category can be assigned per project
    orphan_rows = (
        df[df["epic_key_filled"] == "(no epic)"]
        .groupby(["epic_key_filled", "project"])
        .agg(**_agg_spec())
        .reset_index()
        .rename(columns={"epic_key_filled": "epic_key"})
    )

    return pd.concat([epic_rows, orphan_rows], ignore_index=True)


# ---------------------------------------------------------------------------
# Merge aggregates with epic metadata and finalize column order
# ---------------------------------------------------------------------------

def build_output(agg, epics):
    epic_meta = (
        pd.DataFrame(epics.values())
        if epics
        else pd.DataFrame(
            columns=["epic_key", "epic_summary", "epic_status", "epic_owner",
                     "project", "existing_labels"]
        )
    )

    merged = agg.merge(epic_meta, on="epic_key", how="left", suffixes=("", "_meta"))

    # Use meta project where available, fall back to aggregated project
    if "project_meta" in merged.columns:
        merged["project"] = merged["project_meta"].fillna(merged["project"])
        merged.drop(columns=["project_meta"], inplace=True)

    # Blank column for the user to fill in
    merged["cost_category"] = ""

    columns = [
        "project", "epic_key", "epic_summary", "epic_status", "epic_owner",
        "ticket_count", "story_points_sum", "story_points_estimated",
        "assignee_count", "assignees", "components",
        "earliest_resolved", "latest_resolved", "avg_cycle_days",
        "existing_labels", "cost_category",
    ]
    for col in columns:
        if col not in merged.columns:
            merged[col] = ""

    out = merged[columns].copy()
    out["story_points_sum"] = out["story_points_sum"].fillna(0).astype(int)
    out = out.sort_values(["project", "story_points_sum"], ascending=[True, False])
    return out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Markdown summary
# ---------------------------------------------------------------------------

def write_markdown(df, output_path, cutoff, run_date):
    total_tickets = int(df["ticket_count"].sum())
    total_points = int(df["story_points_sum"].sum())
    n_epics = int((df["epic_key"] != "(no epic)").sum())

    lines = [
        "# Epic Effort Summary",
        "",
        f"**Period:** {cutoff} to {run_date}  ",
        f"**Projects:** {', '.join(PROJECTS)}  ",
        f"**Total named epics:** {n_epics}  ",
        f"**Total tickets resolved:** {total_tickets}  ",
        f"**Total story points:** {total_points}  ",
        "",
        "---",
        "",
        "> **Next step:** Open the CSV, fill in `cost_category` for each row using:",
        "> `revenue` | `reliability` | `platform` | `ops-support`",
        "",
    ]

    for project in sorted(df["project"].dropna().unique()):
        proj_df = df[df["project"] == project]
        proj_tickets = int(proj_df["ticket_count"].sum())
        proj_points = int(proj_df["story_points_sum"].sum())

        lines += [
            f"## {project}",
            "",
            f"_{len(proj_df)} epics · {proj_tickets} tickets · {proj_points} story points_",
            "",
            "| Epic | Summary | Tickets | Story Points | Assignees | Avg Cycle (days) |",
            "|------|---------|--------:|-------------:|-----------|----------------:|",
        ]

        for _, row in proj_df.head(15).iterrows():
            summary = str(row.get("epic_summary") or "").replace("|", "/")
            if len(summary) > 55:
                summary = summary[:52] + "..."
            assignees = str(row.get("assignees") or "")
            if len(assignees) > 35:
                assignees = assignees[:32] + "..."
            lines.append(
                f"| {row['epic_key']} "
                f"| {summary} "
                f"| {row['ticket_count']} "
                f"| {row['story_points_sum']} "
                f"| {assignees} "
                f"| {row.get('avg_cycle_days') or ''} |"
            )

        if len(proj_df) > 15:
            lines.append(f"| _(+{len(proj_df) - 15} more — see CSV)_ | | | | | |")

        lines.append("")

    output_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info(f"Markdown summary written to: {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Summarize Jira epic effort for the last N days."
    )
    parser.add_argument(
        "--config",
        default=str(Path.home() / "bin/jira.yaml"),
        help="Path to jira.yaml (default: ~/bin/jira.yaml)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Lookback window in days (default: 90)",
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path.home() / "Desktop/debris"),
        help="Directory for output files (default: ~/Desktop/debris)",
    )
    args = parser.parse_args()

    run_date = datetime.today().strftime("%Y-%m-%d")
    cutoff = (datetime.today() - timedelta(days=args.days)).strftime("%Y-%m-%d")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_out = output_dir / f"epic_effort_{args.days}d.csv"
    md_out = output_dir / f"epic_effort_{args.days}d_summary.md"

    # --- Auth & session ---
    config = load_config(args.config)
    base_url = extract_base_url(config)
    logger.info(f"Connecting to {base_url} ...")
    session = create_jira_session(base_url, config["email"], config["api_token"])

    # --- Fetch & resolve ---
    issues = fetch_child_issues(session, base_url, cutoff)
    if not issues:
        logger.warning("No resolved issues found in the date window. Exiting.")
        sys.exit(0)

    epic_map = resolve_epic_keys(issues, session, base_url)
    epics = fetch_epic_metadata(set(epic_map.values()), session, base_url)

    # --- Aggregate & build output ---
    children_df = flatten_children(issues, epic_map)
    agg = aggregate_by_epic(children_df)
    output_df = build_output(agg, epics)

    # --- Write ---
    output_df.to_csv(csv_out, index=False)
    logger.info(f"CSV written to: {csv_out}")
    write_markdown(output_df, md_out, cutoff, run_date)

    # --- Console summary ---
    n_epics = int((output_df["epic_key"] != "(no epic)").sum())
    n_with_pts = int((output_df["story_points_sum"] > 0).sum())
    n_orphan_tickets = int(
        output_df.loc[output_df["epic_key"] == "(no epic)", "ticket_count"].sum()
    )

    print(f"""
╔══════════════════════════════════════════════════════════╗
  Epic Effort Summary — {args.days}-day window
  {cutoff}  →  {run_date}
══════════════════════════════════════════════════════════
  Projects scanned : {', '.join(PROJECTS)}
  Resolved tickets : {len(issues)}
  Unique epics     : {n_epics}
    With story pts : {n_with_pts}
  Orphan tickets   : {n_orphan_tickets}
══════════════════════════════════════════════════════════
  Output:
    {csv_out}
    {md_out}
╚══════════════════════════════════════════════════════════╝

Next: open the CSV, fill in `cost_category` for each row, then
pass the labeled file back to Claude for allocation analysis.
""")


if __name__ == "__main__":
    main()
