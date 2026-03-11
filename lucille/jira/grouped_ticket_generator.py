#!/usr/bin/env python3
"""
Grouped Jira ticket generator — one ticket per unique value in a group_by column.

Each ticket's description contains a full ADF table of all rows belonging to
that group. Driven by a job config YAML; supports deduplication via JQL check
before creation.

Usage:
    ~/venv/basic-pandas/bin/python lucille/jira/grouped_ticket_generator.py \\
        --job lucille/jira/jobs/pci_user_role_review.yaml [--dry-run]
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
import yaml

# Handle both direct script execution and module import
try:
    from .ticket_utils import (
        compute_derived_variables,
        load_credentials,
        resolve,
        text_to_adf,
        lookup_account_id,
        create_issue,
    )
    from .utils import create_jira_session
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from lucille.jira.ticket_utils import (
        compute_derived_variables,
        load_credentials,
        resolve,
        text_to_adf,
        lookup_account_id,
        create_issue,
    )
    from lucille.jira.utils import create_jira_session


logging.basicConfig(
    format="%(levelname)-10s %(asctime)s %(filename)s %(lineno)d %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config / IO
# ---------------------------------------------------------------------------

def load_job(path: str) -> dict:
    with Path(path).expanduser().open() as f:
        job = yaml.safe_load(f)
    for key in ("csv", "template", "output_csv"):
        if key in job:
            job[key] = str(Path(job[key]).expanduser())
    required = ["jira", "csv", "template", "group_by", "table_columns"]
    missing = [k for k in required if k not in job]
    if missing:
        raise ValueError(f"Job config missing required key(s): {missing}")
    jira_required = ["project", "issue_type", "summary_template"]
    missing_jira = [k for k in jira_required if k not in job["jira"]]
    if missing_jira:
        raise ValueError(f"Job config missing required jira key(s): {missing_jira}")
    return job


def load_and_group(csv_path: str, group_by_col: str) -> pd.core.groupby.DataFrameGroupBy:
    df = pd.read_csv(csv_path)
    if group_by_col not in df.columns:
        raise ValueError(
            f"group_by column '{group_by_col}' not found in CSV. "
            f"Available columns: {list(df.columns)}"
        )
    return df.groupby(group_by_col, sort=True)


def load_template_body(path: str) -> str:
    return Path(path).expanduser().read_text(encoding="utf-8").strip()


# ---------------------------------------------------------------------------
# ADF table construction
# ---------------------------------------------------------------------------

def _adf_cell(text: str, cell_type: str = "tableCell") -> dict:
    return {
        "type": cell_type,
        "attrs": {},
        "content": [{"type": "paragraph", "content": [{"type": "text", "text": str(text)}]}],
    }


def build_role_table_adf(
    group_df: pd.DataFrame,
    table_columns: list,
    sort_by: Optional[list] = None,
) -> dict:
    """Build an ADF table node from a DataFrame group.

    Args:
        group_df: The supervisor's subset of the CSV.
        table_columns: Column names to include, in order.
        sort_by: Columns to sort by before building rows (defaults to table_columns[0]).
    """
    sort_cols = sort_by or table_columns[:1]
    # Only sort by columns that exist in the DataFrame
    valid_sort = [c for c in sort_cols if c in group_df.columns]
    sorted_df = group_df.sort_values(valid_sort) if valid_sort else group_df

    header_row = {
        "type": "tableRow",
        "content": [_adf_cell(col.title(), "tableHeader") for col in table_columns],
    }

    data_rows = [
        {
            "type": "tableRow",
            "content": [_adf_cell(row[col]) for col in table_columns],
        }
        for _, row in sorted_df.iterrows()
    ]

    return {
        "type": "table",
        "attrs": {"isNumberColumnEnabled": False, "layout": "default"},
        "content": [header_row, *data_rows],
    }


# ---------------------------------------------------------------------------
# ADF description assembly
# ---------------------------------------------------------------------------

def build_adf_description(
    template_body: str,
    group_df: pd.DataFrame,
    ctx: dict,
    supervisor_email: str,
    job: dict,
) -> dict:
    """Assemble the full ADF description doc.

    1. Resolve {placeholders} leniently — {ROLE_TABLE} passes through unchanged.
    2. Split on {ROLE_TABLE} sentinel.
    3. Convert surrounding text blocks to ADF content.
    4. Inject the ADF role table between them.
    """
    vars_ = {**ctx, "supervisor": supervisor_email}
    resolved = resolve(template_body, vars_, {}, strict=False)

    if "{ROLE_TABLE}" not in resolved:
        raise ValueError(
            f"{{ROLE_TABLE}} sentinel not found in template '{job['template']}'. "
            "Add a line containing exactly '{ROLE_TABLE}' where the table should appear."
        )

    before, after = resolved.split("{ROLE_TABLE}", 1)

    before_content = text_to_adf(before.strip())["content"] if before.strip() else []
    after_content = text_to_adf(after.strip())["content"] if after.strip() else []

    role_table = build_role_table_adf(
        group_df,
        job["table_columns"],
        job.get("table_sort_by"),
    )

    return {
        "version": 1,
        "type": "doc",
        "content": [*before_content, role_table, *after_content],
    }


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def check_duplicate(
    supervisor: str,
    ctx: dict,
    session: requests.Session,
    base_url: str,
    project: str,
) -> Optional[str]:
    """Return an existing open ticket key if one already exists, else None.

    Searches for open tickets in `project` whose summary contains both the
    review phrase and the supervisor email. Skips creation if found, to prevent
    duplicates on re-runs.
    """
    jql = (
        f'project = {project} '
        f'AND summary ~ "PCI User Role Review" '
        f'AND summary ~ "{supervisor}" '
        f'AND status != Done'
    )
    resp = session.get(
        f"{base_url}/rest/api/3/search/jql",
        params={"jql": jql, "maxResults": 1, "fields": "summary,status"},
    )
    if not resp.ok:
        logger.warning(f"Dedup check failed for {supervisor} ({resp.status_code}); proceeding with creation.")
        return None
    results = resp.json().get("issues", [])
    if results:
        return results[0]["key"]
    return None


# ---------------------------------------------------------------------------
# Payload
# ---------------------------------------------------------------------------

def build_payload(
    job: dict,
    supervisor_email: str,
    ctx: dict,
    adf_body: dict,
    account_id: Optional[str],
) -> dict:
    jira_cfg = job["jira"]
    vars_ = {**ctx, "supervisor": supervisor_email}

    fields: dict = {
        "project":     {"key": jira_cfg["project"]},
        "issuetype":   {"name": jira_cfg["issue_type"]},
        "summary":     resolve(jira_cfg["summary_template"], vars_, {}),
        "description": adf_body,
    }

    labels = [resolve(lbl, vars_, {}) for lbl in jira_cfg.get("labels", [])]
    if labels:
        fields["labels"] = labels

    if account_id:
        fields["assignee"] = {"accountId": account_id}

    due_date_tmpl = jira_cfg.get("due_date")
    if due_date_tmpl:
        fields["duedate"] = resolve(str(due_date_tmpl), vars_, {})

    priority = jira_cfg.get("priority")
    if priority:
        fields["priority"] = {"name": priority}

    for field_id, tmpl in jira_cfg.get("custom_fields", {}).items():
        fields[field_id] = resolve(tmpl, vars_, {})

    return {"fields": fields}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create one Jira ticket per group from a CSV, with an ADF table of group rows."
    )
    parser.add_argument("--job", required=True, help="Path to job config YAML")
    parser.add_argument(
        "--credentials",
        default=str(Path("~/bin/jira_epic_config.yaml").expanduser()),
        help="Path to Jira credentials YAML (default: ~/bin/jira_epic_config.yaml)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print payloads; skip Jira API calls")
    parser.add_argument("--output-csv", help="Override output CSV path from job config")
    args = parser.parse_args()

    creds = load_credentials(args.credentials)
    job = load_job(args.job)
    dry_run = args.dry_run or job.get("dry_run", False)
    output_csv = args.output_csv or job.get("output_csv")

    ctx = compute_derived_variables(job.get("derived_variables", []), run_date=date.today())
    logger.info(f"Derived context: {ctx}")

    body_template = load_template_body(job["template"])
    if "{ROLE_TABLE}" not in body_template:
        raise ValueError(
            f"{{ROLE_TABLE}} sentinel not found in template '{job['template']}'. "
            "Add a line containing exactly '{ROLE_TABLE}' where the table should appear."
        )

    grouped = load_and_group(job["csv"], job["group_by"])
    logger.info(f"Loaded {grouped.ngroups} groups from '{job['csv']}' (group_by='{job['group_by']}')")

    session = None
    if not dry_run:
        session = create_jira_session(creds["base_url"], creds["username"], creds["api_token"])

    assignee_col = job["jira"].get("assignee_column")
    account_cache: dict = {}
    results = []
    created = no_assignee = skipped = errors = 0

    for supervisor_email, group_df in grouped:
        num_reportees = group_df[job["group_by"]].count()  # rows in group (not unique people)
        num_roles = len(group_df)
        label = supervisor_email

        # Dedup check
        if not dry_run:
            existing_key = check_duplicate(
                supervisor_email, ctx, session, creds["base_url"], job["jira"]["project"]
            )
            if existing_key:
                logger.info(f"Skipping {label} — open ticket already exists: {existing_key}")
                results.append({
                    "supervisor": supervisor_email,
                    "num_roles": num_roles,
                    "jira_key": existing_key,
                    "jira_url": f"{creds['base_url']}/browse/{existing_key}",
                    "status": "skipped_duplicate",
                })
                skipped += 1
                continue

        adf_body = build_adf_description(body_template, group_df, ctx, supervisor_email, job)

        account_id = None
        status = "created"

        if assignee_col:
            email = supervisor_email  # group_by col is the assignee col
            if dry_run:
                account_id = f"<accountId:{email}>"
            else:
                account_id = lookup_account_id(email, session, creds["base_url"], account_cache)
            if not account_id and not dry_run:
                status = "created_no_assignee"

        payload = build_payload(job, supervisor_email, ctx, adf_body, account_id)
        result = create_issue(payload, session, creds["base_url"], dry_run)

        jira_key = result.get("key")
        if jira_key and jira_key != "DRY-RUN":
            jira_url = f"{creds['base_url']}/browse/{jira_key}"
        else:
            jira_url = result.get("self", "")

        if jira_key is None:
            status = "error"
            errors += 1
            logger.error(f"Failed to create ticket for '{label}': {result.get('error')}")
        elif dry_run:
            status = "dry_run"
        elif status == "created_no_assignee":
            no_assignee += 1
            created += 1
        else:
            created += 1

        results.append({
            "supervisor": supervisor_email,
            "num_roles": num_roles,
            "jira_key": jira_key,
            "jira_url": jira_url,
            "status": status,
        })

    logger.info(
        f"Done. Created: {created}  No-assignee: {no_assignee}  "
        f"Skipped (duplicate): {skipped}  Errors: {errors}"
    )

    results_df = pd.DataFrame(results)
    if output_csv:
        out_path = Path(output_csv).expanduser()
        results_df.to_csv(out_path, index=False)
        logger.info(f"Results written to {out_path}")
    else:
        print(results_df.to_string())


if __name__ == "__main__":
    main()
