#!/usr/bin/env python3
"""
Generic Jira ticket generator driven by a job config YAML.

Each job config specifies a CSV (rows = tickets), a template file (description body),
and Jira field mappings. Placeholders in templates are filled from CSV columns and
a set of built-in derived variables (quarter, year, quarter_end_date, etc.).

Usage:
    ~/venv/basic-pandas/bin/python -m lucille.jira.ticket_generator \\
        --job lucille/jira/jobs/pci_access_review.yaml [--dry-run]

    # or directly:
    ~/venv/basic-pandas/bin/python lucille/jira/ticket_generator.py \\
        --job lucille/jira/jobs/pci_access_review.yaml [--dry-run]
"""

import argparse
import logging
import re
import sys
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml

# Handle both direct script execution and module import
try:
    from .ticket_utils import (
        compute_derived_variables,
        load_credentials,
        _LenientMap,
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
        _LenientMap,
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
    return job


def load_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


def load_template_body(path: str) -> str:
    """Return only the description body from the template (everything after 'Description')."""
    text = Path(path).expanduser().read_text(encoding="utf-8")
    marker = "\nDescription\n"
    if marker in text:
        return text.split(marker, 1)[1].strip()
    # Fallback: strip metadata lines (lines starting with * or the title block)
    lines = [
        line for line in text.splitlines()
        if not line.startswith("*") and line not in ("Template Fields",)
    ]
    return "\n".join(lines).strip()


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_job_templates(job: dict, available: set) -> None:
    """Verify all {placeholders} in job config template strings are resolvable.

    Only checks word-character placeholders (e.g. {resource_name}) since those
    are the ones that will be resolved. Raises KeyError with a clear message on
    the first missing placeholder, before any API calls are made.
    """
    jira_cfg = job.get("jira", {})
    templates_to_check = []

    if "summary_template" in jira_cfg:
        templates_to_check.append(("jira.summary_template", jira_cfg["summary_template"]))
    if "due_date" in jira_cfg:
        templates_to_check.append(("jira.due_date", str(jira_cfg["due_date"])))
    for label in jira_cfg.get("labels", []):
        templates_to_check.append(("jira.labels", label))
    for field_id, tmpl in jira_cfg.get("custom_fields", {}).items():
        templates_to_check.append((f"jira.custom_fields.{field_id}", tmpl))

    placeholder_re = re.compile(r"\{(\w+)\}")
    for location, tmpl in templates_to_check:
        for match in placeholder_re.finditer(tmpl):
            name = match.group(1)
            if name not in available:
                raise KeyError(
                    f"Placeholder '{{{name}}}' in '{location}' has no source. "
                    f"Available variables: {sorted(available)}"
                )


# ---------------------------------------------------------------------------
# Payload
# ---------------------------------------------------------------------------

def build_payload(
    job: dict,
    row: dict,
    ctx: dict,
    adf_body: dict,
    account_id: Optional[str],
) -> dict:
    jira_cfg = job["jira"]

    fields: dict = {
        "project":     {"key": jira_cfg["project"]},
        "issuetype":   {"name": jira_cfg["issue_type"]},
        "summary":     resolve(jira_cfg["summary_template"], row, ctx),
        "description": adf_body,
    }

    labels = [resolve(lbl, row, ctx) for lbl in jira_cfg.get("labels", [])]
    if labels:
        fields["labels"] = labels

    if account_id:
        fields["assignee"] = {"accountId": account_id}

    due_date_tmpl = jira_cfg.get("due_date")
    if due_date_tmpl:
        fields["duedate"] = resolve(str(due_date_tmpl), row, ctx)

    priority = jira_cfg.get("priority")
    if priority:
        fields["priority"] = {"name": priority}

    for field_id, tmpl in jira_cfg.get("custom_fields", {}).items():
        fields[field_id] = resolve(tmpl, row, ctx)

    return {"fields": fields}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create Jira tickets from a CSV + template, driven by a job config YAML."
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

    df = load_csv(job["csv"])
    available_vars = set(df.columns) | set(ctx)

    validate_job_templates(job, available_vars)
    logger.info(f"Template validation passed. Processing {len(df)} rows.")

    body_template = load_template_body(job["template"])

    session = None
    if not dry_run:
        session = create_jira_session(creds["base_url"], creds["username"], creds["api_token"])

    assignee_col = job["jira"].get("assignee_column")
    account_cache: dict = {}
    results = []
    created = no_assignee = errors = 0

    for _, row in df.iterrows():
        row_dict = row.to_dict()
        label = row_dict.get("resource_name", list(row_dict.values())[0])

        filled_body = resolve(body_template, row_dict, ctx, strict=False)
        adf_body = text_to_adf(filled_body)

        account_id = None
        status = "created"

        if assignee_col and assignee_col in row_dict:
            email = row_dict[assignee_col]
            if dry_run:
                account_id = f"<accountId:{email}>"
            else:
                account_id = lookup_account_id(email, session, creds["base_url"], account_cache)
            if not account_id and not dry_run:
                status = "created_no_assignee"

        payload = build_payload(job, row_dict, ctx, adf_body, account_id)
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

        results.append({**row_dict, "jira_key": jira_key, "jira_url": jira_url, "status": status})

    logger.info(f"Done. Created: {created}  No-assignee: {no_assignee}  Errors: {errors}")

    results_df = pd.DataFrame(results)
    if output_csv:
        out_path = Path(output_csv).expanduser()
        results_df.to_csv(out_path, index=False)
        logger.info(f"Results written to {out_path}")
    else:
        print(results_df.to_string())


if __name__ == "__main__":
    main()
