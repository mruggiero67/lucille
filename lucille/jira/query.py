import requests
from requests.auth import HTTPBasicAuth
import argparse
import logging
import sys
from pathlib import Path, PosixPath
import datetime
from re import sub
import pandas as pd
import yaml

# Handle both direct script execution and module import
try:
    from .utils import fetch_all_issues
except ImportError:
    # Add parent directory to path for direct script execution
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from lucille.jira.utils import fetch_all_issues


logging.basicConfig(
    format="%(levelname)-10s %(asctime)s %(filename)s %(lineno)d %(message)s",
    level=logging.DEBUG,
)


def get_all_issues(
    url,
    auth,
    jql,
    fields="summary",
    max_results=5000,
    next_page_token=None,
    collected_issues=None,
):
    """
    Legacy function for backward compatibility.
    Now uses the shared utils function internally.
    """
    # Create a session for the API calls
    session = requests.Session()
    session.auth = auth
    session.headers.update({"Accept": "application/json"})

    # Extract base_url from the full URL
    base_url = url.replace("/rest/api/3/search/jql", "")

    # Convert fields string to list if needed
    if isinstance(fields, str):
        fields = [fields]

    try:
        # Use the shared utils function for pagination
        issues = fetch_all_issues(
            session=session,
            base_url=base_url,
            jql=jql,
            fields=fields,
            max_results=max_results
        )

        return issues

    except Exception as e:
        logging.error(f"Error fetching issues: {e}")
        return []


def flatten_issue(issue):
    fields = issue.get("fields", {})
    return {
        "issue_key": issue.get("key"),
        "issue_type": fields.get("issuetype", {}).get("name"),
        "parent_key": fields.get("parent", {}).get("key"),
        "parent_summary": fields.get("parent", {}).get("fields", {}).get("summary", "n/a") if fields.get("parent") else "n/a",
        "parent": fields.get("parent", {}).get("fields", {}).get("summary"),
        "assignee": fields.get("assignee", {}).get("displayName", 'n/a') if fields.get("assignee") else 'n/a',
        "creator": fields.get("creator", {}).get("displayName", 'n/a') if fields.get("creator") else 'n/a',
        "summary": fields.get("summary"),
        "project_key": fields.get("project", {}).get("key"),
        "status": fields.get("statusCategory", {}).get("name"),
        "resolution": fields.get("resolution", {}).get("name") if fields.get("resolution") else "n/a",
        "created": fields.get("created"),
        "updated": fields.get("updated"),
    }


def jira_issues_to_dataframe(jira_data):
    flat_data = [flatten_issue(issue) for issue in jira_data]
    return pd.DataFrame(flat_data)


def snake_case(s: str) -> str:
    # Replace hyphens with spaces, then apply regular expression substitutions for title case conversion
    # and add an underscore between words, finally convert the result to lowercase
    return "_".join(
        sub(
            "([A-Z][a-z]+)",
            r" \1",
            sub("([A-Z]+)", r" \1", s.replace("-", " ").replace("'", "")),
        ).split()
    ).lower()


def canonical_date(dt: datetime) -> str:
    try:
        return dt.strftime("%Y_%m_%d")
    except AttributeError as e:
        raise AttributeError(f"arg '{dt}' not a valid datetime")


def mk_filepath(
    base_dir, label, file_extension, dt=datetime.datetime.now()
) -> PosixPath:
    formatted_date = canonical_date(dt)
    formatted_label = snake_case(label)
    p = Path(base_dir)
    return p / f"{formatted_date}_{formatted_label}{file_extension}"


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Run Jira query stored in a conifg")
    parser.add_argument("config", type=str, help="path to config file")
    parser.add_argument("query", type=str, help="key of the query you want to run")
    args = parser.parse_args()
    config_file = args.config
    query_key = args.query if args.query else "last_month"

    with open(config_file, mode="rt", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    auth = HTTPBasicAuth(config["email"], config["api_token"])
    url = config["url"]
    directory = config.get("directory", "")

    jql = config.get("queries", {}).get(query_key)
    logging.debug(f"query: '{jql}'")

    fields = "*all"
    issues = get_all_issues(url, auth, jql, fields)

    df = jira_issues_to_dataframe(issues)
    print(df.head())
    print(df.shape)

    csv_filepath = mk_filepath(directory, "jira query results", ".csv")
    df.to_csv(csv_filepath, index=False)
