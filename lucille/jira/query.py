import requests
from requests.auth import HTTPBasicAuth
import argparse
import json
import logging
from pathlib import Path, PosixPath
import datetime
from re import sub
import pandas as pd
import yaml


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
    if collected_issues is None:
        collected_issues = []

    query = {"jql": jql, "maxResults": max_results, "fields": fields}

    # Include nextPageToken if provided
    if next_page_token:
        query["nextPageToken"] = next_page_token

    headers = {"Accept": "application/json"}

    response = requests.get(url, headers=headers, params=query, auth=auth)
    if response.status_code != 200:
        raise Exception(
            f"Failed to fetch issues: {response.status_code}, {response.text}"
        )

    data = response.json()
    issues = data.get("issues", [])
    collected_issues.extend(issues)

    logging.debug(f"max_results: {max_results}, next_page_token: {next_page_token}")
    next_page_token = data.get("nextPageToken")
    if next_page_token:
        return get_all_issues(
            url, auth, jql, fields, max_results, next_page_token, collected_issues
        )
    else:
        return collected_issues


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
