import requests
from requests.auth import HTTPBasicAuth
import json
import yaml

def get_all_issues(url,
                   auth,
                   jql,
                   fields="summary",
                   max_results=50,
                   start_at=0,
                   collected_issues=None):
    if collected_issues is None:
        collected_issues = []

    query = {
        'jql': jql,
        'startAt': start_at,
        'maxResults': max_results,
        'fields': fields
    }
    headers = {
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers, params=query, auth=auth)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch issues: {response.status_code}, {response.text}")

    data = response.json()
    issues = data.get("issues", [])
    collected_issues.extend(issues)

    if start_at + max_results < data.get("total", 0):
        return get_all_issues(jql, fields, max_results, start_at + max_results, collected_issues)
    else:
        return collected_issues

# Example usage
if __name__ == "__main__":
    CONFIG = "/Users/michael@jaris.io/bin/jira.yaml"

    with open(CONFIG, mode="rt", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    auth = HTTPBasicAuth(config["email"], config["api_token"])
    url = config["url"]

    jql = 'parent=SSJ-753 AND status != "Done"'

    issues = get_all_issues(url, auth, jql)
    print(f"Retrieved {len(issues)} issues.")
    for issue in issues:
        print(issue['key'], "-", issue['fields']['summary'])
