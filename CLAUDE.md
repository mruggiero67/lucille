# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Lucille is a Python-based analytics and data extraction toolkit for engineering metrics. It collects data from GitHub, Jira, Slack, and OpsGenie, then transforms and visualizes this data for DORA metrics analysis, sprint reporting, and deployment tracking.

## Core Architecture

### Data Sources & Modules

The codebase is organized around three primary data sources:

**GitHub Analytics** (`lucille/github/`)
- `pr_analyzer.py`: PR age analysis across multiple repositories, calculating urgency and review status
- `fetch_analytics.py`: Multi-repository metrics extraction (commits, deployments, releases, workflow runs)
- `github_actions_success_rate.py`: CI/CD reliability metrics
- All GitHub modules use token-based authentication via YAML configs

**Jira Integration** (`lucille/jira/`)
- `active_sprints.py`: Extracts stories and epics from active sprints across boards
- `epic_completion.py`: Analyzes epic completion rates by examining child story statuses
- `utils.py`: **Shared pagination handler** - uses Jira's `nextPageToken` API (not `startAt/maxResults`)
- `sprint_analyzer.py`, `sprint_cycle_time_analyzer.py`: Sprint metrics and cycle time analysis
- `lead_time_baseline_calculator.py`: Lead time calculations for workflow stages
- Authentication uses HTTPBasicAuth with username/api_token

**Visualization & Processing**
- `deployment_graph.py`: Creates bar charts from CSV data with pandas/seaborn
- `slack_deploys.py`: Parses Slack deployment logs into CSV
- `opsgenie_graph.py`: OpsGenie incident visualization

### Key Design Patterns

**YAML Configuration-Driven**: Nearly all scripts accept a config file path as the primary argument. Config files contain:
- API credentials (GitHub tokens, Jira API tokens)
- Repository/board lists to analyze
- Output directories for CSV/PNG files
- Custom thresholds (e.g., PR age filters, done statuses)

**CSV-First Output**: Scripts generate timestamped CSV files (format: `{type}_{timestamp}.csv`) for downstream analysis in pandas, Google Sheets, or Excel. Some also generate summary CSVs aggregating multiple sources.

**Pagination Handling**:
- **Jira**: Use `utils.fetch_all_issues()` which handles `nextPageToken` pagination (Jira API v3)
- **GitHub**: Use `_paginated_request()` methods that check `Link` headers for `rel="next"`

**Multi-Repository Collection**: GitHub scripts use `MultiRepoMetricsCollector` pattern to iterate over repository lists and aggregate results.

## Common Development Commands

### Running Tests
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=lucille

# Run specific test file
pytest tests/test_pr_analyzer.py
```

### Code Formatting
```bash
# Format code with black
black lucille/

# Run on specific file
black lucille/github/pr_analyzer.py
```

### Running Analysis Scripts

All scripts follow this pattern:
```bash
python lucille/{module}/{script}.py /path/to/config.yaml
```

Examples:
```bash
# GitHub PR analysis
python lucille/github/pr_analyzer.py ~/bin/github_config.yaml

# Jira active sprints
python lucille/jira/active_sprints.py ~/bin/jira_epic_config.yaml

# Epic completion analysis
python lucille/jira/epic_completion.py ~/bin/jira_epic_config.yaml
```

### Using Makefile Targets

The Makefile provides shortcuts for common workflows:
```bash
# List all available targets
make list

# Run specific analysis
make prs              # GitHub PR analysis
make active_sprints   # Jira sprint extraction
make epic_completion  # Epic completion analysis
make deploy_graph     # Create deployment visualization
make opsgenie         # OpsGenie incident analysis
```

Note: Makefile targets reference hardcoded paths in `~/bin/` and `~/Desktop/debris/` - adjust paths or run scripts directly.

## Configuration File Structure

### GitHub Config Example
```yaml
github_token: "ghp_your_token_here"
csv_directory: "./output"
repositories:
  - org: "your-org"
    repo: "repo-name"
  - org: "your-org"
    repo: "another-repo"
# Optional PR filtering
subset_min_days: 7
subset_max_days: 21
subset_columns:
  - repo_name
  - author
  - created_at
  - age_days
  - pr_url
```

### Jira Config Example
```yaml
jira:
  base_url: "https://your-domain.atlassian.net"
  username: "your-email@company.com"
  api_token: "your_jira_api_token"
board_ids:
  - 123
  - 456
output_directory: "./jira_output"
epic_keys_file: "./epic_keys.csv"
done_statuses:
  - "Done"
  - "Closed"
  - "Resolved"
```

## Important Implementation Notes

### Jira Pagination
The Jira API v3 uses token-based pagination, not offset-based. Always use `lucille/jira/utils.py::fetch_all_issues()` for JQL queries. This function:
- Handles `nextPageToken` automatically
- Supports `max_results` limiting
- Returns complete issue lists across all pages
- Handles pagination errors gracefully

### GitHub Rate Limiting
GitHub API requests handle rate limits automatically via `_make_request()` methods. If rate limit is hit, scripts sleep until reset time. Consider this for large multi-repo collections.

### Date Parsing
Multiple date formats exist across data sources. Use appropriate parsers:
- GitHub: ISO 8601 with `dateutil.parser.parse()` or `datetime.fromisoformat()`
- Slack logs: Custom formats in `slack_deploys.py`
- CSV visualization: Configurable parsers in `deployment_graph.py`

### Epic/Story Relationships
Jira epics link to children via: `"Epic Link" = {epic_key} OR parent = {epic_key}`
This JQL pattern captures both legacy "Epic Link" and modern "parent" relationships.

## Testing

Test files are in `tests/` directory:
- `test_pr_analyzer.py`: GitHub PR analysis tests
- `test_fetch_github_analytics.py`: GitHub metrics extractor tests
- `test_lead_time_baseline_calculator.py`: Jira lead time calculation tests
- `context.py`: Test setup helper for imports

Tests use pytest with standard assertions. Mock external API calls using `unittest.mock` or pytest fixtures.

## Dependencies

Key libraries (from requirements.txt):
- `requests`: HTTP clients for GitHub/Jira APIs
- `pandas`: CSV manipulation and data analysis
- `matplotlib`, `seaborn`: Data visualization
- `PyYAML`: Configuration file parsing
- `pytest`, `pytest-cov`: Testing framework
- `black`: Code formatting

Install with: `pip install -r requirements.txt`

## Output Files

All scripts generate timestamped output files in configured directories:
- CSV files: `{type}_{org}_{repo}_{timestamp}.csv` (individual repos)
- Summary CSVs: `summary_{type}_{timestamp}.csv` (aggregated)
- PNG charts: `{chart_title}_{timestamp}.png`
- Log files: Some scripts log to `.log` files in working directory

## Common Patterns for New Features

When adding new analysis scripts:
1. Accept config file path as argparse argument
2. Use YAML for configuration with validation
3. Create timestamped output filenames
4. Log progress with Python logging module (DEBUG level for detailed info)
5. Handle pagination for APIs (use existing utility functions)
6. Generate both summary and detailed CSV outputs
7. Handle API errors gracefully with try/except blocks
8. Write unit tests for new functions, focusing on pure logic
9. Instead of print statements, use logging for all output

When modifying Jira integrations:
- Import and use `utils.fetch_all_issues()` for pagination
- Create a requests.Session for authentication
- Use JQL for flexible querying
- Extract fields explicitly in API params

When modifying GitHub integrations:
- Extend from GitHubMetricsExtractor pattern
- Use `_paginated_request()` for list endpoints
- Handle rate limiting with `_make_request()`
- Support multi-repository analysis via lists
