#!/usr/bin/env python3
"""
Config loader for the CFR tool.

Merges settings from three existing ~/bin YAML files into a single dict:
  - ~/bin/jira_epic_config.yaml  → cfr settings, confluence, sprint_boards
  - ~/bin/lead_time_config.yaml  → jira credentials
  - ~/bin/github_config.yaml     → github token + repo list

No credentials are stored here; all values come from the existing config files.
Run from virtualenv: source ~/venv/basic-pandas/bin/activate
"""

import sys
import yaml
from pathlib import Path
from typing import Any, Dict, List

try:
    from lucille.github.github_utils import fetch_org_repos
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from lucille.github.github_utils import fetch_org_repos

DEFAULT_JIRA_EPIC_CONFIG = Path.home() / "bin" / "jira_epic_config.yaml"
DEFAULT_LEAD_TIME_CONFIG = Path.home() / "bin" / "lead_time_config.yaml"
DEFAULT_GITHUB_CONFIG = Path.home() / "bin" / "github_config.yaml"


def _load_yaml(path: Path) -> Dict[str, Any]:
    try:
        with open(path, "r") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        print(f"Error: Config file not found: {path}", file=sys.stderr)
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing {path}: {e}", file=sys.stderr)
        sys.exit(1)


def load_config(
    jira_epic_config_path: Path = DEFAULT_JIRA_EPIC_CONFIG,
    lead_time_config_path: Path = DEFAULT_LEAD_TIME_CONFIG,
    github_config_path: Path = DEFAULT_GITHUB_CONFIG,
) -> Dict[str, Any]:
    """
    Load and merge config from three existing ~/bin YAML files.

    Returns a single dict with keys:
      cfr, confluence, jira, github, effective_repos, project_keys
    """
    epic_cfg = _load_yaml(jira_epic_config_path)
    lead_cfg = _load_yaml(lead_time_config_path)
    github_cfg = _load_yaml(github_config_path)

    if "cfr" not in epic_cfg:
        print(
            f"Error: No 'cfr:' section found in {jira_epic_config_path}.\n"
            "Please append the cfr: block from cfr_research.md.",
            file=sys.stderr,
        )
        sys.exit(1)

    config: Dict[str, Any] = {
        "cfr": epic_cfg["cfr"],
        "confluence": epic_cfg.get("confluence", {}),
        "sprint_boards": epic_cfg.get("sprint_boards", {}),
        "jira": lead_cfg.get("jira", {}),
        "github": {
            "token": github_cfg.get("github_token"),
            "org": github_cfg.get("org"),
        },
    }

    _validate(config)

    # Dynamically fetch all non-archived repos from the GitHub org
    all_repos: List[str] = fetch_org_repos(config["github"]["org"], config["github"]["token"])

    # Build the effective repo list: scoped_repos ∩ all org repos (optional filter)
    scoped: List[str] = config["cfr"].get("scoped_repos") or []
    if scoped:
        unknown = [r for r in scoped if r not in all_repos]
        if unknown:
            print(
                f"Warning: scoped_repos not found in GitHub org: {unknown}",
                file=sys.stderr,
            )
        config["effective_repos"] = [r for r in scoped if r in all_repos]
    else:
        config["effective_repos"] = all_repos

    return config


def _validate(config: Dict[str, Any]) -> None:
    errors = []
    if not config["github"].get("token"):
        errors.append("github_token missing from github_config.yaml")
    if not config["github"].get("org"):
        errors.append("org missing from github_config.yaml")
    if not config["jira"].get("api_token"):
        errors.append("jira.api_token missing from lead_time_config.yaml")
    if not config["jira"].get("base_url"):
        errors.append("jira.base_url missing from lead_time_config.yaml")
    if not config["jira"].get("username"):
        errors.append("jira.username missing from lead_time_config.yaml")
    if errors:
        for e in errors:
            print(f"Config error: {e}", file=sys.stderr)
        sys.exit(1)
