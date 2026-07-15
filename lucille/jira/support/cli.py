"""Shared CLI-argument and credential helpers for SUP weekly analyses."""

from __future__ import annotations

import argparse
from typing import Dict, Optional, Tuple

from lucille.common.paths import BIN_DIR, DEBRIS_DIR

DEFAULT_CONFIG = str(BIN_DIR / "jira.yaml")
DEFAULT_OUTPUT_DIR = str(DEBRIS_DIR)
DEFAULT_JIRA_BASE_URL = "https://jarisinc.atlassian.net"


def build_common_parser(
    description: str,
    *,
    epilog: Optional[str] = None,
) -> argparse.ArgumentParser:
    """Build an argparse parser with the four flags every SUP analyzer uses.

    Flags: ``-c/--config``, ``-o/--output-dir``, ``-w/--weeks``,
    ``--log-level``.
    """
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
    )
    parser.add_argument(
        "-c", "--config",
        default=DEFAULT_CONFIG,
        help=f"Path to Jira configuration YAML file (default: {DEFAULT_CONFIG})",
    )
    parser.add_argument(
        "-o", "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory for generated files (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "-w", "--weeks",
        type=int,
        default=8,
        help="Number of weeks to analyze (default: 8)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    return parser


def resolve_jira_credentials(config: Dict) -> Tuple[str, str, str]:
    """Extract ``(base_url, username, api_token)`` from a jira.yaml dict.

    Falls back to ``DEFAULT_JIRA_BASE_URL`` if the config's ``url`` field is
    missing (mirroring the pre-refactor behavior of both SUP scripts).

    Returns:
        ``(base_url, username, api_token)``. Any missing string is returned
        as empty; callers are expected to validate.
    """
    base_url = config.get("url", "").replace("/rest/api/3/search/jql", "")
    if not base_url:
        base_url = DEFAULT_JIRA_BASE_URL
    username = config.get("email", "")
    api_token = config.get("api_token", "")
    return base_url, username, api_token
