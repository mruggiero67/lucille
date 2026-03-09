#!/usr/bin/env python3
"""
CFR Calculator — CLI entrypoint.

Activate venv before running:
  source ~/venv/basic-pandas/bin/activate

Usage:
  python cfr.py [options]
  python cfr.py --repo analytics --days 30 --dry-run
  python cfr.py --repo public-api --no-cache
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Handle both direct execution and import from package root
try:
    from lucille.cfr.config_loader import load_config
    from lucille.cfr.sources.github_client import GitHubClient
    from lucille.cfr.sources.jira_client import JiraClient
    from lucille.cfr.logic.deployment_detector import DeploymentDetector
    from lucille.cfr.logic.pr_classifier import classify_deployment
    from lucille.cfr.logic.intervention_detector import InterventionDetector
    from lucille.cfr.logic.cfr_rollup import compute_cfr, DeploymentRecord
    from lucille.cfr.output.csv_writer import write_csv
    from lucille.cfr.output.summary_reporter import print_summary, write_summary
    from lucille.cfr.output.confluence_publisher import publish_to_confluence
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from lucille.cfr.config_loader import load_config
    from lucille.cfr.sources.github_client import GitHubClient
    from lucille.cfr.sources.jira_client import JiraClient
    from lucille.cfr.logic.deployment_detector import DeploymentDetector
    from lucille.cfr.logic.pr_classifier import classify_deployment
    from lucille.cfr.logic.intervention_detector import InterventionDetector
    from lucille.cfr.logic.cfr_rollup import compute_cfr, DeploymentRecord
    from lucille.cfr.output.csv_writer import write_csv
    from lucille.cfr.output.summary_reporter import print_summary, write_summary
    from lucille.cfr.output.confluence_publisher import publish_to_confluence

logging.basicConfig(
    format="%(levelname)-8s %(asctime)s %(filename)s:%(lineno)d %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute Change Failure Rate (CFR) from GitHub + Jira data."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path.home() / "bin" / "jira_epic_config.yaml",
        help="Path to jira_epic_config.yaml (contains cfr: section)",
    )
    parser.add_argument(
        "--github-config",
        type=Path,
        default=Path.home() / "bin" / "github_config.yaml",
        help="Path to github_config.yaml",
    )
    parser.add_argument(
        "--jira-config",
        type=Path,
        default=Path.home() / "bin" / "lead_time_config.yaml",
        help="Path to lead_time_config.yaml",
    )
    parser.add_argument(
        "--repo",
        type=str,
        default=None,
        help="Run for a single repo only (overrides cfr.scoped_repos)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Lookback window in days (overrides cfr.lookback_days)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data and compute CFR but do not write output files",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Bypass local cache; re-fetch from APIs",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Override the output directory from config",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Load merged config from three existing ~/bin YAML files
    config = load_config(
        jira_epic_config_path=args.config,
        lead_time_config_path=args.jira_config,
        github_config_path=args.github_config,
    )

    # CLI overrides
    if args.repo:
        config["effective_repos"] = [args.repo]
        logger.info(f"Scoped to single repo: {args.repo}")

    lookback_days = args.days or config["cfr"].get("lookback_days", 90)
    since = datetime.now(tz=timezone.utc) - timedelta(days=lookback_days)
    period_start = since.date()
    period_end = datetime.now(tz=timezone.utc).date()

    output_dir = args.output_dir or Path(
        config["cfr"].get("output_directory", str(Path.home() / "Desktop" / "debris"))
    )

    logger.info(
        f"CFR run: repos={config['effective_repos']}, "
        f"lookback={lookback_days}d, since={since.date()}, dry_run={args.dry_run}"
    )

    # Initialise clients
    gh = GitHubClient(
        token=config["github"]["token"],
        org=config["github"]["org"],
        use_cache=not args.no_cache,
    )
    jira_cfg = config["jira"]
    jira = JiraClient(
        base_url=jira_cfg["base_url"],
        username=jira_cfg["username"],
        api_token=jira_cfg["api_token"],
    )

    detector = DeploymentDetector(gh, config)
    intervention_detector = InterventionDetector(gh, jira, config)

    all_records = []

    for repo in config["effective_repos"]:
        logger.info(f"Processing repo: {repo}")
        try:
            events = detector.detect(repo, since)
        except Exception as e:
            logger.error(f"Failed to detect deployments for {repo}: {e}")
            continue

        for event in events:
            category = classify_deployment(event.prs, config)
            try:
                intervention = intervention_detector.check(event)
            except Exception as e:
                logger.warning(f"Intervention check failed for {event.deployment_id}: {e}")
                from lucille.cfr.logic.intervention_detector import InterventionResult
                intervention = InterventionResult(detected=False)

            all_records.append(
                DeploymentRecord(
                    event=event,
                    category=category,
                    intervention=intervention,
                )
            )
            status = "FAIL" if intervention.detected else "OK"
            logger.info(
                f"  {event.deployment_id} [{category}] → {status}"
                + (f": {intervention.reason}" if intervention.reason else "")
            )

    if not all_records:
        print("No deployment records found. Check your scoped_repos and lookback_days.")
        return

    result = compute_cfr(all_records, period_start, period_end)

    print_summary(result)

    if not args.dry_run:
        csv_filename = config["cfr"].get("csv_filename", "cfr_report.csv")
        csv_path = output_dir / csv_filename
        write_csv(all_records, csv_path, config)

        summary_path = output_dir / "cfr_summary.md"
        write_summary(result, summary_path)

        publish_to_confluence(result, config)
    else:
        print("\n[dry-run] No files written.")


if __name__ == "__main__":
    main()
