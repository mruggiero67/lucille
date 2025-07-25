#!/usr/bin/env python3
"""
Parse Slack deployment messages to extract deployment frequency metrics.
Works with copy-pasted text from Slack channels.
"""

import re
from collections import defaultdict, Counter
import csv
from typing import List, Dict, Any
import argparse
import logging

logging.basicConfig(
    format="%(levelname)-10s %(asctime)s %(filename)s %(lineno)d %(message)s",
    level=logging.INFO,
)


class SlackDeploymentParser:
    def __init__(self):
        # Common deployment message patterns - adjust these based on your actual messages
        self.deployment_patterns = [
            r"(?i)deployed?\s+(\w+[-\w]*)\s+(?:v?[\d.]+\s+)?to\s+(?:production|prod)",
            r"(?i)🚀\s*(\w+[-\w]*)\s+deployed?\s+to\s+(?:production|prod)",
            r"(?i)(?:production|prod)\s+deployment\s+complete\s*[-:]\s*(\w+[-\w]*)",
            r"(?i)(\w+[-\w]*)\s+(?:is\s+)?(?:now\s+)?live\s+(?:in\s+)?(?:production|prod)",
            r"(?i)released?\s+(\w+[-\w]*)\s+(?:v?[\d.]+\s+)?to\s+(?:production|prod)",
        ]

        # Time patterns for Slack timestamps
        self.time_patterns = [
            r"(\d{1,2}:\d{2})\s*(AM|PM)",  # 3:45 PM
            r"(\d{1,2}:\d{2})",  # 15:45 (24-hour)
        ]

        # Date patterns
        self.date_patterns = [
            r"(\w+\s+\d{1,2})",  # "Jan 15", "January 15"
            r"(\d{1,2}/\d{1,2})",  # "1/15", "01/15"
            r"(\d{4}-\d{2}-\d{2})",  # "2025-01-15"
        ]

    def parse_slack_export(self, text_content: str) -> List[Dict[str, Any]]:
        """
        Parse copied Slack messages to extract deployment information.
        Format: "2025-05-20 deployed jakub 6:40 AM BankingInsights 1.55.0 released"

        Args:
            text_content: Raw text copied from Slack channel

        Returns:
            List of deployment dictionaries
        """
        deployments = []
        lines = text_content.strip().split("\n")

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Parse the specific format: "YYYY-MM-DD deployed username H:MM AM/PM ServiceName version released"
            deployment = self._parse_deployment_line(line)
            if deployment:
                deployments.append(deployment)

        return deployments

    def _parse_deployment_line(self, line: str) -> Dict[str, Any]:
        """
        Parse a single deployment line with format:
        "2025-05-20 deployed jakub 6:40 AM BankingInsights 1.55.0 released"
        """
        # Pattern to match the specific format
        pattern = r"^(\d{4}-\d{2}-\d{2})\s+deployed\s+(\w+)\s+(\d{1,2}:\d{2})\s+(AM|PM)\s+(\S+)\s+([\d.]+)\s+released"

        match = re.match(pattern, line)
        if match:
            date = match.group(1)
            user = match.group(2)
            time = match.group(3)
            meridiem = match.group(4)
            service = match.group(5)
            version = match.group(6)

            # Create full timestamp
            full_time = f"{time} {meridiem}"
            timestamp = f"{date} {full_time}"

            return {
                "date": date,
                "time": full_time,
                "user": user,
                "service": service,
                "version": version,
                "raw_message": line.strip(),
                "timestamp": timestamp,
            }

        # Fallback: try to extract any deployment-like patterns from the line
        # Look for common deployment keywords
        if any(
            keyword in line.lower() for keyword in ["deployed", "released", "deploy"]
        ):
            # Try to extract date if present
            date_match = re.search(r"(\d{4}-\d{2}-\d{2})", line)
            date = date_match.group(1) if date_match else "unknown"

            # Try to extract service name (word before version number)
            service_match = re.search(r"(\w+)\s+[\d.]+", line)
            service = service_match.group(1) if service_match else "unknown"

            # Try to extract version
            version_match = re.search(r"([\d.]+)", line)
            version = version_match.group(1) if version_match else "unknown"

            # Try to extract user (word after "deployed")
            user_match = re.search(r"deployed\s+(\w+)", line)
            user = user_match.group(1) if user_match else "unknown"

            # Try to extract time
            time_match = re.search(r"(\d{1,2}:\d{2})\s*(AM|PM)", line)
            time = (
                f"{time_match.group(1)} {time_match.group(2)}"
                if time_match
                else "unknown"
            )

            return {
                "date": date,
                "time": time,
                "user": user,
                "service": service,
                "version": version,
                "raw_message": line.strip(),
                "timestamp": (
                    f"{date} {time}"
                    if date != "unknown" and time != "unknown"
                    else line
                ),
            }

        return None

    def analyze_deployments(self, deployments: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze deployment patterns and generate insights."""
        if not deployments:
            return {"error": "No deployments found"}

        # Count deployments by service
        service_counts = Counter(d["service"] for d in deployments)

        # Count by user
        user_counts = Counter(d["user"] for d in deployments)

        # Count by day (if we can parse dates)
        daily_counts = defaultdict(int)
        for d in deployments:
            if d["date"]:
                daily_counts[d["date"]] += 1

        # Deployment frequency
        total_deployments = len(deployments)
        days_with_deployments = len(daily_counts)
        avg_per_day = total_deployments / max(days_with_deployments, 1)

        return {
            "total_deployments": total_deployments,
            "unique_services": len(service_counts),
            "days_with_deployments": days_with_deployments,
            "avg_deployments_per_day": round(avg_per_day, 2),
            "service_breakdown": dict(service_counts),
            "user_breakdown": dict(user_counts),
            "daily_breakdown": dict(daily_counts),
            "most_deployed_service": (
                service_counts.most_common(1)[0] if service_counts else None
            ),
            "busiest_day": (
                max(daily_counts.items(), key=lambda x: x[1]) if daily_counts else None
            ),
        }

    def save_to_csv(self,
                    deployments: List[Dict[str, Any]],
                    filename: str) -> None:
        """Save deployment data to CSV."""
        if not deployments:
            logging.info("No deployments to save")
            return

        fieldnames = [
            "date",
            "time",
            "user",
            "service",
            "version",
            "timestamp",
            "raw_message",
        ]

        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(deployments)

        logging.info(f"Deployment data saved to {filename}")

    def print_analysis(self, analysis: Dict[str, Any]):
        """Print deployment analysis summary."""
        logging.info("\n" + "=" * 50)
        logging.info("DEPLOYMENT ANALYSIS SUMMARY")
        logging.info("=" * 50)

        if "error" in analysis:
            logging.info(f"Error: {analysis['error']}")
            return

        logging.info(f"Total Deployments: {analysis['total_deployments']}")
        logging.info(f"Unique Services: {analysis['unique_services']}")
        logging.info(f"Days with Deployments: {analysis['days_with_deployments']}")
        logging.info(f"Average Deployments/Day: {analysis['avg_deployments_per_day']}")

        if analysis["most_deployed_service"]:
            service, count = analysis["most_deployed_service"]
            logging.info(f"Most Deployed Service: {service} ({count} times)")

        if analysis["busiest_day"]:
            day, count = analysis["busiest_day"]
            logging.info(f"Busiest Day: {day} ({count} deployments)")

        logging.info(f"\nDeployments by Service:")
        for service, count in sorted(
            analysis["service_breakdown"].items(), key=lambda x: x[1], reverse=True
        ):
            logging.info(f"  {service}: {count}")

        logging.info(f"\nDeployments by User:")
        for user, count in sorted(
            analysis["user_breakdown"].items(), key=lambda x: x[1], reverse=True
        ):
            logging.info(f"  {user}: {count}")


def main():
    parser = argparse.ArgumentParser(description="Parses txt file of Slack deploy messages, saves data")
    parser.add_argument("-l", "--log", type=str, help="path to Slack log file")
    parser.add_argument("-d", "--directory", type=str, help="directory to store CSV you'll create")

    args = parser.parse_args()
    slack_file = args.log
    target_csv_dir = args.directory

    """Main function to parse Slack deployment messages."""
    parser = SlackDeploymentParser()

    try:
        with open(slack_file, "r", encoding="utf-8") as f:
            content = f.read()
    except FileNotFoundError:
        logging.info(f"Error: {slack_file} file not found.")
        logging.info("Please create this file with your copied Slack messages.")
        return

    # Parse the content
    logging.info("Parsing Slack messages...")
    deployments = parser.parse_slack_export(content)

    if not deployments:
        logging.info("No deployments found. Check message format or patterns.")
        logging.info("Sample expected formats:")
        logging.info("  - 'Deployed frontend v1.2.3 to production'")
        logging.info("  - '🚀 backend-api deployed to prod'")
        logging.info("  - 'Production deployment complete - mobile-app'")
        return

    # Analyze and display results
    analysis = parser.analyze_deployments(deployments)
    parser.print_analysis(analysis)

    # Save to CSV
    csv_filepath = f"{target_csv_dir}/deployment_analysis.csv"
    parser.save_to_csv(deployments, csv_filepath)

    logging.info(f"\nFound {len(deployments)} deployments!")
    logging.info(f"Saved in {csv_filepath}")


if __name__ == "__main__":
    main()
