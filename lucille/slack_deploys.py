#!/usr/bin/env python3
"""
Parse Slack deployment messages to extract deployment frequency metrics.
Works with copy-pasted text from Slack channels.
"""

import re
from datetime import datetime
from collections import defaultdict, Counter
import csv
from typing import List, Dict, Any
import argparse


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

    def save_to_csv(
        self,
        deployments: List[Dict[str, Any]],
        filename: str = "deployment_analysis.csv",
    ):
        """Save deployment data to CSV."""
        if not deployments:
            print("No deployments to save")
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

        print(f"Deployment data saved to {filename}")

    def print_analysis(self, analysis: Dict[str, Any]):
        """Print deployment analysis summary."""
        print("\n" + "=" * 50)
        print("DEPLOYMENT ANALYSIS SUMMARY")
        print("=" * 50)

        if "error" in analysis:
            print(f"Error: {analysis['error']}")
            return

        print(f"Total Deployments: {analysis['total_deployments']}")
        print(f"Unique Services: {analysis['unique_services']}")
        print(f"Days with Deployments: {analysis['days_with_deployments']}")
        print(f"Average Deployments/Day: {analysis['avg_deployments_per_day']}")

        if analysis["most_deployed_service"]:
            service, count = analysis["most_deployed_service"]
            print(f"Most Deployed Service: {service} ({count} times)")

        if analysis["busiest_day"]:
            day, count = analysis["busiest_day"]
            print(f"Busiest Day: {day} ({count} deployments)")

        print(f"\nDeployments by Service:")
        for service, count in sorted(
            analysis["service_breakdown"].items(), key=lambda x: x[1], reverse=True
        ):
            print(f"  {service}: {count}")

        print(f"\nDeployments by User:")
        for user, count in sorted(
            analysis["user_breakdown"].items(), key=lambda x: x[1], reverse=True
        ):
            print(f"  {user}: {count}")


def main():
    parser = argparse.ArgumentParser(description="Parses txt file of Slack deploy messages, saves data")
    parser.add_argument("filepath", type=str, help="path to Slack file")
    args = parser.parse_args()
    slack_file = args.filepath

    """Main function to parse Slack deployment messages."""
    parser = SlackDeploymentParser()

    # Instructions for user
    print("Slack Deployment Message Parser")
    print("=" * 40)
    print("1. Go to your #deploys Slack channel")
    print("2. Scroll back ~4 weeks")
    print("3. Copy all messages to a text file named 'slack_deploys.txt'")
    print("4. Run this script")
    print()

    # Try to read the file
    try:
        with open(slack_file, "r", encoding="utf-8") as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: {slack_file} file not found.")
        print("Please create this file with your copied Slack messages.")
        return

    # Parse the content
    print("Parsing Slack messages...")
    deployments = parser.parse_slack_export(content)

    if not deployments:
        print("No deployments found. Check your message format or patterns.")
        print("Sample expected formats:")
        print("  - 'Deployed frontend v1.2.3 to production'")
        print("  - '🚀 backend-api deployed to prod'")
        print("  - 'Production deployment complete - mobile-app'")
        return

    # Analyze and display results
    analysis = parser.analyze_deployments(deployments)
    parser.print_analysis(analysis)

    # Save to CSV
    parser.save_to_csv(deployments)

    print(f"\nFound {len(deployments)} deployments!")
    print("Data saved to 'deployment_analysis.csv'")


if __name__ == "__main__":
    main()
