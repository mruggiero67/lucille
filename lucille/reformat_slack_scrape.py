#!/usr/bin/env python3
"""
Converts rough Slack channel scrapes into single-line log entries.
Blank lines are used as delimiters between entries.
"""

import sys
import re
from datetime import datetime


def clean_text(text):
    """Clean up text by removing extra whitespace and special characters."""
    # Replace special characters and normalize whitespace
    text = re.sub(r"[^\w\s\-\.\:\/@#]", " ", text)
    # Replace multiple whitespace with single space
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def parse_slack_entry(entry_lines):
    """Parse a single Slack entry and return a structured log line."""
    if not entry_lines:
        return None

    # Join all lines and clean
    full_entry = " ".join(line.strip() for line in entry_lines if line.strip())
    full_entry = clean_text(full_entry)

    # Try to extract timestamp if present
    timestamp_match = re.search(r"(\d{1,2}:\d{2}\s*(?:AM|PM)?)", full_entry)
    timestamp = timestamp_match.group(1) if timestamp_match else "UNKNOWN_TIME"

    # Try to extract username (typically first word or before timestamp)
    lines = [line.strip() for line in entry_lines if line.strip()]
    if lines:
        # Look for username pattern (word followed by timestamp)
        first_line = lines[0]
        username_match = re.match(r"^(\w+)\s+(\d{1,2}:\d{2})", first_line)
        if username_match:
            username = username_match.group(1)
        else:
            # Fallback: use first word if it looks like a username
            words = first_line.split()
            username = words[0] if words and words[0].isalpha() else "UNKNOWN_USER"
    else:
        username = "UNKNOWN_USER"

    # Extract the main content (everything after user and timestamp)
    content = full_entry
    # Remove the username and timestamp from the beginning if found
    content = re.sub(
        r"^" + re.escape(username) + r"\s+\d{1,2}:\d{2}\s*(?:AM|PM)?\s*", "", content
    )

    # Create single log line
    # NB: this pegs to current date; Slack scrapes don't have dates.
    # You will have to manually adjust the dates.
    current_time = datetime.now().strftime("%Y-%m-%d")
    log_line = f"{current_time} {username} ({timestamp}): {content}"

    return log_line


def convert_slack_scrape_to_logs(input_file, output_file=None):
    """Convert Slack scrape file to log format."""
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
        return
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    # Split by double newlines (blank line delimiters)
    entries = content.split("\n\n")

    log_lines = []

    for i, entry in enumerate(entries):
        if not entry.strip():
            continue

        entry_lines = entry.split("\n")
        log_line = parse_slack_entry(entry_lines)

        if log_line:
            log_lines.append(log_line)

    # Output results
    if output_file:
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                for line in log_lines:
                    f.write(line + "\n")
            print(f"Successfully converted {len(log_lines)} entries to '{output_file}'")
        except Exception as e:
            print(f"Error writing to output file: {e}")
    else:
        # Print to stdout
        for line in log_lines:
            print(line)

    return log_lines


def main():
    """Main function to handle command line arguments."""
    if len(sys.argv) < 2:
        print("Usage: python slack_log_converter.py <input_file> [output_file]")
        print("Example: python slack_log_converter.py fake_log.txt converted_logs.txt")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    convert_slack_scrape_to_logs(input_file, output_file)


if __name__ == "__main__":
    main()
