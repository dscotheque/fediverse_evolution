#!/usr/bin/env python3
"""
Fetch Mastodon instances from instances.social API and export to CSV or JSON.
Filters for instances with more than 10 users.
Output format is auto-detected from file extension (.json or .csv).
"""

import csv
import json
import os
import requests
import sys
from typing import Any


def sanitize_for_csv(value: Any) -> str:
    """Sanitize a value for CSV output by handling newlines and special characters."""
    if value is None:
        return ""

    text = str(value)
    # Replace newlines and carriage returns with spaces
    text = text.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    # Replace tabs with spaces
    text = text.replace("\t", " ")
    # Collapse multiple spaces into one
    while "  " in text:
        text = text.replace("  ", " ")
    # Strip leading/trailing whitespace
    return text.strip()


def fetch_instances(api_token: str, min_users: int = 10) -> list[dict]:
    """Fetch instances from instances.social API with user count filtering."""
    
    url = "https://instances.social/api/1.0/instances/list"
    headers = {
        "Authorization": f"Bearer {api_token}"
    }
    params = {
        "count": 0,  # 0 means all instances
        "include_closed": "true",
        "include_down": "false",
        "sort_by": "users",
        "sort_order": "desc"
    }
    
    response = requests.get(url, headers=headers, params=params, timeout=60)
    response.raise_for_status()
    
    data = response.json()
    instances = data.get("instances", [])
    
    filtered = [
        inst for inst in instances
        if inst.get("users") and int(inst.get("users", 0)) > min_users
    ]
    
    return filtered


def write_csv(instances: list[dict], output_file: str):
    """Write instances to CSV file with proper sanitization."""

    if not instances:
        print("No instances to write.")
        return

    fieldnames = [
        "name",
        "title",
        "users",
        "statuses",
        "connections",
        "open_registrations",
        "uptime",
        "https_score",
        "ipv6",
        "version",
        "short_description",
        "full_description"
    ]

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore",
            quoting=csv.QUOTE_ALL  # Quote all fields to handle special characters
        )
        writer.writeheader()

        for inst in instances:
            info = inst.get("info") or {}
            row = {
                "name": sanitize_for_csv(inst.get("name")),
                "title": sanitize_for_csv(inst.get("title")),
                "users": inst.get("users", ""),
                "statuses": inst.get("statuses", ""),
                "connections": inst.get("connections", ""),
                "open_registrations": inst.get("openRegistrations", ""),
                "uptime": inst.get("uptime", ""),
                "https_score": inst.get("https_score", ""),
                "ipv6": inst.get("ipv6", ""),
                "version": sanitize_for_csv(inst.get("version")),
                "short_description": sanitize_for_csv(info.get("short_description")),
                "full_description": sanitize_for_csv(info.get("full_description"))
            }
            writer.writerow(row)


def write_json(instances: list[dict], output_file: str):
    """Write instances to JSON file preserving full structure."""

    if not instances:
        print("No instances to write.")
        return

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(instances, f, ensure_ascii=False, indent=2)


def get_output_format(output_file: str) -> str:
    """Determine output format from file extension."""
    ext = os.path.splitext(output_file)[1].lower()
    if ext == ".json":
        return "json"
    return "csv"


def main():
    if len(sys.argv) < 2:
        print("Usage: python instance_fetch.py <API_TOKEN> [output_file] [min_users]")
        print("\nOutput format is auto-detected from file extension:")
        print("  .json - JSON format (preserves full structure)")
        print("  .csv  - CSV format (default)")
        print("\nExamples:")
        print("  python instance_fetch.py YOUR_TOKEN instances.json 10")
        print("  python instance_fetch.py YOUR_TOKEN instances.csv 10")
        sys.exit(1)

    api_token = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "instances.json"
    min_users = int(sys.argv[3]) if len(sys.argv) > 3 else 10

    output_format = get_output_format(output_file)
    print(f"Fetching instances with more than {min_users} users...")

    try:
        instances = fetch_instances(api_token, min_users)
        print(f"Found {len(instances)} instances with more than {min_users} users")

        if output_format == "json":
            write_json(instances, output_file)
        else:
            write_csv(instances, output_file)

        print(f"Data written to {output_file} ({output_format.upper()} format)")

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        if e.response.status_code == 401:
            print("Check that your API token is valid.")
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()