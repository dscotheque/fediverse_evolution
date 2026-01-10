#!/usr/bin/env python3
"""
Fetch Wayback Machine archives of Mastodon instance 'about' pages.
Retrieves one archive snapshot per quarter for all available years.
"""

import csv
import json
import os
import sys
import time
from datetime import datetime
from typing import Optional
from urllib.parse import quote

import requests


CDX_API_URL = "http://web.archive.org/cdx/search/cdx"
WAYBACK_BASE = "https://web.archive.org/web"


def get_quarter(timestamp: str) -> tuple[int, int]:
    """Extract year and quarter (1-4) from a Wayback timestamp."""
    year = int(timestamp[:4])
    month = int(timestamp[4:6])
    quarter = (month - 1) // 3 + 1
    return (year, quarter)


def fetch_cdx_records(url: str, retries: int = 3) -> list[dict]:
    """Fetch all CDX records for a URL from Wayback Machine."""
    
    params = {
        "url": url,
        "output": "json",
        "filter": "statuscode:200",
        "fl": "timestamp,original,statuscode,mimetype,digest"
    }
    
    for attempt in range(retries):
        try:
            response = requests.get(CDX_API_URL, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            if not data:
                return []
            
            fields = data[0]
            records = []
            for row in data[1:]:
                record = dict(zip(fields, row))
                records.append(record)
            
            return records
            
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            print(f"  Error fetching CDX for {url}: {e}")
            return []
        except json.JSONDecodeError:
            return []
    
    return []


def filter_quarterly(records: list[dict]) -> list[dict]:
    """Filter records to keep only one per quarter, preferring mid-quarter dates."""
    
    if not records:
        return []
    
    by_quarter: dict[tuple[int, int], list[dict]] = {}
    
    for record in records:
        ts = record["timestamp"]
        quarter_key = get_quarter(ts)
        
        if quarter_key not in by_quarter:
            by_quarter[quarter_key] = []
        by_quarter[quarter_key].append(record)
    
    selected = []
    for quarter_key in sorted(by_quarter.keys()):
        candidates = by_quarter[quarter_key]
        year, quarter = quarter_key
        
        # prefer snapshot closest to middle of quarter (month 2, 5, 8, 11)
        mid_month = (quarter - 1) * 3 + 2
        target = f"{year}{mid_month:02d}15"
        
        best = min(candidates, key=lambda r: abs(int(r["timestamp"][:8]) - int(target[:8])))
        selected.append(best)
    
    return selected


def build_wayback_url(timestamp: str, original_url: str, raw: bool = False) -> str:
    """Build a Wayback Machine URL for a given timestamp and original URL."""
    if raw:
        return f"{WAYBACK_BASE}/{timestamp}id_/{original_url}"
    return f"{WAYBACK_BASE}/{timestamp}/{original_url}"


def process_instance(instance_name: str) -> list[dict]:
    """Process a single instance and return quarterly archive records."""
    
    about_url = f"https://{instance_name}/about"
    
    records = fetch_cdx_records(about_url)
    
    if not records:
        alt_url = f"http://{instance_name}/about"
        records = fetch_cdx_records(alt_url)
    
    if not records:
        return []
    
    quarterly = filter_quarterly(records)
    
    results = []
    for record in quarterly:
        ts = record["timestamp"]
        year, quarter = get_quarter(ts)
        
        results.append({
            "instance": instance_name,
            "year": year,
            "quarter": quarter,
            "timestamp": ts,
            "archive_url": build_wayback_url(ts, record["original"]),
            "raw_url": build_wayback_url(ts, record["original"], raw=True),
            "original_url": record["original"],
            "mimetype": record.get("mimetype", ""),
            "digest": record.get("digest", "")
        })
    
    return results


def load_instances(input_file: str) -> list[str]:
    """Load instance names from a file (CSV or plain text)."""
    
    instances = []
    
    with open(input_file, "r", encoding="utf-8") as f:
        if input_file.endswith(".csv"):
            reader = csv.DictReader(f)
            for row in reader:
                name = row.get("name") or row.get("instance") or row.get("domain")
                if name:
                    instances.append(name.strip())
        else:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    instances.append(line)
    
    return instances


def download_snapshot(url: str, output_path: str, retries: int = 3) -> bool:
    """Download a snapshot HTML to a file."""
    
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            
            with open(output_path, "wb") as f:
                f.write(response.content)
            return True
            
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            return False
    
    return False


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Fetch Wayback Machine archives of Mastodon instance about pages"
    )
    parser.add_argument(
        "input_file",
        help="Input file with instance names (CSV with 'name' column or plain text)"
    )
    parser.add_argument(
        "-o", "--output",
        default="wayback_archives.csv",
        help="Output CSV file (default: wayback_archives.csv)"
    )
    parser.add_argument(
        "-d", "--download-dir",
        help="Directory to download HTML snapshots (optional)"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay between API requests in seconds (default: 1.0)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of instances to process (for testing)"
    )
    
    args = parser.parse_args()
    
    instances = load_instances(args.input_file)
    if args.limit:
        instances = instances[:args.limit]
    
    print(f"Processing {len(instances)} instances...")
    
    if args.download_dir:
        os.makedirs(args.download_dir, exist_ok=True)
    
    all_results = []
    
    for i, instance in enumerate(instances):
        print(f"[{i+1}/{len(instances)}] {instance}...", end=" ", flush=True)
        
        results = process_instance(instance)
        
        if results:
            print(f"found {len(results)} quarterly snapshots")
            all_results.extend(results)
            
            if args.download_dir:
                for r in results:
                    filename = f"{r['instance']}_{r['year']}Q{r['quarter']}.html"
                    filepath = os.path.join(args.download_dir, filename)
                    if not os.path.exists(filepath):
                        download_snapshot(r["raw_url"], filepath)
                        time.sleep(0.5)
        else:
            print("no archives found")
        
        if i < len(instances) - 1:
            time.sleep(args.delay)
    
    if all_results:
        fieldnames = [
            "instance", "year", "quarter", "timestamp",
            "archive_url", "raw_url", "original_url", "mimetype", "digest"
        ]
        
        with open(args.output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_results)
        
        print(f"\nWrote {len(all_results)} records to {args.output}")
        
        unique_instances = len(set(r["instance"] for r in all_results))
        print(f"Coverage: {unique_instances}/{len(instances)} instances have archives")
    else:
        print("\nNo archive records found.")


if __name__ == "__main__":
    main()