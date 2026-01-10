#!/usr/bin/env python3
"""
Query the Fediverse Observer GraphQL API for English instances with >10 users.

Based on the API documentation at https://api.fediverse.observer/
This returns all available fields for instances matching the criteria.

Usage:
    pip install requests
    python fediverse_query.py
"""

import requests
import json
import sys
from datetime import datetime

API_URL = "https://api.fediverse.observer/"

HEADERS = {
    'Accept': 'application/json',
    'Content-Type': 'application/json; charset=utf-8',
}

ALL_FIELDS_QUERY = """
{
  nodes(softwarename: "") {
    id
    name
    metatitle
    metadescription
    metalocation
    owner
    metaimage
    terms
    pp
    support
    zipcode
    domain
    admin_statement
    masterversion
    shortversion
    softwarename
    daysmonitored
    monthsmonitored
    fullversion
    score
    ip
    detectedlanguage
    country
    countryname
    city
    state
    lat
    long
    ipv6
    signup
    total_users
    active_users_halfyear
    active_users_monthly
    local_posts
    uptime_alltime
    status
    latency
    dnssec
    comment_counts
    date_updated
    date_laststats
    date_created
  }
}
"""

MINIMAL_QUERY = """
{
  nodes(softwarename: "") {
    domain
    softwarename
    total_users
    detectedlanguage
    country
    countryname
  }
}
"""


def query_api(query):
    """Execute a GraphQL query against the API."""
    response = requests.post(
        API_URL,
        headers=HEADERS,
        json={'query': query},
        timeout=120
    )
    response.raise_for_status()
    return response.json()


def filter_by_users(nodes, min_users=10):
    """Filter nodes to only include those with more than min_users."""
    return [
        node for node in nodes
        if node.get('total_users') is not None and node['total_users'] > min_users
    ]


def filter_by_language(nodes, language="en"):
    """Filter nodes to only include those with the specified detected language."""
    return [
        node for node in nodes
        if node.get('detectedlanguage') == language
    ]


def introspect_schema():
    """Get available query arguments to understand filtering options."""
    query = """
    {
        __schema {
            queryType {
                fields {
                    name
                    args {
                        name
                        type { name kind ofType { name } }
                    }
                }
            }
        }
        __type(name: "Node") {
            fields {
                name
                type { name kind ofType { name } }
            }
        }
    }
    """
    return query_api(query)


def main():
    print("=" * 70)
    print("Fediverse Observer API Query Tool")
    print("Querying for English instances with >10 users")
    print("=" * 70)

    print("\n[1] Introspecting API schema...")
    try:
        schema = introspect_schema()
        
        if "data" in schema:
            query_fields = schema["data"]["__schema"]["queryType"]["fields"]
            for field in query_fields:
                if field["name"] == "nodes":
                    print(f"\n    'nodes' query supports these filter arguments:")
                    for arg in field.get("args", []):
                        type_info = arg["type"]
                        type_name = type_info.get("name") or type_info.get("ofType", {}).get("name", "?")
                        print(f"      - {arg['name']}: {type_name}")
            
            node_type = schema["data"]["__type"]
            if node_type:
                print(f"\n    Node type has {len(node_type['fields'])} available fields")
        else:
            print("    Schema introspection returned no data")
    except Exception as e:
        print(f"    Schema introspection failed: {e}")

    print("\n[2] Fetching all instances...")
    try:
        result = query_api(ALL_FIELDS_QUERY)

        if "errors" in result:
            print(f"    Query errors: {result['errors']}")
            print("    Trying minimal query...")
            result = query_api(MINIMAL_QUERY)

        if "data" not in result or not result["data"].get("nodes"):
            print("    No data returned from API")
            print(f"    Response: {json.dumps(result, indent=2)}")
            return

        all_nodes = result["data"]["nodes"]
        print(f"    Retrieved {len(all_nodes)} instances total")

        all_nodes = filter_by_language(all_nodes, "en")
        print(f"    Filtered to {len(all_nodes)} English instances")

    except requests.exceptions.RequestException as e:
        print(f"    API request failed: {e}")
        return
    except Exception as e:
        print(f"    Unexpected error: {e}")
        return

    print("\n[3] Filtering for instances with >10 users...")
    filtered_nodes = filter_by_users(all_nodes, min_users=10)
    print(f"    Found {len(filtered_nodes)} instances with >10 users")

    print("\n[4] Saving results...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    output_file = f"fediverse_english_gt10users_{timestamp}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump({
            "query_time": datetime.now().isoformat(),
            "filters": {"language": "en", "min_users": 10},
            "total_count": len(filtered_nodes),
            "instances": filtered_nodes
        }, f, indent=2, ensure_ascii=False)
    print(f"    Saved to: {output_file}")

    all_output = f"fediverse_english_all_{timestamp}.json"
    with open(all_output, "w", encoding="utf-8") as f:
        json.dump({
            "query_time": datetime.now().isoformat(),
            "filters": {"language": "en"},
            "total_count": len(all_nodes),
            "instances": all_nodes
        }, f, indent=2, ensure_ascii=False)
    print(f"    Saved all English instances to: {all_output}")

    print("\n[5] Sample results (first 10 by user count):")
    print("-" * 70)
    sorted_nodes = sorted(filtered_nodes, key=lambda x: x.get('total_users', 0) or 0, reverse=True)
    
    print(f"{'Domain':<40} {'Software':<15} {'Users':>10}")
    print("-" * 70)
    for node in sorted_nodes[:10]:
        domain = (node.get('domain') or 'N/A')[:39]
        software = (node.get('softwarename') or 'N/A')[:14]
        users = node.get('total_users') or 0
        print(f"{domain:<40} {software:<15} {users:>10,}")

    print("\n[6] Statistics:")
    print("-" * 70)
    total_users = sum(n.get('total_users', 0) or 0 for n in filtered_nodes)
    print(f"    Total users across filtered instances: {total_users:,}")
    
    software_counts = {}
    for node in filtered_nodes:
        sw = node.get('softwarename') or 'unknown'
        software_counts[sw] = software_counts.get(sw, 0) + 1
    
    print("\n    Instances by software:")
    for sw, count in sorted(software_counts.items(), key=lambda x: -x[1])[:10]:
        print(f"      {sw}: {count}")

    print("\n" + "=" * 70)
    print("Done!")


if __name__ == "__main__":
    main()