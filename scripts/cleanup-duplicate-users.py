#!/usr/bin/env python3
"""
Clean up duplicate user records in the users table.

Duplicates happen when a user signs up via email (USER#{cognito_sub}) and then
logs in via GitHub (USER#{github_id}) — creating two records for the same person.

This script:
1. Scans users table for all USER# records
2. Groups by normalized email (strips +tag aliases)
3. For each email with multiple records: keeps the one with the most data (has github_id),
   copies missing fields to the keeper, deletes the duplicate
4. Dry-run by default (--apply to actually delete)
"""

import argparse
import re
import boto3
from collections import defaultdict


def normalize_email(email: str) -> str:
    email = email.strip().lower()
    if "@" in email:
        local, domain = email.split("@", 1)
        local = local.split("+")[0]
        return f"{local}@{domain}"
    return email


def scan_all_users(table):
    items = []
    params = {
        "FilterExpression": "begins_with(sk, :prefix)",
        "ExpressionAttributeValues": {":prefix": "USER#"},
    }
    while True:
        resp = table.scan(**params)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        params["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items


def pick_keeper(records):
    """Pick the record with the most useful data (has github_id preferred)."""
    # Prefer record with github_id set
    with_github = [r for r in records if r.get("github_id")]
    if with_github:
        return with_github[0]
    # Prefer record with role=owner
    owners = [r for r in records if r.get("role") == "owner"]
    if owners:
        return owners[0]
    return records[0]


def main():
    parser = argparse.ArgumentParser(description="Clean up duplicate user records")
    parser.add_argument("--table", required=True, help="DynamoDB users table name")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--apply", action="store_true", help="Actually delete duplicates (default: dry-run)")
    args = parser.parse_args()

    dynamodb = boto3.resource("dynamodb", region_name=args.region)
    table = dynamodb.Table(args.table)

    print(f"Scanning {args.table}...")
    users = scan_all_users(table)
    print(f"Found {len(users)} user records")

    # Group by (team, normalized_email)
    groups = defaultdict(list)
    for u in users:
        team = u.get("pk", "")
        email = u.get("email", "")
        if not email:
            continue
        key = (team, normalize_email(email))
        groups[key].append(u)

    duplicates = {k: v for k, v in groups.items() if len(v) > 1}

    if not duplicates:
        print("No duplicates found!")
        return

    print(f"\nFound {len(duplicates)} duplicate groups:\n")

    for (team, norm_email), records in duplicates.items():
        print(f"  Team: {team}")
        print(f"  Email:  {norm_email}")
        keeper = pick_keeper(records)
        for r in records:
            marker = " <-- KEEP" if r["sk"] == keeper["sk"] else " <-- DELETE"
            github = r.get("github_login", r.get("github_id", ""))
            role = r.get("role", "?")
            print(f"    sk={r['sk']}  role={role}  github={github}{marker}")

        to_delete = [r for r in records if r["sk"] != keeper["sk"]]

        if args.apply:
            # Merge fields from duplicates into keeper
            merge_fields = ["github_id", "github_login", "avatar_url", "name"]
            update_parts = []
            attr_values = {}
            for field in merge_fields:
                if not keeper.get(field):
                    for dup in to_delete:
                        if dup.get(field):
                            update_parts.append(f"{field} = :{field}")
                            attr_values[f":{field}"] = dup[field]
                            break

            # Always set GSI1 if keeper doesn't have it but a dup does
            if not keeper.get("gsi1pk"):
                for dup in to_delete:
                    if dup.get("gsi1pk"):
                        update_parts.append("gsi1pk = :g1pk")
                        update_parts.append("gsi1sk = :g1sk")
                        attr_values[":g1pk"] = dup["gsi1pk"]
                        attr_values[":g1sk"] = dup.get("gsi1sk", team)
                        break

            if update_parts:
                table.update_item(
                    Key={"pk": keeper["pk"], "sk": keeper["sk"]},
                    UpdateExpression="SET " + ", ".join(update_parts),
                    ExpressionAttributeValues=attr_values,
                )
                print(f"    -> Merged fields into keeper: {list(attr_values.keys())}")

            for dup in to_delete:
                table.delete_item(Key={"pk": dup["pk"], "sk": dup["sk"]})
                print(f"    -> Deleted {dup['sk']}")
        print()

    if not args.apply:
        print("Dry run — use --apply to delete duplicates")


if __name__ == "__main__":
    main()
